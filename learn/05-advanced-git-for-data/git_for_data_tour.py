"""

Git-for-data tour: a walkthrough of Bauplan's versioning primitives.

Demonstrates branching, time travel, tagging, auditing, fault isolation,
reverts, and multi-step transactions — all driven by the Bauplan Python SDK.

The --file-path argument must point to a Parquet file in an S3 bucket that
your Bauplan environment can reach. For example:

    uv run python git_for_data_tour.py \\
        --file-path s3://alpha-hello-bauplan/taxi-2024/yellow_tripdata_2021-01.parquet

"""

import argparse

import bauplan


# ── helpers ──────────────────────────────────────────────────────────


def _header(title: str):
    print(f"\n{'=' * 60}")
    print(f"  {title}")
    print(f"{'=' * 60}")


def _step(n: int, total: int, msg: str):
    print(f"\n  [{n}/{total}]  {msg}")


def _ok(msg: str):
    print(f"         {msg}")


# ── main ─────────────────────────────────────────────────────────────


def main(file_path: str):
    client = bauplan.Client()
    user = client.info().user
    username = user.username
    full_name = user.full_name
    assert full_name is not None and username is not None and user is not None

    source_branch_name = "main"
    my_branch_name = f"{username}.commit_flow"
    my_test_table_name = "my_taxxxi_zones"
    my_compliance_dataset_tag = "compliance_tag"
    run_id_query = f"SELECT run_id FROM {my_test_table_name}"
    txn_branch_name = f"{username}.multi_step_transaction"
    txn_table_name = "transactional_ingested_table"
    TOTAL = 12

    _header("Git for Data Tour")
    print(f"  user: {full_name} ({username})")
    print(f"  branch: {my_branch_name}")

    # clean slate
    if client.has_branch(my_branch_name):
        client.delete_branch(my_branch_name)

    # ── branching & first run ────────────────────────────────────────

    _step(1, TOTAL, "Creating branch and running first pipeline...")
    # clean up any leftover tables on main from previous runs
    for tbl in [my_test_table_name, txn_table_name]:
        if client.has_table(tbl, ref=source_branch_name):
            client.delete_table(tbl, branch=source_branch_name)

    client.create_branch(my_branch_name, from_ref=source_branch_name)

    # a new branch shares the same HEAD as its source — zero-copy
    my_branch_last_commit = next(iter(client.get_commits(my_branch_name, limit=1)))
    source_branch_last_commit = next(
        iter(client.get_commits(source_branch_name, limit=1))
    )
    assert my_branch_last_commit.ref.hash == source_branch_last_commit.ref.hash
    _ok("Branch created — HEAD matches main (zero-copy)")

    run_1 = client.run(
        project_dir="./my_project",
        ref=my_branch_name,
        cache="off",
        parameters={"run_id": 1},
    )
    assert run_1.job_id is not None and run_1.job_status == "SUCCESS"
    my_branch_last_commit = next(iter(client.get_commits(my_branch_name, limit=1)))
    assert my_branch_last_commit.ref.hash != source_branch_last_commit.ref.hash
    _ok(f"Run 1 complete — branch diverged from main (job {run_1.job_id})")

    # the commit tracks which job produced it
    job_id_in_the_commit = my_branch_last_commit.properties["bpln_job_id"]
    assert job_id_in_the_commit == run_1.job_id

    # ── second run & time travel ─────────────────────────────────────

    _step(2, TOTAL, "Running second pipeline to build a commit history...")
    client.run(
        project_dir="./my_project",
        ref=my_branch_name,
        cache="off",
        parameters={"run_id": 2},
    )
    rows = client.query(run_id_query, ref=my_branch_name).to_pylist()
    assert rows == [{"run_id": 2}]
    _ok("Run 2 complete — HEAD now shows run_id=2")

    _step(3, TOTAL, "Time-traveling back to the previous commit...")
    rows = client.query(run_id_query, ref=my_branch_last_commit.ref).to_pylist()
    assert rows == [{"run_id": 1}]
    _ok("Queried the same table at a past commit — got run_id=1")

    # ── tagging ──────────────────────────────────────────────────────

    _step(4, TOTAL, "Tagging the run-1 commit for compliance...")
    if client.has_tag(my_compliance_dataset_tag):
        client.delete_tag(my_compliance_dataset_tag)
    client.create_tag(my_compliance_dataset_tag, my_branch_last_commit.ref)
    _ok(f"Tag '{my_compliance_dataset_tag}' points to run-1 commit")

    # ── audit ────────────────────────────────────────────────────────

    _step(5, TOTAL, f"Auditing commit history for author '{full_name}'...")
    
    if not full_name:
        my_author_commit_history = client.get_commits(
            my_branch_name, filter_by_username=username, limit=5
        )
    else:
        my_author_commit_history = client.get_commits(
            my_branch_name, filter_by_author_name=full_name, limit=5
        )
    found = any(
        c.ref.hash == my_branch_last_commit.ref.hash for c in my_author_commit_history
    )
    assert found
    _ok("Found run-1 commit in author's history")

    target_tag = client.get_tag(my_compliance_dataset_tag)
    rows = client.query(run_id_query, ref=target_tag).to_pylist()
    assert rows == [{"run_id": 1}]
    _ok("Querying by tag returns the compliant dataset (run_id=1)")

    # ── transactions & fault isolation ───────────────────────────────

    _step(6, TOTAL, "Simulating a faulty run (run_id=5 triggers an error)...")
    run_5 = client.run(
        project_dir="./my_project",
        ref=my_branch_name,
        cache="off",
        parameters={"run_id": 5},
    )
    assert run_5.job_status != "SUCCESS" and run_5.job_id is not None
    _ok(f"Run failed as expected (job {run_5.job_id})")

    # failed runs are transactional — the branch is never polluted
    assert client.query(run_id_query, ref=my_branch_name).to_pylist() == [{"run_id": 2}]
    _ok("Branch is untouched — still shows run_id=2")

    # ── revert ───────────────────────────────────────────────────────

    _step(7, TOTAL, "Reverting table to the tagged (compliant) version...")
    client.revert_table(
        table=my_test_table_name,
        source_ref=target_tag,
        into_branch=my_branch_name,
        commit_body=f"Revert to tag {my_compliance_dataset_tag}",
        replace=True,
    )
    rows = client.query(run_id_query, ref=my_branch_name).to_pylist()
    assert rows == [{"run_id": 1}]
    my_branch_last_commit = next(iter(client.get_commits(my_branch_name, limit=1)))
    _ok("Table reverted — HEAD is back to run_id=1")
    _ok(f"Revert recorded as: {my_branch_last_commit.message}")

    # ── clean up git-for-data example ────────────────────────────────

    _step(8, TOTAL, "Cleaning up git-for-data example...")
    client.delete_branch(my_branch_name)
    if client.has_tag(my_compliance_dataset_tag):
        client.delete_tag(my_compliance_dataset_tag)
    _ok("Branch and tag deleted")

    # ── multi-step transaction ───────────────────────────────────────
    #
    # Branches aren't just for isolating pipeline steps — they can
    # sandbox an entire workflow: ingest raw data, transform it, and
    # only merge to main when everything succeeds.

    _header("Multi-Step Transaction")
    print(f"  branch: {txn_branch_name}")
    print(f"  source: {file_path}")

    if client.has_branch(txn_branch_name):
        client.delete_branch(txn_branch_name)

    _step(9, TOTAL, "Creating transaction branch...")
    client.create_branch(txn_branch_name, from_ref="main")
    _ok(f"Branch '{txn_branch_name}' created from main")

    _step(10, TOTAL, f"Ingesting data into '{txn_table_name}'...")
    # create the table schema from the parquet file
    client.create_table(
        table=txn_table_name,
        search_uri=file_path,
        branch=txn_branch_name,
        replace=True,
    )
    # import the actual data
    import_state = client.import_data(
        table=txn_table_name,
        search_uri=file_path,
        branch=txn_branch_name,
        client_timeout=60 * 60,
    )
    assert not import_state.error, f"Import failed: {import_state.error}"
    row_count = client.query(
        f"SELECT COUNT(*) AS n FROM {txn_table_name}", ref=txn_branch_name
    ).to_pylist()[0]["n"]
    _ok(f"Table created and loaded — {row_count:,} rows")

    _step(11, TOTAL, "Running pipeline on transaction branch...")
    txn_run = client.run(
        project_dir="./my_project",
        ref=txn_branch_name,
        cache="off",
        parameters={"run_id": 3},
    )
    assert txn_run.job_status == "SUCCESS", f"Run failed: {txn_run.job_status}"
    _ok(f"Pipeline succeeded (job {txn_run.job_id})")

    # everything worked — merge to main and clean up
    _step(12, TOTAL, "Merging to main and cleaning up...")
    client.merge_branch(source_ref=txn_branch_name, into_branch="main")
    client.delete_branch(txn_branch_name)
    _ok("Merged and branch deleted")

    _header("Done. See you, Space Cowboy.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Git-for-data tour: branching, time travel, and multi-step transactions."
    )
    parser.add_argument(
        "--file-path",
        type=str,
        required=True,
        help="S3 URI to a Parquet file reachable by your Bauplan environment "
        "(e.g. s3://my_bucket/yellow_tripdata_2021-01.parquet)",
    )
    args = parser.parse_args()
    main(args.file_path)
