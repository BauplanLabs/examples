# Git for Data

A deep dive into Bauplan's git-for-data model through a series of SDK commands that go beyond basic pipeline runs. The script `git_for_data_tour.py` walks through 12 steps that cover branching, time travel, tagging, fault isolation, reverts, and multi-step transactions - all driven by the Python SDK.

For an exhaustive introduction to the core concepts of git for data see https://docs.bauplanlabs.com/concepts/git_for_data

## Overview

The tour uses a small helper pipeline in `my_project/` that materializes a table with a `run_id` column. By varying `run_id` across runs, the script builds up a commit history and then explores it using Bauplan's versioning primitives.

The 12 steps break into two parts:

### Part 1 — Git primitives

1. **Create a branch and run a pipeline** — branches are zero-copy: the new branch shares the same HEAD as `main` until a run produces a new commit.
2. **Run a second pipeline** — now the branch has two commits, each with a different `run_id`.
3. **Time travel** — query the same table at a previous commit hash and get the old `run_id` back.
4. **Tag a commit** — mark the first commit with a compliance tag for later retrieval.
5. **Audit by author and tag** — filter the commit history by author name, then query the table by tag to confirm it returns the compliant dataset.
6. **Fault isolation** — trigger a deliberate error (`run_id=5` raises an exception in the pipeline). The failed run is sandboxed — the branch still shows `run_id=2`.
7. **Revert** — roll the table back to the tagged commit with `client.revert_table()`. HEAD now shows `run_id=1` again.
8. **Clean up** — delete the branch and tag.

### Part 2 — Multi-step transactions

9. **Create a transaction branch** — a fresh branch to sandbox the entire workflow.
10. **Ingest data from S3** — `create_table()` infers the schema from a Parquet file, then `import_data()` loads the rows.
11. **Run a pipeline on the branch** — transform the ingested data.
12. **Merge to main** — everything succeeded, so the branch is merged and deleted. If any step had failed, `main` would be untouched.

## Run

```bash
uv run python git_for_data_tour.py \
    --file-path s3://alpha-hello-bauplan/taxi-2024/yellow_tripdata_2021-01.parquet
```

The script is fully self-contained: it creates branches, runs pipelines, performs assertions, and cleans up after itself.

## Key takeaways

- Branches are zero-copy forks — creating one is instant and costs no additional storage until new data is written
- Every commit records which job produced it (`bpln_job_id`), giving full lineage from any table state back to the run that created it
- Time travel lets you query any historical version of a table by passing a commit ref instead of a branch name
- Tags give human-readable names to specific commits — useful for compliance snapshots or release markers
- Failed pipeline runs are transactional: they never write partial results to the branch
- `revert_table()` restores a table to a previous state without rewriting history — it creates a new commit
- Branches can sandbox entire multi-step workflows (ingest, transform, merge), not just individual pipeline runs
