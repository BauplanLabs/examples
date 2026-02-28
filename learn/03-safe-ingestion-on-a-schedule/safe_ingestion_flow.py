"""
This is a simple stand-alone script (it requires only Bauplan installed and the relevant credentials)
that showcases safe ingestion of parquet files to a lakehouse, i.e. Iceberg table backed by a catalog.
In particular, the script will:

* Ingest data from an S3 source into an Iceberg table
* Run quality checks on the data using Bauplan and Arrow (we assume a column named "Age" is present in the dataset, but you can change this in the code if needed)
* Merge the branch into the main branch

If dependencies are installed, just fix the global vars at the top of the script and run:

    uv run safe_ingestion_flow.py

Note how much lighter the integration is compared to other datalake tools ;-)
"""

### IMPORTS
from datetime import datetime
import bauplan
from prefect import flow, task
from prefect.cache_policies import NONE
from prefect.transactions import transaction, get_transaction


@task(cache_policy=NONE)
def source_to_iceberg_table(
    bauplan_client: bauplan.Client,
    table_name: str,
    namespace: str,
    source_s3_pattern: str,
    bauplan_ingestion_branch: str,
):
    """Wrap the table creation and upload process in Bauplan."""
    get_transaction().set("bauplan_ingestion_branch", bauplan_ingestion_branch)
    
    # Since this is a demo, we'll delete the branch and recreate it from scratch.
    bauplan_client.delete_branch(bauplan_ingestion_branch, if_exists=True)

    # Create the branch from main HEAD.
    bauplan_client.create_branch(bauplan_ingestion_branch, from_ref="main")
    
    # We check if the branch is there.
    assert bauplan_client.has_branch(bauplan_ingestion_branch), "Branch not found"
    
    # Ensure the namespace exists on the branch, create it if not.
    if not bauplan_client.has_namespace(namespace=namespace, ref=bauplan_ingestion_branch):
        print(f"Namespace '{namespace}' not found, creating it...")
        bauplan_client.create_namespace(namespace=namespace, branch=bauplan_ingestion_branch)
    
    # Now we create the table in the branch.
    bauplan_client.create_table(
        table=table_name,
        search_uri=source_s3_pattern,
        namespace=namespace,
        branch=bauplan_ingestion_branch,
        
        # Just in case the test table is already there for other reasons.
        replace=True,
    )
    
    # We check if the table is there.
    fq_name = f"{namespace}.{table_name}"
    assert bauplan_client.has_table(table=fq_name, ref=bauplan_ingestion_branch), (
        "Table not found"
    )
    is_imported = bauplan_client.import_data(
        table=table_name,
        search_uri=source_s3_pattern,
        namespace=namespace,
        branch=bauplan_ingestion_branch,
    )

    return is_imported


@task(cache_policy=NONE)
def run_quality_checks(
    bauplan_client: bauplan.Client, bauplan_ingestion_branch: str, table_name: str, namespace: str
):
    """
    This task uses the Bauplan SDK to query the data as an Arrow table,
    and checks if the target column is not null with vectorized PyArrow
    operations.
    """
    get_transaction().set("bauplan_ingestion_branch", bauplan_ingestion_branch)
    
    # We retrieve the data and check if the column has no nulls.
    # Make sure the column you're checking is in
    # the table, so change this appropriately
    # if you're using a different dataset
    column_to_check = "Age"
    
    # NOTE: If you don't want to use any SQL, you
    # can interact with the lakehouse in pure Python
    # and still get back an Arrow table (for this one
    # column) with a performant scan.
    print("Perform a S3 columnar scan on the column {}".format(column_to_check))
    fq_name = f"{namespace}.{table_name}"
    ingestion_table = bauplan_client.scan(
        table=fq_name, ref=bauplan_ingestion_branch, columns=[column_to_check]
    )
    print("Read the table successfully!")
    assert ingestion_table[column_to_check].null_count == 0, "Quality check failed"
    print("Quality check passed")

    return True


@task(cache_policy=NONE)
def merge_branch(bauplan_client: bauplan.Client, bauplan_ingestion_branch: str):
    """
    We merge the ingestion branch into the main branch. If this succeeds,
    the transaction itself is considered successful.
    """
    get_transaction().set("bauplan_ingestion_branch", bauplan_ingestion_branch)
    # We merge the branch into the main branch.
    return bauplan_client.merge_branch(
        source_ref=bauplan_ingestion_branch, into_branch="main"
    )


@source_to_iceberg_table.on_rollback
@run_quality_checks.on_rollback
@merge_branch.on_commit
def delete_branch_if_exists(transaction):
    """If the task fails or the merge succeeded, we delete the branch to avoid clutter!"""
    _client = bauplan.Client()
    ingestion_branch = transaction.get("bauplan_ingestion_branch")
    if _client.has_branch(ingestion_branch):
        print(f"Deleting the branch {ingestion_branch}")
        _client.delete_branch(ingestion_branch)
    else:
        print(f"Branch {ingestion_branch} does not exist, nothing to delete.")

    return


def _generate_branch_name(bauplan_client: bauplan.Client) -> str:
    """Generate a unique ingestion branch name from the authenticated username."""
    user = bauplan_client.info().user
    username = user.username
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"{username}.ingestion_{timestamp}"


@flow(log_prints=True)
def safe_ingestion_with_bauplan(
    source_s3_pattern: str,
    table_name: str,
    namespace: str,
):
    """
    Run the safe ingestion pipeline using Bauplan in a Prefect flow
    leveraging the concept of transactions:

    https://docs-3.prefect.io/3.0rc/develop/transactions#write-your-first-transaction
    """
    print("Starting safe ingestion at {}!".format(datetime.now()))
    bauplan_client = bauplan.Client()
    bauplan_ingestion_branch = _generate_branch_name(bauplan_client)
    print(f"Using ingestion branch: {bauplan_ingestion_branch}")
    # Start a Prefect transaction.
    with transaction():
        
        ### Write ###
        # First, ingest data from the S3 source
        # into a table on the Bauplan branch.
        source_to_iceberg_table(
            bauplan_client,
            table_name,
            namespace,
            source_s3_pattern,
            bauplan_ingestion_branch,
        )
        
        ### Audit ###
        # We query the table in the branch and check we have no nulls.
        run_quality_checks(
            bauplan_client, bauplan_ingestion_branch, table_name=table_name, namespace=namespace
        )
        
        ### Publish ###
        # Finally, we merge the branch into the main
        # branch if the quality checks passed.
        merge_branch(bauplan_client, bauplan_ingestion_branch)

    # Say goodbye.
    print("All done at {}, see you, space cowboy.".format(datetime.now()))

    return


if __name__ == "__main__":
    # Parse the args when the script is run from the command line.
    import argparse

    parser = argparse.ArgumentParser()
    
    # Table_name and s3_path are the main arguments from the command line.
    # The ingestion branch is auto-generated from the authenticated username.
    parser.add_argument("--table_name", type=str, default="titanic_from_prefect")
    parser.add_argument("--s3_path", type=str, default="s3://alpha-hello-bauplan/titanic.csv")
    parser.add_argument("--namespace", type=str, default="prefect")
    args = parser.parse_args()

    # The name of the table we will be ingesting data into.
    table_name = args.table_name
    
    # Namespace for the table: note that bauplan is the default.
    namespace = args.namespace
    
    # The S3 pattern for the data we want to ingest.
    # NOTE: If you're using Bauplan Alpha environment,
    # this should be a publicly accessible path
    # (list and get should be allowed).
    s3_path = args.s3_path
    print(
        f"Starting the safe ingestion flow with the following parameters: {table_name}, {s3_path}"
    )
    
    safe_ingestion_with_bauplan(
        source_s3_pattern=s3_path,
        table_name=table_name,
        namespace=namespace,
    )
