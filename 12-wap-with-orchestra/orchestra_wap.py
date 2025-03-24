"""

    This is a simple stand-alone script that runs a WAP ingestion pipeline: the 
    script is meant to be picked up by an Orchestra flow, and it uses Bauplan
    as a programmable lakehouse to perform the ingestion and quality checks.
    in pure Python.
    
    Note how much lighter the integration is compared to other datalake tools ;-)

"""


import bauplan
import os


def source_to_iceberg_table(
    bauplan_client: bauplan.Client,
    table_name: str,
    namespace: str,
    source_s3_pattern: str,
    bauplan_ingestion_branch: str
):
    """
    
    Wrap the table creation and upload process in Bauplan.
    
    """
    # if the branch already exists, we delete it and create a new one
    if bauplan_client.has_branch(bauplan_ingestion_branch):
        bauplan_client.delete_branch(bauplan_ingestion_branch)
    
    # create the branch from main
    bauplan_client.create_branch(bauplan_ingestion_branch, from_ref='main')
    # create namespace if it doesn't exist
    if not bauplan_client.has_namespace(namespace, ref=bauplan_ingestion_branch):
        bauplan_client.create_namespace(namespace, branch=bauplan_ingestion_branch)
    
    # now we create the table in the branch
    bauplan_client.create_table(
        table=table_name,
        search_uri=source_s3_pattern,
        namespace=namespace,
        branch=bauplan_ingestion_branch,
        # just in case the test table is already there for other reasons
        replace=True  
    )
    # now we import the data
    is_imported = bauplan_client.import_data(
        table=table_name,
        search_uri=source_s3_pattern,
        namespace=namespace,
        branch=bauplan_ingestion_branch
    )

    return is_imported


def run_quality_checks(
    bauplan_client: bauplan.Client,
    bauplan_ingestion_branch: str,
    namespace: str,
    table_name: str
) -> bool:
    """
    
    We check the data quality by running the checks in-process: we use 
    Bauplan SDK to query the data as an Arrow table, and check if the 
    target column is not null through vectorized PyArrow operations.
    
    """
    # we retrieve the data and check if the table is column has any nulls
    # make sure the column you're checking is in the table, so change this appropriately
    # if you're using a different dataset
    column_to_check = 'passenger_count'
    # NOTE:  you can interact with the lakehouse in pure Python (no SQL)
    # and still back an Arrow table (in this one column) through a performant scan.
    wap_table = bauplan_client.scan(
        table=table_name,
        ref=bauplan_ingestion_branch,
        namespace=namespace,
        columns=[column_to_check]
    )
    # we return a boolean, True if the quality check passed, False otherwise
    return wap_table[column_to_check].null_count > 0


def wap_with_bauplan():
    """
    Run the WAP ingestion pipeline using Bauplan in Orchestra.
    
    """
    
    # get some vars from orchestra environment
    s3_path = os.getenv("S3_PATH")
    bauplan_api_key = os.getenv("BAUPLAN_API_KEY")
    # start the Bauplan client
    bauplan_client = bauplan.Client(api_key=bauplan_api_key)
    username = bauplan_client.info().user.username
    # change the vars here if you wish to use a different table / namespace
    ingestion_branch = f'{username}.wap_with_orchestra'
    table_name = 'trip_wap'
    namespace = 'orchestra'
    
    ### THIS IS THE WRITE
    # first, ingest data from the s3 source into a table the Bauplan branch
    source_to_iceberg_table(
        bauplan_client,
        table_name, 
        namespace,
        s3_path,
        ingestion_branch
    )
    ### THIS IS THE AUDIT
    # we query the table in the branch and check we have no nulls
    is_check_passed = run_quality_checks(
        bauplan_client,
        ingestion_branch,
        namespace=namespace,
        table_name=table_name
    )
    assert is_check_passed, 'Quality checks failed'
    # THIS IS THE PUBLISH 
    # finally, we merge the branch into the main branch if the quality checks passed
    bauplan_client.merge_branch(
        source_ref=ingestion_branch,
        into_branch='main'
    )
    bauplan_client.delete_branch(ingestion_branch)
    
    return


if __name__ == "__main__":
    wap_with_bauplan()
    