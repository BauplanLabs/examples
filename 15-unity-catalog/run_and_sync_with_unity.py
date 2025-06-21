from databricks import sql
import os
import bauplan
from dotenv import load_dotenv
from pathlib import Path
# load the environment variables from a .env file and assert they are set
load_dotenv()
assert os.getenv("DATABRICKS_HOST") is not None, "DATABRICKS_HOST environment variable is not set"
assert os.getenv("DATABRICKS_TOKEN") is not None, "DATABRICKS_TOKEN environment variable is not set"
assert os.getenv("DATABRICKS_PATH") is not None, "DATABRICKS_PATH environment variable is not set"
assert os.getenv("ICEBERG_BUCKET") is not None, "ICEBERG_BUCKET environment variable is not set"
assert os.getenv("STORAGE_CREDENTIALS") is not None, "STORAGE_CREDENTIALS environment variable is not set"


def test_reading_from_dx(
    connection
): 
    connection = sql.connect(
            server_hostname = os.getenv("DATABRICKS_HOST"),
            http_path = os.getenv("DATABRICKS_PATH"),
            access_token = os.getenv("DATABRICKS_TOKEN"),
        )

    cursor = connection.cursor()
    cursor.execute("SELECT 1 from range(1)")
    rows = cursor.fetchall()
    assert len(rows) == 1, "Expected one row from the test query"
    assert rows[0][0] == 1, "Expected the value to be 1 from the test query"
    cursor.close()
    
    return True


def create_external_location(
    connection,
    iceberg_bucket: str,
    storage_credential: str
):
    cursor = connection.cursor()
    create_location_query = f"""
        CREATE EXTERNAL LOCATION IF NOT EXISTS my_external_location
        URL 's3://{iceberg_bucket}'
        WITH (STORAGE CREDENTIAL {storage_credential});
    """
    cursor.execute(create_location_query)
    cursor.close()
    
    return True


def create_iceberg_table( 
    connection,
    table_name: str,
    metadata_location: str,
):
    cursor = connection.cursor()
    create_table_query = f"""
        CREATE TABLE {table_name}
        USING iceberg
        LOCATION '{metadata_location}'
    """
    cursor.execute(create_table_query)
    cursor.close()
    
    return True


def run_bauplan_pipeline_on_a_branch():
    client = bauplan.Client(profile='prod')
    username = client.info().user.username
    assert username is not None
    source_branch_name = 'main'
    my_branch_name = f'{username}.unity_catalog_sync'
    # clean up the branch if it exists - this is a demo flow!
    if client.has_branch(my_branch_name):
        client.delete_branch(my_branch_name)
    # create a new branch from the source branch
    client.create_branch(my_branch_name, from_ref=source_branch_name)
    path = Path(__file__).parent.absolute()
    state = client.run(
        project_dir=os.path.join(path, 'bpln_pipeline'),
        ref=my_branch_name
    )
    if state.job_status.lower() != "success":
        raise Exception(f"Bauplan run failed with status: {state.job_status}")
    # make sure the table is created
    assert client.has_table('unity_trips', ref=my_branch_name), "The table 'unity_trips' was not created in the branch"
    
    return client.get_table('unity_trips', ref=my_branch_name).metadata_location


def run_and_sync_with_unity(
    server_hostname: str,
    http_path: str,
    access_token: str,
    iceberg_bucket: str,
    storage_credential: str,
):
    print("Running bauplan and syncing with Unity Catalog...")
    connection = sql.connect(
            server_hostname = server_hostname,
            http_path = http_path,
            access_token = access_token,
        )
    try:
        # Test reading from Databricks SQL
        is_reachable = test_reading_from_dx(connection)
        if is_reachable:
            print("====> Databricks SQL connection is reachable.")
        
        # -- Step 1: Create an external location for Iceberg
        create_external_location(
            connection,
            iceberg_bucket=iceberg_bucket,
            storage_credential=storage_credential
        )
        print(f"====> External location created for bucket: {iceberg_bucket}")
        
        # -- Step 2: Run the Bauplan pipeline to create the Iceberg table
        metadata_location = run_bauplan_pipeline_on_a_branch()
        json_file = metadata_location.split('/')[-1]
        print(f"====> Bauplan pipeline completed. Metadata location: {json_file}")
    
        # -- Step 3: Create the external Iceberg table
        create_iceberg_table(
            connection,
            table_name='unity_trips_from_bauplan',
            metadata_location=metadata_location
        )
        
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        connection.close()    
    
    return
    


if __name__ == "__main__":
    
    run_and_sync_with_unity(
        server_hostname=os.getenv("DATABRICKS_HOST"),
        http_path=os.getenv("DATABRICKS_PATH"),
        access_token=os.getenv("DATABRICKS_TOKEN"),
        iceberg_bucket=os.getenv("ICEBERG_BUCKET"),
        storage_credential=os.getenv("STORAGE_CREDENTIALS")
    )