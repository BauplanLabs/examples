from databricks import sql
import os
import bauplan
from dotenv import load_dotenv
from pyiceberg.catalog import load_catalog
from pathlib import Path
import json
from urllib.parse import urlparse
import boto3
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


def sync_iceberg_table( 
    table_name: str,
    table_schema: str,
    parquet_files: list,
    connection
):
    # Load the Unity Catalog
    catalog = load_catalog('unity_catalog')
    print("Unity Catalog loaded successfully.")
    _table_exists = catalog.table_exists(f"default.{table_name}")
    print(f"{table_name} exists: {_table_exists}")
    if not _table_exists:
        tbl = catalog.create_table(
            identifier=f"default.{table_name}",
            schema=table_schema,
        )
        print(f"Table {table_name} created successfully.")
    else:
        print(f"Table {table_name} already exists, skipping creation.")

    return insert_parquet_files_into_iceberg_table(
        connection=connection,
        table_name=table_name,
        parquet_files=parquet_files
    )


def insert_parquet_files_into_iceberg_table(
    connection,
    table_name: str,
    parquet_files: list
):
    connection = sql.connect(
        server_hostname = os.getenv("DATABRICKS_HOST"),
        http_path = os.getenv("DATABRICKS_PATH"),
        access_token = os.getenv("DATABRICKS_TOKEN"),
    )

    cursor = connection.cursor()
    for parquet_file in parquet_files:
        cursor.execute(f"""
            INSERT INTO {table_name}
            SELECT * FROM parquet.`{parquet_file}`
        """)
    connection.commit()
    cursor.close()
    
    return True


def run_bauplan_pipeline_on_a_branch(
    # this is the table created by the Bauplan pipeline
    table_name: str = 'unity_trips'
):
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
    assert client.has_table(table_name, ref=my_branch_name), "The table 'unity_trips' was not created in the branch"
    # get schema as PyArrow schema (this could be improved by using the Iceberg directly)
    first_row = client.query(f"SELECT * FROM {table_name} LIMIT 1", ref=my_branch_name)
    first_row_schema = first_row.schema
    
    return client.get_table('unity_trips', ref=my_branch_name).metadata_location, first_row_schema


def get_parquet_files_from_metadata(metadata_location: str):
    """
    Read Iceberg metadata JSON and extract Parquet file paths from the manifest files.
    Thanks chatgpt for the initial code!
    
    Args:
        metadata_location: S3 path to the metadata.json file
        
    Returns:
        List of Parquet file paths
    """
    # Parse the S3 path
    parsed = urlparse(metadata_location)
    bucket = parsed.netloc
    key = parsed.path.lstrip('/')
    
    # Read the metadata JSON from S3
    s3 = boto3.client('s3')
    response = s3.get_object(Bucket=bucket, Key=key)
    metadata = json.loads(response['Body'].read())
    
    # Get the current snapshot
    current_snapshot_id = metadata.get('current-snapshot-id')
    snapshots = metadata.get('snapshots', [])
    
    current_snapshot = None
    for snapshot in snapshots:
        if snapshot['snapshot-id'] == current_snapshot_id:
            current_snapshot = snapshot
            break
    
    if not current_snapshot:
        print("No current snapshot found")
        return []
    
    # Get the manifest list location
    manifest_list = current_snapshot.get('manifest-list')
    if not manifest_list:
        print("No manifest list found in snapshot")
        return []
    
    # Read the manifest list (Avro format)
    parsed_manifest = urlparse(manifest_list)
    manifest_bucket = parsed_manifest.netloc
    manifest_key = parsed_manifest.path.lstrip('/')
    
    response = s3.get_object(Bucket=manifest_bucket, Key=manifest_key)
    manifest_list_content = response['Body'].read()
    
    # Parse Avro manifest list to get manifest files
    import avro.io
    import avro.datafile
    from io import BytesIO
    
    parquet_files = []
    
    # Read the Avro manifest list
    bytes_reader = BytesIO(manifest_list_content)
    reader = avro.datafile.DataFileReader(bytes_reader, avro.io.DatumReader())
    
    manifest_files = []
    for record in reader:
        manifest_path = record['manifest_path']
        manifest_files.append(manifest_path)
    reader.close()
    
    # Read each manifest file to get data files
    for manifest_path in manifest_files:
        parsed_manifest = urlparse(manifest_path)
        manifest_bucket = parsed_manifest.netloc
        manifest_key = parsed_manifest.path.lstrip('/')
        
        response = s3.get_object(Bucket=manifest_bucket, Key=manifest_key)
        manifest_content = response['Body'].read()
        
        # Read the Avro manifest file
        bytes_reader = BytesIO(manifest_content)
        reader = avro.datafile.DataFileReader(bytes_reader, avro.io.DatumReader())
        
        for entry in reader:
            # Check if the entry is not deleted (status: 1 = ADDED, 2 = DELETED)
            if entry.get('status') != 2:
                data_file = entry.get('data_file', {})
                file_path = data_file.get('file_path')
                if file_path:
                    parquet_files.append(file_path)
        
        reader.close()
    
    return parquet_files


def run_and_sync_with_unity(
    table_to_sync: str,
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
        metadata_location, target_schema = run_bauplan_pipeline_on_a_branch(
            # pass the table created by the Bauplan pipeline (if successful)
            table_name=table_to_sync
        )
        print(f"====> Bauplan pipeline completed. Metadata location: {metadata_location}")
        print(f"====> Target schema: {target_schema}")
        
        # -- Step 3: Get Parquet files from metadata
        parquet_files = get_parquet_files_from_metadata(metadata_location)
        print(f"====> Found {len(parquet_files)} Parquet files:")
        for file in parquet_files:
            print(f"      - {file}")
    
        # -- Step 4: Create the external Iceberg table (if not exists) and add the files (zero-copy)
        sync_iceberg_table(
            table_name=table_to_sync,
            table_schema=target_schema,
            parquet_files=parquet_files,
            connection=connection
        )
        
        print(f"====> Iceberg table '{table_to_sync}' synced successfully with Parquet files.")
        
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        connection.close()    
    
    return
    


if __name__ == "__main__":
    
    run_and_sync_with_unity(
        # this is the table created by the Bauplan pipeline, which we want
        # to sync with Unity Catalog
        table_to_sync='unity_trips',
        server_hostname=os.getenv("DATABRICKS_HOST"),
        http_path=os.getenv("DATABRICKS_PATH"),
        access_token=os.getenv("DATABRICKS_TOKEN"),
        iceberg_bucket=os.getenv("ICEBERG_BUCKET"),
        storage_credential=os.getenv("STORAGE_CREDENTIALS")
    )