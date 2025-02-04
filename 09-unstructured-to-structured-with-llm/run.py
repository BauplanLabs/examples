"""

A local script which takes local files in a folder, upload them to S3,
use Bauplan to:

* create (or re-create) a namespaced table in a branch;
* add metadata corresponding to the files to it;
* run a LLM-powered extraction pipeline (from file content to tabular data)
* save the final results in an Iceberg table;
* merge back in main over the lake.

This simple script showcases how easy is to use Bauplan SDK to
script data lake operations and run code safely over sandboxes.

Make sure to pass custom parameters to this script if you change the file location,
or the transformation pipeline.

To run with default configurations on a branch your user can write to (assuming the script 
runs in a valid AWS context for the relevant S3 bucket), run:

python run.py --ingestion-branch my_bauplan_user.ingestion_branch

"""


import bauplan
import boto3
from datetime import datetime
import pyarrow.parquet as pq
import tempfile
import glob
import pyarrow as pa
from pathlib import Path


def create_metadata_table_in_bauplan(
    bpln_client,
    s3_bucket: str,
    s3_metadata_file: str, 
    table_name: str,
    ingestion_branch: str,
    namespace: str
):
    """
    
    We leverage the bauplan SDK to create a table from the S3 URI
    and add data to it in a pure Pythonic way.
    
    For a deeper dive into the Write-Audit-Publish pattern for ingestion, see our blog post:
    
    https://www.prefect.io/blog/prefect-on-the-lakehouse-write-audit-publish-pattern-with-bauplan
    
    """
    
    # drop and recreate the ingestion branch
    # since this is a demo!
    if bpln_client.has_branch(ingestion_branch):
        bpln_client.delete_branch(ingestion_branch)
    
    bpln_client.create_branch(ingestion_branch, from_ref='main')
    
    # create namespace if not exists
    if not bpln_client.has_namespace(namespace, ingestion_branch):
        bpln_client.create_namespace(namespace, ingestion_branch)
    
    # create (or replace, it's a demo!) the table from S3 URI
    s3_uri = f's3://{s3_bucket}/{s3_metadata_file}'
    bpln_table = bpln_client.create_table(
        table=table_name,
        search_uri=s3_uri,
        branch=ingestion_branch,
        namespace=namespace,
        replace=True
    )
    # add the data
    plan_state = bpln_client.import_data(
        table=table_name,
        search_uri=s3_uri,
        branch=ingestion_branch,
        namespace=namespace,
        client_timeout=60*60
    )
    if plan_state.error:
        raise Exception(f"Error importing data: {plan_state.error}")
    
    return True


def upload_pdf_files(
        s3_client, 
        local_folder: str,
        s3_data_folder: str,
        s3_bucket,
    ):
    pdf_files = glob.glob(f"{local_folder}/*.pdf")
    s3_file_paths = []
    for pdf_file in pdf_files:
        # get the file name without the path and without the extension
        # replace any whitespace with underscores, and make it lowercase
        _file_name = Path(pdf_file).stem.replace(' ', '_').lower()
        s3_file = f"{s3_data_folder}/{_file_name}.pdf"
        s3_client.upload_file(pdf_file, s3_bucket, s3_file)
        s3_file_paths.append(s3_file)
    
    return s3_file_paths


def build_metadata_file(
    s3_client,
    s3_metadata_folder: str,
    s3_file_paths : list,
    s3_bucket : str
):
    # map the 4 letter company code to the full name
    code_to_company = {
        'aapl': 'Apple Inc.',
        'amzn': 'Amazon.com Inc.',
        'msft': 'Microsoft Corporation',
        'nvda': 'NVIDIA Corporation',
        'intc': 'Intel Corporation',
    }
    
    # create a parquet file with the metadata
    file_name = 'my_pdf_metadata.parquet'
    pydict = {
        'id': [f"sec_10_q_{i}" for i in range(len(s3_file_paths))],
        'company': [code_to_company[Path(file).stem.replace(' ', '_').lower().split('_')[-1]] for file in s3_file_paths],
        'year': [int(Path(file).stem.replace(' ', '_').lower().split('_')[0]) for file in s3_file_paths],
        'quarter': [int(Path(file).stem.replace(' ', '_').lower().split('_')[1][1:]) for file in s3_file_paths],
        'bucket': [s3_bucket for _ in range(len(s3_file_paths))],
        'pdf_path': s3_file_paths
    }
    table = pa.Table.from_pydict(pydict)
    metadata_file = f"{s3_metadata_folder}/{file_name}"
    with tempfile.NamedTemporaryFile() as tmp:
        pq.write_table(table, tmp.name)
        s3_client.upload_file(tmp.name, s3_bucket, metadata_file)
        
    return metadata_file


def upload_and_process(
    local_folder: str,
    dag_folder: str,
    s3_bucket: str,
    s3_data_folder: str,
    s3_metadata_folder: str,
    table_name: str,
    ingestion_branch: str,
    namespace: str
):
    # start processing
    print(f"\nStarting at {datetime.now()}...")
    
    # instantiate the clients
    # we assume the envs / local credentials are already set and working
    # with the target bucket and bauplan runner
    s3_client = boto3.client('s3')
    bpln_client = bauplan.Client()
    
    ### STEP 1: PDF (and their metadata) upload
    
    # upload the PDFs and get back their location in S3
    # the location is used in the metadata Iceberg table which tracks
    # the files in Bauplan 
    s3_file_paths = upload_pdf_files(
        s3_client,
        local_folder,
        s3_data_folder,
        s3_bucket
    )
    print(f"{len(s3_file_paths)} PDFs uploaded")
    s3_metadata_file = build_metadata_file(
        s3_client,
        s3_metadata_folder,
        s3_file_paths,
        s3_bucket
    )
    print("Metadata uploaded.")
    
    ### STEP 2: build the Bauplan metadata table to track files 
    
    is_created = create_metadata_table_in_bauplan(
        bpln_client=bpln_client,
        s3_bucket=s3_bucket,
        s3_metadata_file=s3_metadata_file,
        table_name=table_name,
        namespace=namespace,
        ingestion_branch=ingestion_branch
    )
    if is_created:
        print(f"Table {table_name} created successfully!")
    
    ### STEP 3: run the LLM-to-tabular pipeline in our namespace and branch
    
    run_state = bpln_client.run(
        project_dir=dag_folder,
        ref=ingestion_branch,
        namespace=namespace,
        client_timeout=60*60
    )
    print(f"Run with id {run_state.job_id} completed successfully!")
    # make sure we have the target table in the branch 
    # (change here the table name if you modify the pipeline table names)
    assert bpln_client.has_table(f'{namespace}.sec_10_q_analysis', ref=ingestion_branch)
    
    ### STEP 4: merge back to main and delete the ingestion branch
    
    bpln_client.merge_branch(source_ref=ingestion_branch, into_branch='main')
    bpln_client.delete_branch(ingestion_branch)
    
    # say goodbye
    print(f"\nProcessing done at {datetime.now()}.\n\nSee you, Space Cowboy.")
    
    return


if __name__ == '__main__':
    import argparse
    # parse arguments from the command line
    parser = argparse.ArgumentParser()
    parser.add_argument('--local_folder', type=str, default='data')
    parser.add_argument('--dag_folder', type=str, default='bpln_pipeline')
    parser.add_argument('--s3_bucket', type=str, default='alpha-hello-bauplan')
    parser.add_argument('--s3_data_folder', type=str, default='raw_pdf_dataset')
    parser.add_argument('--s3_metadata_folder', type=str, default='my_pdf_metadata')
    parser.add_argument('--table_name', type=str, default='my_pdf_metadata')
    parser.add_argument('--namespace', type=str, default='my_pdfs')
    parser.add_argument('--ingestion-branch', type=str, default='jacopo.raw_pdf_ingestion')
    args = parser.parse_args()
    # run the upload and processing    
    upload_and_process(
        local_folder=args.local_folder,
        dag_folder=args.dag_folder,
        s3_bucket=args.s3_bucket,
        s3_data_folder=args.s3_data_folder,
        s3_metadata_folder=args.s3_metadata_folder,
        table_name=args.table_name,
        ingestion_branch=args.ingestion_branch,
        namespace=args.namespace
    )        