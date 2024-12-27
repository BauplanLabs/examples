"""

This is a script that takes the csv files of the DeepMatcher dataset and upload them to S3
in a bucket as parquet files (file names have been changed to more explicit labels to 
facilitate table creation). Once files are in S3, they can be used to create
Iceberg tables using bauplan built-in functions and abstractions.

This code does NOT need to run as part of the reference implementation, as the dataset is already
available in bauplan's data catalog whenever a new user join the public sandbox. However, it is useful
both as a reference to the original processing and upload choices, as well as a template for users who
wish to upload their own datasets to S3 and use bauplan SDK to quickly get versionable, branchable, 
and queryable tables out of files.

To run:

python dataset_to_s3.py

Check the code for the arguments you can pass to the script.

"""


import bauplan
import boto3
from datetime import datetime
import pyarrow.parquet as pq
from pyarrow import csv
import tempfile
from pathlib import Path


def add_files_to_bauplan_catalog(
    s3_bucket: str,
    s3_folder: str,
    file_name: str,
    table_name: str,
    ingestion_branch: str
):
    """
    
    We leverage the bauplan SDK to create a table from the S3 URI
    and add data to it in a pure Pythonic way.
    
    While not the focus of this, note the use of data branches to sandbox the upload
    and safely merge new tables into the production branch of the lakehouse.
    
    For a deeper dive into the Write-Audit-Publish pattern for ingestion, see our blog post:
    
    https://www.prefect.io/blog/prefect-on-the-lakehouse-write-audit-publish-pattern-with-bauplan
    
    """
    # instantiate the bauplan client
    bpln_client = bauplan.Client()
    # drop and recreate the branch
    if bpln_client.has_branch(ingestion_branch):
        bpln_client.delete_branch(ingestion_branch)
        
    bpln_client.create_branch(ingestion_branch, from_ref='main')
    # create table from S3 URI
    s3_uri = f's3://{s3_bucket}/{s3_folder}/{file_name}'
    bpln_table = bpln_client.create_table(
        table=table_name,
        search_uri=s3_uri,
        branch=ingestion_branch,
        # we use the public namespace, and assume it's already created
        namespace='public',
        replace=True
    )
    print(f"Table {table_name} created!")
    # add the data
    plan_state = bpln_client.import_data(
        table=table_name,
        search_uri=s3_uri,
        branch=ingestion_branch,
        namespace='public',
        client_timeout=60*60
    )
    if plan_state.error:
        raise Exception(f"Error importing data: {plan_state.error}")
    # merge the branch to main
    bpln_client.merge_branch(source_ref=ingestion_branch, into_branch='main')
    
    return


def upload_and_process(
    local_dataset_file: str,
    s3_bucket: str,
    s3_folder: str,
    table_name: str,
    ingestion_branch: str
):
    # instantiate the s3 client
    # we assume the envs / local credentials are already set and working
    # with the target bucket
    s3_client = boto3.client('s3')
    # get the file name without the path and without the extension
    file_name = f'{Path(local_dataset_file).stem}.parquet'.lower()
    # read the csv, convert to parquet and upload to S3
    # NOTE: bauplan can also ingest csv files directly, 
    # but we are using parquet for performance
    with tempfile.NamedTemporaryFile() as tmp:
        parse_options = csv.ParseOptions(newlines_in_values=True)
        table = csv.read_csv(local_dataset_file, parse_options=parse_options)
        # normalize column names in a SQL-friendly way     
        table = table.rename_columns([col.lower().replace(' ', '_') for col in table.column_names])
        pq.write_table(table, tmp.name)
        print(f"File {file_name} converted to parquet.")
        s3_client.upload_file(tmp.name, s3_bucket, f"{s3_folder}/{file_name}")
        print(f"File {file_name} uploaded to S3.")
    # now that the file is in S3, we can create the Iceberg table in bauplan
    add_files_to_bauplan_catalog(
       s3_bucket=s3_bucket,
       s3_folder=s3_folder,
       file_name=file_name,
       table_name=table_name,
       ingestion_branch=ingestion_branch
    )
    return


if __name__ == '__main__':
    import argparse
    # parse arguments from the command line
    parser = argparse.ArgumentParser()
    parser.add_argument('--local_folder', type=str, default='/Users/apo/Downloads')
    parser.add_argument('--s3_bucket', type=str, default='alpha-hello-bauplan')
    parser.add_argument('--s3_folder', type=str, default='product_matching')
    parser.add_argument('--ingestion-branch', type=str, default='jacopo.matching_product_ingestion')
    args = parser.parse_args()
    
    files_in_folder = [ 
        'walmart_products.csv',
        'matching_products.csv',
        'amazon_products.csv',
    ]
    # start the upload one file at a time
    print(f"\nStarting the upload at {datetime.now()}\n")
    
    # for each file in the folder, upload to S3 and create the table
    for f in files_in_folder:
        table_name = f"{f.split('.')[0].lower()}"
        local_dataset_file = f"{args.local_folder}/{f}"
        upload_and_process(
            local_dataset_file=local_dataset_file,
            s3_bucket=args.s3_bucket,
            s3_folder=args.s3_folder,
            table_name=table_name,
            ingestion_branch=args.ingestion_branch
        )
        
    # say goodbye
    print(f"\nUploaded done at {datetime.now()}.\n\nSee you, Space Cowboy.")