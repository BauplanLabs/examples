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

uv run python run.py --ingestion_branch my_bauplan_user.ingestion_branch

"""

import bauplan
import boto3
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
    namespace: str,
):

    bpln_client.delete_branch(ingestion_branch, if_exists=True)

    bpln_client.create_branch(ingestion_branch, from_ref="main")

    # Create namespace if not exists.
    if not bpln_client.has_namespace(namespace, ingestion_branch):
        bpln_client.create_namespace(namespace, ingestion_branch)

    # Create (or replace, it's a demo!) the table from S3 URI.
    s3_uri = f"s3://{s3_bucket}/{s3_metadata_file}"
    bpln_client.create_table(
        table=table_name,
        search_uri=s3_uri,
        branch=ingestion_branch,
        namespace=namespace,
        replace=True,
    )
    # Add the data.
    plan_state = bpln_client.import_data(
        table=table_name,
        search_uri=s3_uri,
        branch=ingestion_branch,
        namespace=namespace,
        client_timeout=60 * 60,
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
        # Get the file name without the path and without the extension.
        # Replace any whitespace with underscores, and make it lowercase.
        _file_name = Path(pdf_file).stem.replace(" ", "_").lower()
        s3_file = f"{s3_data_folder}/{_file_name}.pdf"
        s3_client.upload_file(pdf_file, s3_bucket, s3_file)
        s3_file_paths.append(s3_file)
    return s3_file_paths


def build_metadata_file(
    s3_client, s3_metadata_folder: str, s3_file_paths: list, s3_bucket: str
):
    # Map the 4-letter company code to the full name.
    code_to_company = {
        "aapl": "Apple Inc.",
        "amzn": "Amazon.com Inc.",
        "msft": "Microsoft Corporation",
        "nvda": "NVIDIA Corporation",
        "intc": "Intel Corporation",
    }

    # Create a parquet file with the metadata.
    file_name = "my_pdf_metadata.parquet"
    pydict = {
        "id": [f"sec_10_q_{i}" for i in range(len(s3_file_paths))],
        "company": [
            code_to_company[Path(file).stem.replace(" ", "_").lower().split("_")[-1]]
            for file in s3_file_paths
        ],
        "year": [
            int(Path(file).stem.replace(" ", "_").lower().split("_")[0])
            for file in s3_file_paths
        ],
        "quarter": [
            int(Path(file).stem.replace(" ", "_").lower().split("_")[1][1:])
            for file in s3_file_paths
        ],
        "bucket": [s3_bucket for _ in range(len(s3_file_paths))],
        "pdf_path": s3_file_paths,
    }
    table = pa.Table.from_pydict(pydict)
    metadata_file = f"{s3_metadata_folder}/{file_name}"
    with tempfile.NamedTemporaryFile() as tmp:
        pq.write_table(table, tmp.name)
        s3_client.upload_file(tmp.name, s3_bucket, metadata_file)

    return metadata_file


def _step(n: int, total: int, msg: str):
    print(f"\n  [{n}/{total}]  {msg}")


def upload_and_process(
    local_folder: str,
    dag_folder: str,
    s3_bucket: str,
    s3_data_folder: str,
    s3_metadata_folder: str,
    table_name: str,
    ingestion_branch: str,
    namespace: str,
):
    total_steps = 4
    print(f"\n{'=' * 60}")
    print("  Bauplan LLM ingestion pipeline")
    print(f"  branch: {ingestion_branch}  |  namespace: {namespace}")
    print(f"  bucket: {s3_bucket}")
    print(f"{'=' * 60}")

    s3_client = boto3.client("s3")
    bpln_client = bauplan.Client()

    # --- upload PDFs and build metadata ---
    _step(1, total_steps, "Uploading PDFs to S3...")
    s3_file_paths = upload_pdf_files(s3_client, local_folder, s3_data_folder, s3_bucket)
    s3_metadata_file = build_metadata_file(
        s3_client, s3_metadata_folder, s3_file_paths, s3_bucket
    )
    print(f"         {len(s3_file_paths)} files uploaded, metadata written")

    # --- create the Bauplan metadata table ---
    _step(
        2,
        total_steps,
        f"Creating table '{table_name}' on branch '{ingestion_branch}'...",
    )
    create_metadata_table_in_bauplan(
        bpln_client=bpln_client,
        s3_bucket=s3_bucket,
        s3_metadata_file=s3_metadata_file,
        table_name=table_name,
        namespace=namespace,
        ingestion_branch=ingestion_branch,
    )
    print("         Table ready")

    # --- run the LLM extraction pipeline ---
    _step(3, total_steps, "Running LLM extraction pipeline...")
    run_state = bpln_client.run(
        project_dir=dag_folder,
        ref=ingestion_branch,
        namespace=namespace,
        client_timeout=60 * 60,
    )
    assert bpln_client.has_table(f"{namespace}.sec_10_q_analysis", ref=ingestion_branch)
    print(f"         Pipeline complete (job {run_state.job_id})")

    # --- merge to main and clean up ---
    _step(4, total_steps, "Merging to main and cleaning up...")
    bpln_client.merge_branch(source_ref=ingestion_branch, into_branch="main")
    bpln_client.delete_branch(ingestion_branch)
    print("         Merged and branch deleted")

    print(f"\n{'=' * 60}")
    print("  Done. See you, Space Cowboy.")
    print(f"{'=' * 60}\n")


if __name__ == "__main__":
    import argparse

    # Parse arguments from the command line.
    parser = argparse.ArgumentParser()
    parser.add_argument("--local_folder", type=str, default="data")
    parser.add_argument("--dag_folder", type=str, default="bpln_pipeline")
    parser.add_argument("--s3_bucket", type=str, default="alpha-hello-bauplan")
    parser.add_argument("--s3_data_folder", type=str, default="raw_pdf_dataset")
    parser.add_argument("--s3_metadata_folder", type=str, default="my_pdf_metadata")
    parser.add_argument("--table_name", type=str, default="my_pdf_metadata")
    parser.add_argument("--namespace", type=str, default="my_pdfs")
    parser.add_argument(
        "--ingestion_branch", type=str, required=True
    )
    args = parser.parse_args()
    # Run the upload and processing.
    upload_and_process(
        local_folder=args.local_folder,
        dag_folder=args.dag_folder,
        s3_bucket=args.s3_bucket,
        s3_data_folder=args.s3_data_folder,
        s3_metadata_folder=args.s3_metadata_folder,
        table_name=args.table_name,
        ingestion_branch=args.ingestion_branch,
        namespace=args.namespace,
    )
