# LLM to Tabular Pipelines with Bauplan

Transform unstructured data from financial PDFs (SEC 10-Q filings) into structured, analyzable tabular datasets using a Large Language Model. The pipeline ingests raw PDFs, extracts relevant financial data, and structures it into a final dataset for analysis and visualization.

## Overview

Given a set of financial PDFs (Amazon SEC 10-Q filings are included as sample data), we convert unstructured information into structured [Iceberg](https://iceberg.apache.org/) tables that reside in object storage alongside the raw files - with no ad hoc infrastructure, full versioning, and easy replicability.

The final dataset is explored using a [Streamlit](https://streamlit.io/) application that fetches data directly from Bauplan via its Python APIs.

Credits: The financial PDFs come from the [Llama Index SEC 10-Q dataset](https://llamahub.ai/).

## Additional setup

### Managing secrets

The pipeline requires an LLM API key. Bauplan has a built-in secrets manager - when you set a parameter with `--type secret`, the value is encrypted via KMS and stored as ciphertext in your `bauplan_project.yml`. The plaintext never touches disk or version control.

```bash
cd bpln_pipeline
bauplan parameter set openai_api_key <key> --type secret
```

At runtime, your pipeline function declares the secret as a parameter and Bauplan decrypts and injects it automatically:

```python
def sec_10_q_tabular_dataset(
    data=bauplan.Model("sec_10_q_markdown"),
    open_ai_key=bauplan.Parameter("openai_api_key"),  # decrypted at runtime
):
```

Secrets are set at the project level, so you can experiment on a development branch without re-setting them.

### S3 configuration

The script uploads the sample PDFs to S3 before importing them into Bauplan. You'll need:

1. **AWS credentials** configured locally - the script uses `boto3`, which picks up credentials from environment variables or `~/.aws/credentials`. See the [AWS CLI configuration guide](https://docs.aws.amazon.com/cli/v1/userguide/cli-configure-files.html) if you haven't set this up.

2. **An S3 bucket you own** with:
   - **Write access** for your AWS credentials (to upload the PDFs)
   - **Public read access** (list + get) so the Bauplan sandbox can import data from it

## Data flow

The `run.py` script orchestrates the end-to-end process using the Bauplan SDK:

1. **Data ingestion**: local PDF files are uploaded to S3 object storage.
2. **Metadata management**: a table in Bauplan stores metadata (S3 locations, company, quarter, etc.) on an isolated branch.
3. **LLM processing**: the pipeline in `bpln_pipeline/` performs unstructured-to-structured transformation via an LLM, post-processes the extracted data in Python, and stores the final structured table.
4. **Production deployment**: if no errors occur, the branch is merged into `main`.
5. **Visualization**: the Streamlit app in `app/` provides a web interface to explore the transformed dataset.

## Run

```bash
cd ..
uv run python run.py \
    --ingestion_branch <YOUR_USERNAME>.ingestion_branch \
    --s3_bucket <YOUR_BUCKET>
```

### Verify results

```bash
bauplan branch checkout main
bauplan table get my_pdfs.sec_10_q_analysis
bauplan query "SELECT report_company, statement, usd FROM my_pdfs.sec_10_q_analysis ORDER BY usd DESC"
```

### Explore in Streamlit

```bash
cd app
uv run streamlit run explore_analysis.py
```

The dashboard displays a horizontal bar chart of mean USD values per financial statement type (e.g., net product sales, net service sales, net income) extracted from the SEC 10-Q filings by the LLM pipeline.

## Key takeaways

- `bauplan.Parameter` with `--type secret` encrypts API keys at rest and decrypts them only inside the pipeline function
- `internet_access=True` can be scoped to individual models, so outbound network access is limited to only the functions that need it
- A single DAG can mix PDF parsing (boto3 + markitdown), LLM extraction (Pydantic structured outputs), and tabular aggregation (Polars) - each model with its own dependencies and Python version
- The Bauplan SDK lets you orchestrate the full lifecycle - upload, ingest, transform, merge - from a single script
