# LLM to Tabular Pipelines with Bauplan

## Overview

This is a [Bauplan](https://www.bauplanlabs.com/) reference implementation demonstrating how to transform unstructured data from financial PDFs (SEC 10-Q filings) into structured, analyzable tabular datasets using Large Language Models (LLMs). The pipeline ingests raw PDFs, extracts relevant financial data, and structures it into a final dataset suitable for downstream analysis and visualization.

## Use Case

Given a set of financial PDFs from different companies, we aim to convert unstructured information into structured tables that:

- Reside in object storage alongside the raw files
- Run without ad hoc infrastructure
- Are cost-efficient, versioned, and easily replicable

To achieve this, we use a LLM-powered transformation pipeline within Bauplan, which offers:

- A Python runtime optimized for LLM calls
- Out-of-the-box DAG abstraction for structuring tabular dependencies
- Iceberg-backed data persistence, including:
  - Data branching for safe experimentation
  - Time-travel capabilities for reproducibility
  - Transactional guarantees for consistency

The final dataset is explored using a simple Streamlit application that fetches data directly from Bauplan via its Python APIs.

*Credits: The financial PDFs used in this example come from the **[Llama Index](https://github.com/run-llama/llama_index)** SEC 10-Q dataset.*

## Setup

### Python Environment

To set up the environment, ensure Python >=3.10 is installed and use a virtual environment:

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### Bauplan Setup

1. [Join](https://www.bauplanlabs.com/#join) the Bauplan sandbox, sign in, and create your username and API key.
2. Complete the 3-minute [tutorial](https://docs.bauplanlabs.com/en/latest/tutorial/index.html) to familiarize yourself with the platform.

### Managing Secrets

The pipeline requires API keys for OpenAI and AWS (if using S3):

```bash
bauplan parameter set --name openai_api_key --value your_key --type secret
```

These secrets are encrypted and stored in `bauplan_project.yml` for secure access at runtime.

### S3 Configuration

Ensure you have write access to the S3 bucket specified in `run.py`: the bucket will be used to storing raw PDF and metadata files. When running DAGs in the Bauplan sandbox, buckets must be [publicly readable](https://docs.bauplanlabs.com/en/latest/tutorial/04_import.html) for successful data import.

## Data Flow

The end-to-end use case is managed by the `run.py` script, which leverages the Bauplan SDK to orchestrate data processing. The workflow consists of the following steps:

1. **Data Ingestion:** Local PDF files containing financial data are uploaded to S3 object storage for durability and performance.
2. **Metadata Management:** A table in Bauplan stores metadata (S3 locations, company, quarter, etc.), ensuring efficient filtering and access. This step and all subsequent Bauplan operations occur safely within an isolated [data branch](https://docs.bauplanlabs.com/en/latest/concepts/branches.html).
3. **LLM Processing:** The pipeline in `src/bpln_pipeline` performs:
   - Unstructured-to-structured transformation via an LLM
   - Post-processing in Python to refine extracted data
   - Storage of the final structured table within the same namespace
4. **Production Deployment:** If no errors occur, the temporary branch is merged into production, making the result of the pipeline available for further analysis.
5. **Data Visualization:** The Streamlit app in `src/app` provides a simple web interface to explore the transformed dataset.

*Note: The code includes extensive comments for pedagogical purposes. Contact the Bauplan team for further inquiries.*

## Running the Pipeline

### Execute the End-to-End Pipeline

Run the following command to process PDFs and generate a structured dataset:

```bash
cd src
python run.py --ingestion-branch your_username.ingestion_branch
```

Since branches in Bauplan are user-specific, use the `user.branch_name` pattern for isolation.

### Verify Results

Check the generated table:

```bash
bauplan branch checkout main
bauplan table get my_pdfs.sec_10_q_analysis
```

Check out the maximum value for a column:

```bash
bauplan query "SELECT MAX(usd) as max_usd FROM my_pdfs.sec_10_q_analysis"
```

### Exploring Results in Streamlit

Launch the visualization app:

```bash
cd src/app
streamlit run explore_analysis.py
```

The app will open in your browser, displaying insights from the extracted financial data.

## Summary

This example demonstrates how Bauplan can:

- Handle complex data transformations with LLMs
- Efficiently manage unstructured-to-structured data pipelines
- Provide safe and reproducible data processing via branching
- Securely integrate with cloud services
- Enable rapid prototyping and analysis through Python APIs

## License

The code in this repository is released under the MIT License.

