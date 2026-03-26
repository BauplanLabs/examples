# Iteration 0: Introduction - Initializing the Problematic Scenario

## Overview

We first initialize the problematic scenario that our data team finds themselves in.

This iteration sets up a naive data ingestion workflow for satellite telemetry data. Our data team
will later reveal challenges with data quality in their lakehouse due to a simple data pipeline
that ingests telemetry data and writes it to the lakehouse before validation is run. Perhaps the
lakehouse platform requires materialization before validation logic can be invoked, or perhaps the
data appeared correct initially but new batches are showing degradations.


## Prerequisites

- If `bauplan` CLI commands fail, use `uvx bauplan` to run commands
  - `uvx bauplan` doesn't require the CLI to be globally installed


## Agent Task List


### Phase 1: Preparation

- [ ] Read the README and internalize the skills in the project
- [ ] Create a `pyproject.toml` at the project root with the required dependencies (e.g. `bauplan`, `pyarrow`) so that `uv sync` can install them
- [ ] Install python dependencies: `uv sync --all-extras --all-groups`


### Phase 2: Workflow Script Creation

Create python scripts for a Bauplan lakehouse workflow in `lakehouse_workflow/` to implement the
data ingestion and transformation process. *Ingestion* refers to the whole process of *importing*
raw data from external storage (e.g. S3) into the data lakehouse then curating and transforming it
into some desirable form. This includes a "staging branch" where work is materialized and
conditionally published to production (the "main branch").

Lakehouse data is stratified into layers: a **bronze layer** containing raw data imported from S3
and a **silver layer** containing curated and transformed data from the bronze layer. This demo
represents a pipeline that does not have an additional **gold layer** (or perhaps has only a silver
and gold layer).

The workflow scripts use the Bauplan SDK to orchestrate ingestion of satellite telemetry data.

- [ ] Create module init `lakehouse_workflow/__init__.py`

- [ ] Create import logic `lakehouse_workflow/import_bronze_telemetry.py`
  - Use Bauplan SDK to import data from parquet files in S3
  - Use environment variables:
    - S3 location configuration: `S3_SOURCE_BUCKET`, `S3_SOURCE_PATH`, `S3_SOURCE_PATTERN`
    - Bauplan branch configuration: `STAGING_BRANCH`
    - Bauplan table configuration, 1 table: `BRONZE_TABLE_NAMESPACE`, `BRONZE_TABLE_NAME`
  - [ ] Create staging branch from main
  - [ ] Create bronze layer tables on staging branch
  - [ ] Import data from S3 into bronze layer

- [ ] Create pipeline execution logic `lakehouse_workflow/run_ingestion_pipeline.py`
  - Use Bauplan SDK to select and filter data from bronze layer into silver layer
  - Use environment variables:
    - Bauplan branch configuration: `STAGING_BRANCH`
    - Bauplan table configuration:
      - For bronze layer, 1 table: `BRONZE_TABLE_NAMESPACE`, `BRONZE_TABLE_NAME`
      - For silver layer, 1 table: `SILVER_TABLE_NAMESPACE`, `SILVER_TABLE_NAME`
  - [ ] Create data pipeline in directory `challenged_pipeline/`
    - Use `bauplan-new-pipeline` skill to create data pipeline
    - Prefer PyArrow or datafusion-python for pipeline functions
    - From bronze layer to silver layer
    - Simple pass-through (naive, no quality checks yet)
  - [ ] Run the data pipeline ("challenged pipeline") on staging branch
  - [ ] Monitor job status and report results

- [ ] Create pipeline commit logic `lakehouse_workflow/commit_branch.py`
  - Use Bauplan SDK to merge the staging branch into the main branch
  - Use environment variables:
    - Bauplan branch configuration: `STAGING_BRANCH`
  - [ ] Validate silver layer on staging branch
    - check that row count is non-zero
    - If validation is successful:
      - [ ] Merge staging branch into main
      - [ ] Delete staging branch
    - If validation fails:
      - [ ] report error

- [ ] Create workflow executor `lakehouse_workflow/__main__.py`
  - Use Bauplan SDK to execute an end-to-end lakehouse workflow
  - [ ] Execute workflow logic in order: import → execute pipeline → commit
  - [ ] Handle errors and provide clear status messages


### Phase 3: Execution

Run the naive workflow to prepare for the next iteration (Iteration 01: In plain sight).

- [ ] Execute naive workflow with env file:
  - `uv run --env-file .env.example python -m lakehouse_workflow`


## Technical Details

### Data Source
We use an S3 bucket available to Bauplan alpha users for imports. Data can be uploaded to this
bucket through the alpha webapp.

- **S3 Bucket**: `bauplan-alpha-user-import-uploads`
- **S3 Path**: `user-uploads/case-study-intella/telemetry/raw/`


### Bronze and Silver Layers
The bronze layer for this demo consists of one table, `telemetry.signal_bronze`, and contains raw
imported data from parquet files in S3.

The silver layer for this demo consists of one table, `telemetry.signal`, and contains filtered and
transformed data from the bronze layer.

After this iteration in the narrative, the silver layer should contain incorrect data due to our
minimal validation on the bronze layer. This represents a scenario where data is materialized in
the lakehouse *before* it is **correctly** validated.


### Environment Configuration
The environment file `.env.example` can be used directly or as a template to create your own
environment file, e.g. `.env`.

An environment file can be used with `uv` via the `--env-file` run option and it provides an
explicit method to define environment variables. This demo uses environment files for S3 and
Bauplan configuration such as data locations, table names, and authentication profiles.

