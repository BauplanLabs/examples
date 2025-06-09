# From Notebook to Production with Bauplan and Marimo

This project demonstrates how to build and run a reactive, production-grade data workflow entirely in Python — using [Marimo](https://marimo.io) for notebooks and [Bauplan](https://bauplanlabs.com) for serverless data access and transformation.

> No DSLs. No Spark. No fragile notebooks. Just Python.



## Setup

### Install `uv` (Python package manager)
```bash
curl -Ls https://astral.sh/uv/install.sh | bash
source ~/.profile  # or ~/.zshrc / ~/.bashrc
uv --version
```

### Create a virtual environment and install dependencies
```bash
uv venv .venv
source .venv/bin/activate
uv pip install -r pyproject.toml
```
## Run the Marimo Notebook
Start the notebook with:
```bash 
marimo edit taxi_notebook.py
```

This will open a fully interactive, reactive notebook UI in your browser at http://localhost:2719.

### What the Notebook Does
This notebook demonstrates how to:
- Query live data from a lakehouse using the Bauplan Python SDK.
- Join datasets (NYC taxi trips and taxi zones).
- Compute per-zone statistics (log-transformed trip miles).

Do all of this in a fully reactive, type-safe Python environment with zero infrastructure.

### Data Flow 
From Bauplan-managed Iceberg tables in the Bauplan sandbox:
- Load `taxi_fhvhv` a table with data from taxi trips.
- Load `taxi_zones` a table with NYC taxi zone metadata.
Then:
- Join trips with zone info on `PULocationID`. 
- Clean and filter rows
  - Drop zero or huge mileage trips. 
  - Exclude records before Jan 1, 2022. 
- Add a log-transformed trip distance. 
- Group by zone and compute the median log-trip-distance.
- View results in real time — every edit recomputes the pipeline automatically. 

## Run the pipeline
This repo includes two Bauplan models, defined in `models.py`, that reuse the exact same functions from the notebook.

To run the pipeline create a Bauplan branch first: 
```bash
bauplan checkout --branch <your_bauplan_username>.<your_branch_name>
```

then run the pipeline:
```bash
bauplan run
```

This will:
- Read input data from S3 (on your current branch)
- Join trips and zones
- Compute zone-level stats
- Materialize the results as a new table in the same branch

## Summary
This example demonstrates how Bauplan and Marimo can:

- Enable seamless transition from notebook exploration to production pipelines
- Reuse Python functions from interactive analysis in cloud-scale DAGs
- Access and process large datasets directly from S3 with no local setup
- Simplify reproducible, versioned data workflows through Git-style branching
- Empower data scientists to own production logic without infrastructure overhead

## License
The code in this repository is released under the MIT License.
