# From Notebook to Production with Bauplan and marimo

Go from an interactive [marimo](https://marimo.io) notebook to a production Bauplan pipeline - same Python functions, no rewrite.

* [Blog post](https://www.bauplanlabs.com/blog/everything-as-python) - background and context
* [Demo video](https://www.youtube.com/watch?v=uydisCi5rWE) - 5-min live walkthrough

## Run the notebook

```bash
uv run python -m marimo edit pipeline/taxi_notebook.py
```

This opens a fully interactive notebook UI in your browser. The notebook:

- Queries live data from the Bauplan lakehouse (NYC taxi trips and taxi zones)
- Joins datasets on `PULocationID`
- Cleans rows (drops zero/huge mileage trips, excludes records before Jan 1 2022)
- Adds a log-transformed trip distance
- Computes per-zone median log-trip-distance
- Recomputes automatically as you edit - every cell is reactive

## Run the pipeline

The same functions from the notebook are reused as Bauplan models in `pipeline/models.py`. Create a branch and run:

```bash
bauplan checkout -b <YOUR_USERNAME>.<YOUR_BRANCH_NAME>
bauplan run --project-dir pipeline
```

This will read input data, join trips and zones, compute zone-level stats, and materialize the results as a new table - all in the cloud, with logs streaming to your terminal.

### Verify results

```bash
bauplan table get stats_by_taxi_zones
bauplan query "SELECT Zone, log_trip_miles FROM stats_by_taxi_zones ORDER BY log_trip_miles DESC LIMIT 10"
```

## How it works

The key trick is marimo's `@app.function` decorator. Functions marked with it are importable from outside the notebook:

```python
# in taxi_notebook.py (marimo notebook)
@app.function
def join_taxi_tables(table_1, table_2):
    return table_1.join(table_2, ...)

# in models.py (Bauplan pipeline)
from taxi_notebook import join_taxi_tables
```

The Bauplan pipeline in `models.py` imports `join_taxi_tables` and `compute_stats_by_zone` directly from the notebook. The same functions run interactively in marimo and at scale in Bauplan.

