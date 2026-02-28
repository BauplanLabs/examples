# Iteration 1: In Plain Sight - Discovering Data Quality Issues


## Overview

With the naive workflow in place and data merged into main, our data team realizes that the data
looks good initially but anomaly detection degrades over time. Perhaps missing data has a larger
impact than they first anticipated, and so they begin their investigation.


## Agent Task List


### Phase 1: Data Validation Pipeline

- [ ] Create pipeline execution logic `lakehouse_workflow/run_validation_pipeline.py`
  - Use Bauplan SDK to validate data in the silver layer on the main branch
  - Use environment variables:
    - Bauplan table configuration, 1 table: `SILVER_TABLE_NAMESPACE`, `SILVER_TABLE_NAME`
  - [ ] Create data validation pipeline in directory `validation_pipeline/`
    - Use `bauplan-new-pipeline` skill to create data pipeline
    - Prefer PyArrow or datafusion-python for pipeline functions
    - [ ] Implement validation: `value` column contains only values of type `double` (`float64`)
    - [ ] Implement validation: No nulls in `value`, `dateTime`, or `signal` columns
    - [ ] Implement validation: Composite key `signal, dateTime` is unique
  - [ ] Run the data pipeline ("validation pipeline") on main branch
  - [ ] Monitor job status and report results


### Phase 2: Execution

Run the validation workflow to observe the problem that will be addressed in the next iteration
(Iteration 02).

- [ ] Execute validation workflow with env file:
  - `uv run --env-file .env.example python lakehouse_workflow/run_validation_pipeline.py`


## Technical Details


### Silver Layer

The silver layer for this demo consists of one table, `telemetry.signal`, and contains filtered and
transformed data from the bronze layer.

After this iteration in the narrative, the silver layer should contain incorrect data due to our
minimal validation on the bronze layer. This represents a scenario where we are validating data
*after* it has been materialized in the lakehouse.


### Validation Rules

1. **Type validation**: The `value` column must contain only `double` (float64) values
2. **Null checks**: The columns `value`, `dateTime`, and `signal` must not contain null values
3. **Uniqueness**: The composite key (`signal`, `dateTime`) must be unique across all rows


## Considerations

Without transactional branches, other lakehouse platforms materialize data before running
validations. Some lakehouse platforms are able to validate data in a database transaction, but when
the validation fails the data is also lost (rolled back); this makes incorrect data transient and
makes debugging and triage more difficult.

