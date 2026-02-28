# Iteration 2: Healthy Commitment - Implementing WAP Workflow


## Overview

Our data team has identified clear improvements that can be made to data quality, but are hesitant to make a significant change to their architecture. What is the shortest path to ensuring only cleaned data is used by their analysis software? Medallion architecture allows them to filter data before it goes into the silver layer, but removing that data directly or transactionally has its limitations. Transactional branches are a distinct approach they've heard of but not yet tried. Maybe a quick prototype will help them determine if it's worth a longer term commitment or if they'll need to go back to an older approach.


## Agent Task List


### Phase 1: Add Validation to Data Pipeline

Update the data pipeline in `challenged_pipeline/` to include validation logic that runs during the bronze-to-silver transformation.

- [ ] Update or create validation models in `challenged_pipeline/`
  - Prefer PyArrow or datafusion-python for pipeline functions
  - [ ] Add validation for `value` column: must be `double` (float64) type
  - [ ] Add validation for null checks: `value`, `dateTime`, `signal` must not be null
  - [ ] Add validation for uniqueness: composite key (`signal`, `dateTime`) must be unique
  - [ ] Validation should fail the pipeline run if data quality issues are found

- [ ] Update the silver layer model in `challenged_pipeline/models.py`
  - Prefer PyArrow or datafusion-python for pipeline functions
  - [ ] Include data filtering or assertions within the transformation
  - [ ] Document validation logic in model docstrings


### Phase 2: Adopt WAP Pattern in Workflow Orchestration

Use the WAP skill (see `.claude/skills/bauplan-wap/SKILL.md`) to update the workflow orchestration so that validation happens before publishing to main.

- [ ] Review the WAP pattern: Write → Audit → Publish on transactional branches
- [ ] Update `lakehouse_workflow/__main__.py` to follow WAP pattern
  - [ ] Keep staging branch open after pipeline runs
  - [ ] Only merge to main if pipeline succeeds with all validations passing
  - [ ] Preserve failed branches for debugging


### Phase 3: Execution

Run the updated WAP workflow to see transactional branches in action, observing how bad data is caught before reaching production.

- [ ] Execute WAP workflow with env file:
  - `uv run --env-file .env.example python -m lakehouse_workflow`
- [ ] Observe validation failures (if data quality issues exist)
- [ ] Investigate staging branch to see problematic data in isolation
- [ ] Confirm main branch remains clean


## Technical Details


### Validation in the Pipeline vs. Validation Around the Pipeline

This iteration moves validation INTO the data pipeline itself, not just around it. The pipeline
validates data as part of the bronze-to-silver transformation, causing the pipeline run to fail if
data quality issues are found.

Benefits:
- Validation logic is part of the pipeline definition (versioned with the code)
- Failed runs are visible in job history
- Bad data never materializes in the silver layer, even on staging branches
- Clear separation between infrastructure concerns (orchestration) and data concerns (pipeline logic)


### Transactional Branches

Transactional branches keep failed pipeline runs accessible for debugging without polluting
production. Unlike traditional database rollback, the staging branch remains open for investigation
when validations fail.


## Considerations

Traditional approaches to data quality:
- **Pre-materialization validation**: Requires scanning data twice (once for validation, once for loading)
- **Post-materialization validation**: Bad data reaches the lakehouse before being caught
- **Transactional rollback**: Bad data is lost, making debugging difficult

Transactional branches with pipeline-integrated validation provide a better approach: validate during transformation, fail fast, and keep all data states accessible for debugging.
