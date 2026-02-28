# Data Engineering with Claude Code

Use [Claude Code](https://docs.anthropic.com/en/docs/claude-code) with custom Bauplan skills to build a satellite telemetry ingestion pipeline through a guided, narrative-driven experience. The demo walks through three iterations: building a naive pipeline, discovering data quality issues, and adopting the Write-Audit-Publish (WAP) pattern - all with Bauplan's branch isolation protecting production data while AI builds the pipeline.

## Overview

The scenario simulates a data team ingesting satellite telemetry data into a lakehouse. Across three narrative iterations, Claude Code scaffolds the pipeline, adds validation, and implements the WAP pattern:

1. **Introduction** (`narration/00-introduction.md`) - set up a naive ingestion workflow that imports telemetry from S3 into bronze and silver layers, then merges to main with minimal validation.
2. **In Plain Sight** (`narration/01-in-plain-sight.md`) - discover data quality issues by running a validation pipeline against the silver layer on main. Type errors, nulls, and duplicate keys surface.
3. **Healthy Commitment** (`narration/02-healthy-commitment.md`) - move validation into the pipeline itself and adopt WAP: write to a staging branch, audit with quality gates, publish to main only if checks pass. Failed branches stay open for debugging.

### Layout

- `narration/` - narrative iterations that guide the demo
- `lakehouse_workflow/` - Python scripts for lakehouse operations (created during the demo)
- `challenged_pipeline/` - Bauplan data pipeline (created during the demo)

## Getting Started

To begin the challenge, prompt your LLM with:

> I would like to start the Data Engineering with Claude Code demo.

The LLM will guide you through the narrative iterations in `narration/`, starting with `00-introduction.md`.

## Key takeaways

- Claude Code with custom Bauplan skills can scaffold a complete data pipeline from natural language prompts
- Bauplan supports medallion architectures (bronze and silver layers) natively - each layer is a model in the DAG
- Quality checks can run inside the pipeline itself, catching type mismatches, nulls, and duplicate keys during transformation rather than after materialization
- The WAP pattern with Bauplan branches lets you write to a staging branch, audit with quality gates, and publish to main only on success
- Branch isolation keeps failed pipeline runs accessible for debugging without polluting production
