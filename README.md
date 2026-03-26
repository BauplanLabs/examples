<p align="center">
  <a href="https://bauplanlabs.com">
    <img src="https://public.assets.bauplanlabs.com/logo/bauplan-icon-color.png" height="96">
    <h1 align="center">Bauplan examples</h1>
  </a>
</p>

<p align="center">
  <a href="https://docs.bauplanlabs.com">Docs</a> &middot;
  <a href="https://app.bauplanlabs.com/api-keys">Get an API key</a> &middot;
  <a href="https://docs.bauplanlabs.com/tutorial/quick_start">Quick Start</a>
</p>

---

Welcome to our end-to-end examples! Each project is self-contained and walks you through a different capability - from building your first pipeline to production patterns like write-audit-publish and LLM integration.

## Prerequisites

Every example assumes the following setup:

1. **Bauplan account** - sign up at [bauplanlabs.com](https://www.bauplanlabs.com/#join) and get your [API key](https://app.bauplanlabs.com/api-keys).
2. **CLI and SDK** - install the Bauplan CLI and Python SDK. See the [installation docs](https://docs.bauplanlabs.com/tutorial/installation).
3. **uv** - we use [uv](https://docs.astral.sh/uv/) for Python dependency management. Inside any example folder, run `uv sync` to install its dependencies.
4. **Familiarity with the basics** - if you're new to Bauplan, start with the [quick start tutorial](https://docs.bauplanlabs.com/tutorial/quick_start).

> Bauplan operates entirely in the cloud. Your local scripts communicate with the platform, which orchestrates and executes data workflows on serverless infrastructure - there is nothing to provision or manage.

Some examples require additional services (Prefect, Pinecone, MongoDB, an LLM API key, AWS credentials). These are noted in the individual READMEs.

## Learn Bauplan

These examples introduce core Bauplan concepts one at a time - pipelines, data quality, branching, safe ingestion, and more. Each one focuses on a specific capability so you can learn the platform incrementally.

| # | Example | What you'll learn |
|---|---------|-------------------|
| 01 | [Pipeline to Dashboard](learn/01-pipeline-to-dashboard) | Build a two-model pipeline over NYC taxi data and visualize the output in a Streamlit dashboard |
| 02 | [Data Quality & Expectations](learn/02-data-quality-expectations) | Add expectation tests to catch data quality issues before they reach production |
| 03 | [Safe Ingestion on a Schedule](learn/03-safe-ingestion-on-a-schedule) | Implement the Write-Audit-Publish (WAP) pattern with Prefect to safely ingest data on a schedule |
| 04 | [Using LLMs with Secrets](learn/04-using-llms-with-secrets) | Ingest PDFs from S3, extract structured financial data with an LLM, and explore results in Streamlit |
| 05 | [Advanced Git for Data](learn/05-advanced-git-for-data) | Deep dive into branching, time travel, tagging, reverts, fault isolation, and multi-step transactions |
| 06 | [Data Engineering with an AI Coding Assistant](learn/06-data-engineering-with-claude-code) | Interactive narrative: build a production telemetry pipeline with an AI coding assistant and Bauplan skills |

## End-to-end applications

Full applications that combine Bauplan with third-party tools and services. Find them in the [`projects/`](projects) folder.

| Example | Description |
|---------|-------------|
| [RAG Service Support Agent](projects/RAG-service-support-agent) | RAG pipeline over Stack Overflow data with Pinecone vector search and LLM-powered Q&A |
| [Playlist Recommender](projects/build-a-playlist-recommender) | Embedding-based music recommendations with MongoDB Atlas vector search |
| [From Notebooks to Prod](projects/from-notebooks-to-prod) | From a marimo notebook to a production pipeline - same Python functions, no rewrite |
| [Medallion for Telemetry Data](projects/medallion-for-telemetry-data) | Bronze-Silver-Gold medallion architecture for sensor telemetry with DuckDB, Polars, and a dashboard |

## Learn more

- [Bauplan documentation](https://docs.bauplanlabs.com/)
- [Architecture paper](https://arxiv.org/pdf/2410.17465) — the design behind Bauplan
- [Ergonomics paper](https://arxiv.org/pdf/2602.02335) — developer experience and API design
- [bauplanlabs.com](https://www.bauplanlabs.com/) — product overview

## License

All code in this repository is released under the [MIT License](https://opensource.org/licenses/MIT). Third-party tools and services used in the examples (Prefect, Pinecone, MongoDB, etc.) are subject to their own licenses.
