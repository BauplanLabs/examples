# Entity matching in e-commerce with bauplan and LLMs

## Overview
This is a reference implementation of entity matching in an e-commerce setting, using [bauplan](https://www.bauplanlabs.com/) on Iceberg for data preparation and off-the-shelf LLM APIs for entity matching.

## Use case

Given two datasets of products from two different sources (Amazon and Walmart), we want to match the products that are the same across the two datasets. This is a common problem in e-commerce, where the same product can be listed with different names, descriptions, and prices across different websites. We build a bauplan pipeline to prepare the data for entity matching, and then use an off-the-shelf LLM API to perform the matching: the entire project runs end-to-end on object storage (S3), in open formats (Iceberg), using nothing else than vanilla Python code to orchestrate the DAG and integrate AI services. While we showcase product matching, the same pipeline can be easily adapted to data augmentation ("write a marketing-friendly description for this product") or data enrichment ("what are the most common attributes of this product?").

We finally build a small Streamlit app to explore the predictions made by the LLM model, leveraging again the pure Python APIs from bauplan. 

_Credits_:

* the idea of using LLMs for entity matching comes from [this paper](https://arxiv.org/pdf/2205.09911), first productionize on the "Modern Data Stack" [here](https://towardsdatascience.com/is-this-you-entity-matching-in-the-modern-data-stack-with-large-language-models-19a730373b26);
 * the Wallmart-Amazon dataset comes from the [DeepMatcher website](https://github.com/anhaidgroup/deepmatcher/blob/master/Datasets.md);
 * wrangling logic is adapted from the original [implementation](https://github.com/jacopotagliabue/foundation-models-for-dbt-entity-matching/tree/main) with SQL, Lambdas, dbt and Snowflake: note how much cleaner and straightforward the bauplan version is.

### Data flow

In the end-to-end example, the data flow as follows between tools and environments:

1. the original dataset is stored in bauplan-backed Iceberg tables: one table for Walmart, one for Amazon, one as a test set to simulate an application request for matches. When joining bauplan, the dataset is already available in the `public` namespace;
2. the pipeline in `src/bpln_pipeline` contains the data preparation and training steps as simple, decorated Python functions; running the pipeline in bauplan will execute these functions and store the result of entity matching for the _test dataset_ in a new, "big" table;
3. the streamlit app in `src/app` showcases how to get back the matches using bauplan and visualize them in a simple web interface.

Note: both the pipeline and the app code are heavily commented and written with pedagogical purposes in mind. However, do not hesitate to reach out to the bauplan team for any questions or clarifications.

## Setup

### Python environment

To run the project, you need to have Python >=3.10 installed. We recommend using a virtual environment to manage dependencies:

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### bauplan

* [Join](https://www.bauplanlabs.com/#join) the bauplan sandbox, sign in, create your username and API key;
* complete do the 3-min [tutorial](https://docs.bauplanlabs.com/en/latest/tutorial/index.html) to get familiar with the platform;
* when you gain access, public datasets (including the one used in this project) will be available for you to start building pipelines.

### Open AI 

* Sign up on [OpenAI](https://platform.openai.com/) to get your API key; you're free to experiment with different LLMs by simply replacing the LLM utility code in the pipeline.

## Run

### Check out the dataset

Using bauplan, it is trivial to get acquainted with the dataset and its schema:

```bash
bauplan table get public.amazon_products
bauplan table get public.walmart_products
bauplan table get public.matching_products
```

Note for example that `public.matching_products` contains three columns: the id of a product in the first table (the walmart one), the id of a product in the second table (the amazon one), and a label indicating whether the two products are the same or not. This table will be used to simulate an application request for matches (the `label` column will be useful to evaluate the performance of the LLM model as the ground truth!).

You can quantify the test set imbalance with a simple query directly in your CLI:

```bash
bauplan query "SELECT label, COUNT(*) as _C FROM public.matching_products GROUP BY 1"
```

### Running the pipeline with bauplan

To run the pipeline - i.e. the DAG going from the original table to the vector spaces -- you just need to create a [data branch](https://docs.bauplanlabs.com/en/latest/tutorial/02_catalog.html) to develop safely in the cloud.

```bash
cd src/bpln_pipeline
bauplan branch create <YOUR_USERNAME>.product_matching
bauplan branch checkout <YOUR_USERNAME>.product_matching
```

Now, add your OpenAI key as a secret to your project: this will allow bauplan to connect to the MongoDB cluster securely:

```bash
bauplan parameter set --name openai_api_key --value aaa --type secret
```

If you inspect your `bauplan_project.yml` file, the new parameter will be found:

```yaml
parameters:
    openai_api_key:
        type: secret
        default: kUg6q4141413...
        key: awskms:///arn:aws:kms:us-...
```

You can now run the DAG:

```bash
bauplan run
```

You can check that we successfully created the matching table with the following command:

```bash
bauplan table get product_llm_matches
```

### Exploring LLM mistakes in Streamlit

We can visualize the predictions easily in any Python environment, using the `bauplan` SDK library to interact with the tables we built by running our pipeline. We provide a simple Streamlit app to do so.

To run the app:

```bash
cd src/app
streamlit run explore_matches.py -- --bauplan_user_name <YOUR_USERNAME>

```

The app will open in your browser, and you can start exploring the predictions made by the LLM model vs the ground truth we have in the test set: feel free to modify the query in the app to slice and dice the data as you see fit (e.g. can you restrict the result for a certain category only?).

## Where to go from here?

* The old GPT3-tuned prompt from [here](https://github.com/jacopotagliabue/foundation-models-for-dbt-entity-matching?tab=readme-ov-file) could definitely use some love and improvements: how can we update the LLM-specific code to leverage the latest and greatest LLM features?
* Calling APIs sequentially (LLM or not) is slow: can we leverage bauplan powerful workers to parallelize the calls and speed up the pipeline?

## License

The code in this repository is released under the MIT License and provided as is.
