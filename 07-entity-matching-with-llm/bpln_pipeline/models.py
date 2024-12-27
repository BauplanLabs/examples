import bauplan


# the standard bauplan decorator to declaratively define the necessary
# Python environment and dependencies to run this function
@bauplan.python('3.11', pip={'pandas': '2.2.0'})
@bauplan.model()
def serialized_walmart_products(
    products=bauplan.Model(
      'public.walmart_products',
      # we leverage the columnar nature of the platform to only select the columns we need
      columns=[
        'id',
        'title',
        'category',
        'price',
        'brand',
      ],
      filter="price IS NOT NULL and price > 0.0"
    )
):
    """

    We convert price to a range and serialize the data to a new table following the pattern:
    
    'Title: ' || title  || ' Category: ' || category || ' Price range: ' || price_range || ' Brand: ' || brand
    
    We then produce a final table with the id (for later JOINs) and the serialized product information (note 
    that we keep the original brand column so that we can visualize predictions over certain brands
    specifically).
    
    | id | serialized_product | brand |
    |----|--------------------| ----- |
    | 1  | Title: ...         | brand1|

    """
    # print out the number of rows retrieved to the console
    print("\n\n===> Number of products retrieved: ", products.num_rows)

    # we leverage Python function to abstract away cleaning and re-use it in different parts of the pipeline
    # it is as simple as importing a function from a separate file, in a full Pythonic way!

    from utils import clean_and_serialize_products
    # convert the Arrow table to a pandas DataFrame
    df = products.to_pandas()
    final_df = clean_and_serialize_products(df)

    # as in every bauplan model, functions return a "dataframe-like" object
    # in this case, a pandas DataFrame
    return final_df


@bauplan.python('3.11', pip={'pandas': '2.2.0'})
@bauplan.model()
def serialized_amazon_products(
    products=bauplan.Model(
      'public.amazon_products',
      columns=[
        'id',
        'title',
        'category',
        'price',
        'brand',
      ],
      filter="price IS NOT NULL and price > 0.0"
    )
):
    """

    We apply the exact same transformations to the Amazon dataset as we did to the Walmart dataset above.
    The entire logic is re-used as it should be!

    """
    # print out the number of rows retrieved to the console
    print("\n\n===> Number of clean products retrieved: ", products.num_rows)

    # re-use the same logic to clean and serialize the data
    # we can squeeze the logic in a single line, as we are just re-doing
    # the same transformations as before
    from utils import clean_and_serialize_products

    return clean_and_serialize_products(products.to_pandas())


@bauplan.python('3.11', pip={'duckdb': '1.0.0', 'openai': '1.57.2'})
# bauplan allows us to declaratively define when dataframes should be materialized
# back to the data catalog, backed by object storage.
# We use the REPLACE materialization strategy to overwrite the table every time
# Note: we enable internet access to connect to the OpenAI API
@bauplan.model(materialization_strategy='REPLACE', internet_access=True)
def product_llm_matches(
    amazon_products=bauplan.Model('serialized_amazon_products'),
    walmart_products=bauplan.Model('serialized_walmart_products'),
    matching_products=bauplan.Model('public.matching_products'),
    # this will read the secret in and decrypt it ONLY in the secure worker at runtime!
    openai_api_key=bauplan.Parameter('openai_api_key'),
    max_k=bauplan.Parameter('max_k')
):
    """

    Produce a final table with the product matches based on the serialized product information and LLM calls.

    The final table has the following columns:
    
    | amazon_id | walmart_id | amazon_product | walmart_product | amazon_brand | walmart_brand | prediction | label |
    |-----------|------------|----------------|-----------------|--------------|---------------|------------|-------|
    | 1         | 1          | Title: ...     | Title: ...      | brand1       | brand1        | True       | True  |

    """
    import duckdb
    
    # we read in the matching products table to get a supply of matching requests
    # to be passed to the LLM model: we also use the max_k parameter passed in the DAG 
    # to limit the number of predictions, to make sure our API costs are under control!
    
    # we leverage duckdb to sample the minory class first (1) and then the majority class (0)
    # in the same proportion - this will produce a nicer balance when we visualize the results
    # in the web app
    sql_query = f"""
    WITH test_matches AS (
        (
            SELECT
                ltable_id, rtable_id, label::BOOLEAN as label
            FROM
                matching_products
            WHERE label = 1
            LIMIT {max_k}
        )
        UNION ALL
        (
            SELECT
                ltable_id, rtable_id, label::BOOLEAN as label
            FROM
                matching_products
            WHERE label = 0
            LIMIT {max_k}
        )
    )
    SELECT 
        t.ltable_id as walmart_id,
        t.rtable_id as amazon_id,
        t.label,
        a.serialized_product as amazon_product,
        w.serialized_product as walmart_product,
        a.brand as amazon_brand,
        w.brand as walmart_brand
    FROM 
        test_matches as t
    -- since there is no guarantee of a 100% match because of the filtering, the cardinality after the JOINs
    -- may be smaller than the max_k parameter
    JOIN amazon_products as a ON t.rtable_id = a.id
    JOIN walmart_products as w ON t.ltable_id = w.id
    """
    final_table = duckdb.sql(sql_query).arrow()
    # we should have at most top_k rows in the final table
    print(f"Final One Big Table has {final_table.num_rows} rows.")

    # finally, we connect to MongoDB to store the final vectors for later use (user facing recs)
    from llm_utils import match_with_llm
    print("\n\n=====> Start the LLM loop...\n")
    # instantiate the OpenAI client
    from openai import OpenAI
    llm_client = OpenAI(api_key=openai_api_key)
    predictions = match_with_llm(
        _product_a_list=final_table['walmart_product'].to_pylist(),
        _product_b_list=final_table['amazon_product'].to_pylist(),
        _llm_client=llm_client
    )
    print("\n\n=====> Finished the LLM loop!\n")
    # append the predictions to the final table
    final_table = final_table.append_column('prediction', [predictions])
  
    return final_table