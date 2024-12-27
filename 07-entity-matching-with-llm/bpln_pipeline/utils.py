"""

We separate the DAG logic from implementation details (like data cleaning) to keep the model.py file 
clean and to allow easy re-use of the same functions in different parts of the pipeline (i.e. 
apply the same changes to both Walmart and Amazon data).

"""


def clean_and_serialize_products(
    df # a pandas DataFrame
):
    import math 

    # given the "price" column, we create a new column price_range by rounding the price to the nearest power of 10
    # note that we also cast the result to a string to make it easier to serialize
    # NOTE: log10 should get a positive number, but we filtered out in the bauplan model
    # for price > 0.0
    df['price_range'] = df['price'].apply(lambda x: 10 ** math.ceil(math.log10(x))).astype(str)
    # for category, we split the string by the first space and take the first element
    df['category'] = df['category'].apply(lambda x: x.split('-')[0])
    # finally, we serialize the product information in a new column
    col_to_serialize = ['title', 'category', 'price_range', 'brand']
    serialization_pattern = 'Title: {} Category: {} Price range: {} Brand: {}'
    df['serialized_product'] = df[col_to_serialize].apply(lambda x: serialization_pattern.format(*x), axis=1)
    
    # return only the columns we need
    return df[['id', 'serialized_product', 'brand']]
    