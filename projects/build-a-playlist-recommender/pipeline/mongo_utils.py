"""
Utility functions for interacting with MongoDB - in particular, functions to upload
an Arrow table as a collection in MongoDB and create a search index on the embeddings.

The index will be used in the app to provide real-time recommendations to the user based
on the vector space we trained in the data pipeline.
"""

import pyarrow as pa


def upload_vectors_to_mongodb(
    mongo_uri: str,
    _table: pa.Table,
    db_name: str,
    collection_name: str,
):
    # We import the necessary libraries.
    from pymongo.mongo_client import MongoClient
    from pymongo.server_api import ServerApi
    from pymongo.operations import SearchIndexModel

    # Create a new client and try to connect to the cluster.
    client = MongoClient(mongo_uri, server_api=ServerApi("1"))
    try:
        db = client[db_name]
        if collection_name in db.list_collection_names():
            db[collection_name].drop()
        
        # Create a new collection.
        collection = db[collection_name]
        
        # Insert the table in the MongoDB collection
        # by converting it to a list of dictionaries.
        result = collection.insert_many(_table.to_pylist())
        
        # Create a search index for the vectors.
        # Example from: https://www.mongodb.com/
        # docs/languages/python/pymongo-driver/
        # current/indexes/atlas-search-index/
        search_index_model = SearchIndexModel(
            definition={
                "fields": [
                    {
                        "type": "vector",
                        
                        # This should match the
                        # dimensionality of the vectors.
                        "numDimensions": 48,
                        "path": "embeddings",
                        "similarity": "cosine",
                    }
                ]
            },
            name="bauplan_recs_index",
            type="vectorSearch",
        )
        collection.create_search_index(model=search_index_model)

    except Exception as e:
        # Handle any MongoDB exceptions.
        print(e)

    return len(result.inserted_ids)
