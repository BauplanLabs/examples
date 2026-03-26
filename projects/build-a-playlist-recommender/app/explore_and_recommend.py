"""
This is a simple app reading the artifacts prepared by the data pipeline and saved in both
bauplan and MongoDB. We first retrieve embeddings and metadata for tracks from bauplan,
and display the 2-D TSNE vectors in a scatterplot (color-coded by author!).

We then provide a simple UI to leverage MongoDB for a user-facing recommendation system: the user
can select a track id, and we will use the pre-computed vectors to find the nearest neighbors in the space.

To run the app, simply execute:

uv run python -m streamlit run explore_and_recommend.py

The app assumes you have run the pipeline in a branch: please select the right branch to query the data
and get started!
"""

import streamlit as st
import sys
import polars as pl
import matplotlib.pyplot as plt
import bauplan
from pymongo import MongoClient
import os
import certifi


### Global Clients Setup ###

if "MONGO_URI" not in os.environ:
    raise ValueError("Please set the MONGO_URI environment variable to run the app!")

mongo_client = MongoClient(os.environ["MONGO_URI"], tlsCAFile=certifi.where())
mongo_database = mongo_client["my_bauplan_db"]

bauplan_client = bauplan.Client()


### Utility Functions ###

@st.cache_data()
def query_as_arrow(
    _client: bauplan.Client,
    sql: str,
    branch: str,
):
    """
    This function uses the query method to query a table in bauplan. This is
    handy as a separate function because we can cache the results and avoid
    querying the same data multiple times.

    It returns None if the query fails.
    """

    try:
        return _client.query(sql, ref=branch)
    except Exception as e:
        print(e)

    return None


def plot_scatterplot_with_lookup(
    title: str, items: list, items_to_target_cat: dict, vectors: list
):
    """
    Plot the 2-D vectors in the space, and use the mapping items_to_target_cat
    to color-code the points for convenience
    """
    groups = {}
    for item in items:
        item_idx = items.index(item)
        target_cat = items_to_target_cat[item]
        x = vectors[item_idx][0]
        y = vectors[item_idx][1]
        if target_cat in groups:
            groups[target_cat]["x"].append(x)
            groups[target_cat]["y"].append(y)
        else:
            groups[target_cat] = {"x": [x], "y": [y]}

    fig, ax = plt.subplots(figsize=(10, 10))
    for group, data in groups.items():
        ax.scatter(
            data["x"],
            data["y"],
            alpha=0.05 if group == "unknown" else 0.9,
            edgecolors="none",
            s=25,
            marker="o",
            label=group,
        )

    plt.title(title)
    plt.legend(loc=2)
    st.pyplot(plt)

    return


def check_search_index_availability(database, index_name, collection_name):
    collection = database[collection_name]
    indices = list(collection.list_search_indexes())
    if len(indices) == 0:
        return False
    return next(filter(lambda x: x["name"] == index_name, indices))["queryable"]


def vector_search(database, query_vector, index_name, collection_name, limit=5):
    # Some vars are hardcoded here for simplicity.
    # These should be the same as the ones in the DAG
    # if you want to change them.
    collection = database[collection_name]
    results = collection.aggregate(
        [
            {
                "$vectorSearch": {
                    "index": index_name,
                    "path": "embeddings",
                    "queryVector": query_vector,
                    "numCandidates": 50,
                    "limit": limit,
                }
            },
            # We are extracting 'vectorSearchScore' here.
            # Columns with 1 are included, columns with 0 are excluded.
            {
                "$project": {
                    "_id": 0,
                    "track_name": 1,
                    "artist_name": 1,
                    "search_score": {"$meta": "vectorSearchScore"},
                }
            },
        ]
    )
    return list(results)


### The Streamlit App Begins Here ###

def main(one_big_table_name: str, index_name: str, collection_name: str):
    st.title("Explore the vector space and get recommendations!")

    # Before doing anything, we need to check the status of the search index.
    if not check_search_index_availability(mongo_database, index_name, collection_name):
        st.write(
            "The Mongo search index is not available yet. Please wait a bit and try again!"
        )
        st.stop()
    
    # Automatically infer the bauplan username from the authenticated client.
    bauplan_user_name = bauplan_client.info().user.username
    all_branches = list(
        _.name for _ in bauplan_client.get_branches(user=bauplan_user_name)
    )
    target_branch = st.selectbox(
        f"Pick the branch with {one_big_table_name}:", all_branches, index=None
    )
    st.write(f"You selected: {target_branch}")
    if target_branch is None:
        st.write("Please select a branch to continue!")
        st.stop()
    sql_query = f"""
    SELECT 
        _id, embeddings, two_d_vectors, track_name, artist_name
    FROM 
        {one_big_table_name}
    ORDER BY 
        popularity 
    DESC
    """
    table = query_as_arrow(bauplan_client, sql_query, target_branch)
    if table is None:
        st.write("Something went wrong! Please check your branch and try again!")
        st.stop()

    st.dataframe(pl.from_arrow(table.slice(length=3)), width=1200)
    all_items = table["_id"].to_pylist()
    tracks_to_author = dict(
        zip(table["_id"].to_pylist(), table["artist_name"].to_pylist())
    )
    
    # We highlight a few authors for the scatterplot.
    target_authors = ["Drake", "Kanye West", "Justin Bieber", "Ed Sheeran", "Eminem"]
    
    # We mark as unknown the tracks not written by the target authors, so that
    # the visualization is more readable.
    for t, a in tracks_to_author.items():
        if a not in target_authors:
            tracks_to_author[t] = "unknown"
    
    # Plot the embeddings, color-coded by author.
    plot_scatterplot_with_lookup(
        title="Music in (vector) space",
        items=all_items,
        items_to_target_cat=tracks_to_author,
        vectors=table["two_d_vectors"].to_pylist(),
    )

    # Now, get some recommendations from MongoDB for the top songs.
    # Build a track lookup, mapping track to embeddings for the top 10 tracks.
    track_name_to_embedding = dict(
        zip(table["track_name"].to_pylist()[:10], table["embeddings"].to_pylist()[:10])
    )
    top_tracks = list(track_name_to_embedding.keys())
    query_track = st.selectbox("Find songs similar to:", top_tracks, index=None)
    if query_track is None:
        st.write("Please select a track to continue!")
        st.stop()

    # Get the vector corresponding to the query track selected by the user.
    query_embedding = track_name_to_embedding[query_track]
    results = vector_search(
        mongo_database, query_embedding, index_name, collection_name, limit=5
    )
    st.dataframe(pl.DataFrame(results[1:]), width=1200)

    return


if __name__ == "__main__":
    # Parse the arguments.
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--one_big_table_name", type=str, default="track_vectors_with_metadata"
    )
    args = parser.parse_args()
    
    # These are hardcoded to the same values as in the pipeline.
    # Change them here if you change them in the pipeline.
    COLLECTION_NAME = "track_vectors"
    INDEX_NAME = "bauplan_recs_index"
    
    # Start the app.
    main(
        one_big_table_name=args.one_big_table_name,
        index_name=INDEX_NAME,
        collection_name=COLLECTION_NAME,
    )
