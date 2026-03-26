import bauplan


# The standard bauplan decorator to declaratively define the necessary
# Python interpreter and dependencies to run this function.
@bauplan.python("3.11", pip={"duckdb": "1.0.0"})
@bauplan.model()
def playlists_to_sequences(
    tracks=bauplan.Model(
        "spotify_playlists",
        
        # We leverage the columnar nature of the
        # platform to only select the columns we
        # need.
        columns=["playlist_id", "track_uri", "pos"],
        
        # We filter out the playlists with less
        # than 5 tracks and less than 2 followers.
        # Bauplan is smart enough to push this filter down to the lakehouse.
        # Note how we parametrize the filter using
        # arguments (defaults are in the
        # bauplan_project.yaml).
        filter="num_followers > $num_followers and num_tracks > $num_tracks",
    ),
):
    """
    We build sequences of tracks IDs based on the playlist order.
    The end result is a table with two columns: playlist_id, track_ids:

    | playlist_id     | track_ids |
    |-----------------|-----------|
    | 112             | [1, 2, 3] |
    | 341             | [4, 5]    |
    """
    
    # Print out the number of rows retrieved to the console.
    print("\n\n===> Number of tracks retrieved: ", tracks.num_rows)
    
    # We use the duckdb library to quickly and concisely complete the group by.
    import duckdb

    sql_query = """
    SELECT 
        playlist_id,
        array_agg(track_uri ORDER BY pos ASC) as track_ids
    FROM
        tracks
    GROUP BY 1
    ORDER BY 1 ASC
    """
    data = duckdb.sql(sql_query).arrow()
    
    # We print out the number of rows returned by the query
    # due to the GROUP BY.
    print("\n\n===> Number of playlists: ", data.num_rows)

    # As in every model, functions return a "dataframe-like" object,
    # in this case, an Arrow table.
    return data


@bauplan.python("3.11", pip={"duckdb": "1.0.0"})
@bauplan.model()
def popular_tracks(
    
    # We re-use the playlists_to_sequences model as an input.
    playlists_to_sequences=bauplan.Model("playlists_to_sequences"),
    
    # We take an additional parameter to filter the top k tracks.
    top_k=bauplan.Parameter("top_k"),
):
    """
    Get the most popular tracks in the playlist tables we pre-filtered.

    The result is a table with two columns: track_id, count:

    | track_id | count |
    |----------|-------|
    | 1131     | 100   |
    """
    import duckdb

    sql_query = f"""
    SELECT track_id, COUNT(*) as count
    FROM (
        SELECT
            unnest(track_ids) as track_id
        FROM
            playlists_to_sequences
    )
    GROUP BY 1
    ORDER BY 2 DESC
    LIMIT {top_k}
    """
    rows = duckdb.sql(sql_query).arrow()
    
    # Double-check the number of rows returned == top_k.
    assert rows.num_rows == top_k
    return rows


@bauplan.python(
    "3.11",
    pip={
        "gensim": "4.3.3",
        "scikit_learn": "1.5.2",
        "duckdb": "1.0.0",
        "pymongo": "4.10.1",
    },
)
# Bauplan allows us to declaratively define when
# dataframes should be materialized
# back to the data catalog, backed by object storage.
# We use the REPLACE materialization strategy to overwrite the table every time.
# Note: We enable internet access to connect to our MongoDB cluster!
@bauplan.model(materialization_strategy="REPLACE", internet_access=True)
def track_vectors_with_metadata(
    playlists_to_sequences=bauplan.Model("playlists_to_sequences"),
    popular_tracks=bauplan.Model("popular_tracks"),
    
    # Extract metadata for user-facing purposes.
    metadata=bauplan.Model(
        "spotify_playlists",
        columns=["track_name", "artist_name", "track_uri"],
        
        # For consistency, we filter out the tracks
        # with the same criteria as before.
        filter="num_followers > $num_followers and num_tracks > $num_tracks",
    ),
    
    # This will read the secret in and decrypt it ONLY in the secure worker
    # at runtime!
    mongo_uri=bauplan.Parameter("mongo_uri"),
):
    """
    Produce a final table with the embeddings for each track, including a 2-D representation for visualization
    purposes. We use gensim to train a sequential model on track sequences, scikit-learn for TSNE.

    The final table has the following columns:

    | track_id | sequential_vectors | two_d_vectors | popularity | track_name | artist_name |
    |----------|--------------------|---------------|------------|------------|-------------|
    | 1131     | [1, 2, 3, 4]       | [1, 2]        | 100        | track1     | artist1     |
    """
    import numpy as np
    import pyarrow as pa
    import duckdb

    # We import the utility functions from the utils.py file.
    # We separated the functions to keep in the main file clean only the
    # DAG structure as bauplan functions.
    from utils import tsne_analysis, skipgram_model

    # Compute embeddings based on the track sequences in the playlists.
    model = skipgram_model(
        sequence_data=playlists_to_sequences["track_ids"].to_pylist()
    )
    print(f"Trained a total of {len(model)} vectors!")
    top_k_track_id = popular_tracks["track_id"].to_pylist()
    top_k_tracks_embeddings = np.array([model[t] for t in top_k_track_id])
    
    # Now we compute the 2D embeddings with TSNE.
    two_d_embeddings = tsne_analysis(top_k_tracks_embeddings)
    assert len(two_d_embeddings) == len(top_k_track_id)
    
    # Temp table, before joining with the content embeddings.
    table = pa.Table.from_pydict(
        {
            "track_id": top_k_track_id,
            "sequential_vectors": top_k_tracks_embeddings.tolist(),
            "two_d_vectors": two_d_embeddings.tolist(),
        }
    )
    sql_query = """
    -- just deduplicate the metadata from the playlist dataset
    WITH track_metadata AS (
        SELECT
            track_uri,
            track_name,
            artist_name
        FROM
            metadata
        GROUP BY ALL
    )
    SELECT 
        t.track_id as _id, -- this is the MongoDB primary key convention
        t.sequential_vectors as embeddings,
        t.two_d_vectors,
        popular_tracks.count as popularity,
        track_metadata.track_name as track_name,
        track_metadata.artist_name as artist_name
    FROM 
        track_table as t
    JOIN popular_tracks ON popular_tracks.track_id = t.track_id
    JOIN track_metadata ON track_metadata.track_uri = t.track_id
    """
    duckdb.register("track_table", table)
    final_table = duckdb.sql(sql_query).arrow()
    
    # There should should be at most `top_k` rows in the final table.
    print(f"Final One Big Table has {final_table.num_rows} rows.")

    # Finally, we connect to MongoDB to store the
    # final vectors used in the frontend.
    from mongo_utils import upload_vectors_to_mongodb

    print("\n\n=====> Start the MongoDB upload process...\n")
    num_doc_inserted = upload_vectors_to_mongodb(
        mongo_uri, final_table, db_name="my_bauplan_db", collection_name="track_vectors"
    )
    print(f"Inserted {num_doc_inserted} documents in the MongoDB collection")
    print("\n\n=====> Finished the MongoDB upload process!\n")

    return final_table
