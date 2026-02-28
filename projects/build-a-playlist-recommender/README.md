# Playlist Recommendations with Bauplan and MongoDB

Build an embedding-based music recommender system using Bauplan for data preparation and model training and [MongoDB Atlas](https://www.mongodb.com/products/platform/atlas-database) for serving. A [Streamlit](https://streamlit.io/) app lets users explore the embedding space and get track recommendations in real time.

We use the _Spotify Million Playlist Dataset_ (originally from [AI Crowd](https://www.aicrowd.com/)) - available as a sample dataset in the Bauplan sandbox.

See the companion [blog post](https://www.bauplanlabs.com/blog/embedding-based-recommender-systems-with-bauplan-and-mongodb) for full context on the use case.

_Credits_: The data wrangling code is adapted from the [NYU Machine Learning System Course](https://github.com/jacopotagliabue/MLSys-NYU-2022) by [Jacopo Tagliabue](https://jacopotagliabue.it/) and [Ethan Rosenthal](https://www.ethanrosenthal.com/).

## Overview

Given sequences of music tracks (Spotify playlists), the pipeline learns an embedding for each track and uses these embeddings to recommend similar tracks via vector similarity at inference time.

### Data flow

1. The original dataset is stored as a Bauplan-backed Iceberg table, available in the sandbox as "One Big Table".
2. The pipeline in `pipeline/` handles data preparation, training, and embedding persistence - both in an Iceberg table and in MongoDB Atlas.
3. The Streamlit app in `app/` retrieves embeddings from Bauplan (high throughput) and MongoDB (low latency) to explore the vector space and get recommendations.

![bpln_mongo_pipeline](https://github.com/user-attachments/assets/ae59c15b-3ad0-4605-953d-13ffda1249a1)

## Additional setup

### MongoDB Atlas

* [Sign up](https://account.mongodb.com/account/login?nds=true) for a MongoDB account and create a cluster.
* Note your connection string - find it via `Cluster > Connect > Drivers` in the Atlas console. See MongoDB's [connection string docs](https://www.mongodb.com/docs/manual/reference/connection-string/) for details.
* In `Cluster > Security > Network Access`, enable `Allow access from anywhere (0.0.0.0/0)` so Bauplan can reach the cluster.

## Run

### Explore the dataset

```bash
bauplan table get spotify_playlists
bauplan query "SELECT artist_name, artist_uri, COUNT(*) as _C  FROM spotify_playlists  GROUP BY artist_name, artist_uri ORDER BY _C DESC LIMIT 10"
```

### Run the pipeline

Create a branch and add the Mongo URI as a secret:

```bash
cd pipeline
bauplan checkout -b <YOUR_USERNAME>.music_recommendations
bauplan parameter set mongo_uri "mongodb+srv://user:pass@cluster.mongodb.net/" --type secret
bauplan run
```

Once done, verify the output:

```bash
bauplan table get track_vectors_with_metadata
bauplan query "SELECT artist_name, COUNT(*) as _C FROM track_vectors_with_metadata GROUP BY 1 ORDER BY 2 DESC LIMIT 10"
```

### Serve recommendations

Run the Streamlit app, passing your Mongo URI:

```bash
cd ../app
MONGO_URI=<YOUR_MONGO_URI> uv run python -m streamlit run explore_and_recommend.py
```

Make sure the vector search index created by the pipeline is ready before running the app (check the MongoDB Atlas dashboard).

## Key takeaways

- Bauplan handles the full ML pipeline - data preparation, model training, and vector persistence - in a single DAG
- `bauplan.Parameter` with `--type secret` securely passes connection strings and API keys to pipeline functions without exposing them in code
- Embeddings can be stored in both an Iceberg table (high throughput, batch queries) and MongoDB Atlas (low latency, real-time serving) from the same pipeline run
- Any Python app can query Bauplan tables via the SDK - this example uses a Streamlit dashboard to explore the  embedding space and serve recommendations

## Where to go from here

Building embeddings from sequences is not the only approach - you can also use track metadata (artist name, track name) to build text-based embeddings with a sentence transformer model. See the [blog post](https://www.bauplanlabs.com/blog/embedding-based-recommender-systems-with-bauplan-and-mongodb) for more on this alternative.
