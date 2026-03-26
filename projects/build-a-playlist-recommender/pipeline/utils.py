"""
This file contains the modeling and data science bits, while `models.py` is for the actual
transformation logic.
"""


def skipgram_model(
    sequence_data, vector_size=48, window_size=5, min_count=2, workers=12
):
    """
    Train a skipgram model for embeddings using gensim.
    """
    from gensim.models import Word2Vec

    print("Training embedding model...")
    video_2_vec = Word2Vec(
        sentences=sequence_data,
        vector_size=vector_size,
        window=window_size,
        min_count=min_count,
        workers=workers,
    )
    model = video_2_vec.wv

    return model


def tsne_analysis(embeddings, perplexity=50, n_iter=1000):
    """
    TSNE dimensionality reduction of embeddings - it may take a while!
    """
    from sklearn.manifold import TSNE

    tsne = TSNE(n_components=2, perplexity=perplexity, max_iter=n_iter, verbose=0)
    return tsne.fit_transform(embeddings)
