"""
This file contains the modeling and data science bits, while `models.py` is for the actual
transformation logic.
"""


def tsne_analysis(embeddings, perplexity=50, n_iter=1000):
    """
    TSNE dimensionality reduction of embeddings - it may take a while!
    """
    from sklearn.manifold import TSNE

    tsne = TSNE(n_components=2, perplexity=perplexity, max_iter=n_iter, verbose=0)
    return tsne.fit_transform(embeddings)
