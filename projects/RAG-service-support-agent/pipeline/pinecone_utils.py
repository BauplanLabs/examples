"""
Utility functions for interacting with Pinecone - in particular, functions to get the embeddings
from text using Pinecone API, and then upload the embeddings to Pinecone index.

The index will be used in the app to provide RAG inference for real-time question answering with LLMs.
"""


def get_text_embeddings_from_pinecone(
    pinecone_client, text_to_embed: list, batch_size: int = 96
):
    """
    Sample code from: https://docs.pinecone.io/guides/inference/generate-embeddings
    """

    # Embed text with Pinecone API and some default parameters.
    # Iterate over the text in batches.
    embeddings = []
    for i in range(0, len(text_to_embed), batch_size):
        batch = text_to_embed[i : i + batch_size]
        cnt_embeddings = pinecone_client.inference.embed(
            model="multilingual-e5-large",
            inputs=batch,
            parameters={"input_type": "passage", "truncate": "END"},
        )
        embeddings.extend(cnt_embeddings)

    return embeddings


def upload_documents_to_pinecone(
    pinecone_client,
    ids: list,
    text_list: list,
    index_name: str,
    namespace: str = "bauplan-default",
    batch_size: int = 96,
) -> int:
    # We need to create the index if it doesn't exist.
    existing_idx = pinecone_client.list_indexes()
    if index_name in [_.name for _ in existing_idx]:
        print(f"Index {index_name} already exists.")
    else:
        print(f"Creating index {index_name}...")
        pinecone_client.create_index_for_model(
            name=index_name,
            cloud="aws",
            region="us-east-1",
            embed={
                "model": "multilingual-e5-large",
                "field_map": {"text": "chunk_text"},
            },
        )
    
    # Get the index object.
    index = pinecone_client.Index(index_name)
    
    # Prepare the records for upsert, trying not
    # to exceed the max supported size.
    tot_records = 0
    for i in range(0, len(ids), batch_size):
        records = []
        batch_ids = ids[i : i + batch_size]
        batch_text = text_list[i : i + batch_size]
        for _id, text in zip(batch_ids, batch_text):
            records.append({"_id": _id, "chunk_text": text})
        tot_records += len(batch_ids)
        index.upsert_records(namespace, records)

    return tot_records
