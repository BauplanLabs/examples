import bauplan


# The standard bauplan decorator to declaratively define the necessary
# Python environment and dependencies to run this function.
@bauplan.python("3.11", pip={"duckdb": "1.0.0"})
# Bauplan allows us to declaratively define when
# dataframes should be materialized
# back to the data catalog, backed by object storage.
# We use the REPLACE materialization strategy to overwrite the table every time.
@bauplan.model(materialization_strategy="REPLACE")
def one_big_qa_table(
    questions=bauplan.Model(
        "stack_overflow_questions",
        
        # We leverage the columnar nature of the
        # platform to only select the columns we
        # need.
        columns=["id", "title", "body"],
        
        # We filter out all the questions submitted before a certain date
        # to showcase parametrized filter pushdown
        # to the data lake in a declarative way.
        # NOTE: If you have a production Pinecone
        # cluster, you can process the entire dataset
        # without being throttled by the API!
        filter="creationdate > $creation_date_start",
    ),
    answers=bauplan.Model(
        "stack_overflow_answers",
        columns=[
            "parentid",  # need for the join
            "body",
        ],
        
        # Answers cannot be created before the questions,
        # so the filter should be pushed down also here.
        filter="creationdate > $creation_date_start",
    ),
    tags=bauplan.Model(
        "stack_overflow_tags"
        # We retrieve all the columns here, which
        # are just id (for the join) and tag.
    ),
):
    """
    We build a "one big table" by joining the questions, answers, and tags tables from the Stack Overflow dataset.
    We also save it back in the catalog because it's a useful table to have for further analysis!

    The final table has the following columns:

    | question_id     | question_text  | answer_text  | tags             |
    |-----------------|----------------|--------------|------------------|
    | 1               | How to...      | You d..      | [python, pandas] |
    """
    
    # Print out the number of rows retrieved to the console.
    print(f"\n\n===> Number of questions retrieved: {questions.num_rows}\n\n")
    
    # We use the duckdb library to quickly and concisely complete the join.
    import duckdb

    sql_query = """
    SELECT 
        q.id as question_id,
        q.title || ' ' || q.body as question_text,
        a.body as answer_text,
        array_agg(t.tag) as tags
    FROM
        questions as q
    JOIN
        answers as a
    ON
        q.id = a.parentid
    JOIN
        tags as t
    ON
        q.id = t.id
    GROUP BY
        ALL
    ORDER BY
        question_id ASC
    """
    data = duckdb.sql(sql_query).arrow()
    print(f"\n\n===> Total big table size: {data.num_rows}\n\n")
    
    # As in every bauplan model, functions return a "dataframe-like" object,
    # in this case, an Arrow table.
    return data


@bauplan.python(
    "3.11",
    pip={
        "scikit_learn": "1.5.2",
        "pinecone": "5.4.2",
        "pinecone-plugin-records": "1.1.0",
    },
)
# Note: We enable internet access to connect to our Pinecone cluster!
@bauplan.model(
    
    # We can override the default name of the model in the catalog by
    # specifying the name parameter.
    name="one_big_qa_table_with_embeddings",
    materialization_strategy="REPLACE",
    internet_access=True,
)
def q_and_a_to_rag(
    big_table=bauplan.Model("one_big_qa_table"),
    
    # Read in securely the Pinecone API key.
    pinecone_key=bauplan.Parameter("pinecone_key"),
):
    """
    Produce a final table with the embeddings for each question+answer, including a 2-D representation of the
    questions for visualization purposes. We use scikit-learn for TSNE, and use the Pinecone API both for
    generating the embeddings and uploading the final data+metadata to a Pinecone vector index.

    The final table has the following columns:

    | question_id     | question_text  | answer_text  | tags             | embeddings | two_d_vectors |
    |-----------------|----------------|--------------|------------------|------------|---------------|
    | 1               | How to...      | You d..      | [python, pandas] | [..]       | [[..], [..]]  |
    """
    import numpy as np

    # Utility functions live in utils.py to
    # keep this module focused on the DAG structure.
    from utils import tsne_analysis
    
    # We connect to Pinecone to get the embeddings.
    from pinecone_utils import (
        get_text_embeddings_from_pinecone,
        upload_documents_to_pinecone,
    )
    from pinecone import Pinecone

    # Initialize a Pinecone client with your API key.
    pc = Pinecone(api_key=pinecone_key)
    
    # Concatenate the question and answer text to embed them together.
    # Put a limit to the length of the text to embed for convenience.
    text_to_embed = [
        f"{q} {a}"[:600]
        for q, a in zip(
            big_table["question_text"].to_pylist(), big_table["answer_text"].to_pylist()
        )
    ]
    print("\n\n=====> Start getting the embeddings from Pinecone...\n")
    text_embeddings = get_text_embeddings_from_pinecone(pc, text_to_embed)
    print("\n\n=====> Finished getting the embeddings from Pinecone!\n")
    
    # Get the vectors out of the Pinecone return object.
    _vectors = [e["values"] for e in text_embeddings]
    
    # Now we compute the 2D embeddings with TSNE.
    two_d_embeddings = tsne_analysis(np.array(_vectors))
    assert (
        len(two_d_embeddings)
        == len(text_to_embed)
        == len(text_embeddings)
        == len(_vectors)
    )
    
    # Upload to Pinecone.
    print("\n\n=====> Start the Pinecone upsert process...\n")
    num_doc_inserted = upload_documents_to_pinecone(
        pc,
        ids=[str(_) for _ in big_table["question_id"].to_pylist()],
        text_list=text_to_embed,
        index_name="so-qa-index",
    )
    print(f"\nInserted {num_doc_inserted} documents in Pinecone")
    print("\n\n=====> Finished the Pinecone upload process!\n")
    
    # Add the embeddings and their 2D version to the table before returning it.
    final_table = big_table.append_column("embeddings", [_vectors])
    final_table = final_table.append_column(
        "two_d_vectors", [two_d_embeddings.tolist()]
    )

    return final_table
