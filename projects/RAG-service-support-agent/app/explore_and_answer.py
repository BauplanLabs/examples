"""
This is a simple app reading the artifacts prepared by the data pipeline and saved in both
bauplan and Pinecone. We first retrieve embeddings from bauplan, and display the 2-D TSNE vectors in a scatterplot
(color-coded by tag!).

We then provide a simple UI to leverage Pinecone + OpenAI for a user-facing Q&A system: the user
can ask a question, and we will use the pre-computed vectors to find the nearest neighbors in the space, and feed those
as context to OpenAI's LLM to generate an answer.

To run the app, simply execute:

OPENAI_API_KEY=your_openai_key PINECONE_KEY=your_pinecone_key uv run python -m streamlit run explore_and_answer.py

The app assumes you have run the pipeline in a branch: please select the right branch to query the data
and get started!
"""

import os
import sys
import streamlit as st
import polars as pl
import matplotlib.pyplot as plt


# Set them as environment variables
assert os.environ["OPENAI_API_KEY"], "OPENAI_API_KEY environment variable is not set!"
assert os.environ["PINECONE_KEY"], "PINECONE_KEY environment variable is not set!"

# --- Import libraries that depend on the credentials ---
import bauplan  # noqa: E402
from pinecone import Pinecone  # noqa: E402
from openai import OpenAI  # noqa: E402

# Instantiate global clients using the loaded credentials
pc = Pinecone(api_key=os.environ["PINECONE_KEY"])

# This client will use OPENAI_API_KEY from the environment.
oai_client = OpenAI()
bauplan_client = bauplan.Client()

# --- Utility Functions ---


@st.cache_data()
def query_as_arrow(_client: bauplan.Client, sql: str, branch: str):
    """
    Query a table in Bauplan using caching to avoid redundant queries.
    Returns None if the query fails.
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
    Plot the 2-D vectors in the space with color-coded points.
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


def vector_search(pinecone_client: Pinecone, query: str, k: int = 3):
    """
    Use Pinecone to perform a vector search with the given query.
    """
    # NOTE: The index name and namespace are hardcoded. Adjust if needed.
    index = pinecone_client.Index("so-qa-index")
    results = index.search_records(
        namespace="bauplan-default",
        query={"inputs": {"text": query}, "top_k": k},
        fields=["chunk_text"],
    )
    # Retrieve the closest neighbors
    rows = []
    for r in results["result"]["hits"]:
        rows.append(
            {"id": r["_id"], "score": r["_score"], "text": r["fields"]["chunk_text"]}
        )
    return rows


# --- Main App ---
def main(one_big_table_name: str):
    st.title("Explore the Vector Space and Ask AI About Your Programming Issue!")

    # Automatically infer the bauplan username from the authenticated client
    bauplan_user_name = bauplan_client.info().user.username

    # Retrieve branches from Bauplan and let the user select one
    all_branches = [
        branch.name for branch in bauplan_client.get_branches(user=bauplan_user_name)
    ]
    target_branch = st.selectbox(
        f"Pick the branch with {one_big_table_name}:", all_branches, index=0
    )
    st.write(f"You selected: {target_branch}")

    # Check if the selected branch contains the required table
    if target_branch is None or not bauplan_client.has_table(
        one_big_table_name, target_branch
    ):
        st.write(f"Please select a branch containing the {one_big_table_name} table!")
        st.stop()

    sql_query = f"""
    SELECT
        question_id, two_d_vectors, tags
    FROM
        {one_big_table_name}
    """
    table = query_as_arrow(bauplan_client, sql_query, target_branch)
    if table is None:
        st.write("Something went wrong! Please check your branch and try again!")
        st.stop()

    st.dataframe(pl.from_arrow(table.slice(length=3)), width=1200)

    # Prepare data for scatterplot
    all_items = table["question_id"].to_pylist()
    question_to_tag = {}
    for q, t in zip(table["question_id"].to_pylist(), table["tags"].to_pylist()):
        question_to_tag[q] = t[0] if len(t) > 0 else "unknown"

    # Highlight specific languages in the visualization
    target_languages = ["javascript", "python", "c++"]
    for q in question_to_tag:
        if question_to_tag[q] not in target_languages:
            question_to_tag[q] = "unknown"

    plot_scatterplot_with_lookup(
        title="StackOverflow in (Vector) Space",
        items=all_items,
        items_to_target_cat=question_to_tag,
        vectors=table["two_d_vectors"].to_pylist(),
    )

    # Provide a simple Q&A interface
    user_question = st.text_input(
        "Ask a question:", value="how can I open a Json file in Python?"
    )
    if user_question == "":
        st.stop()

    # Retrieve context using vector search
    results = vector_search(pc, user_question, k=10)
    st.dataframe(pl.DataFrame(results[:3]), width=1200)

    # Prepare the prompt for the LLM
    LLM_CONTEXT = (
        "You are a friendly AI, expert in all things computer. "
        "Help me with this question, please: '{}'\n"
        "As useful context, please consider the following interactions occurred on this topic on StackOverflow: {}"
    )
    prompt = LLM_CONTEXT.format(user_question, "\n".join([r["text"] for r in results]))

    # Call the LLM (using GPT-5.4) to generate an answer
    completion = oai_client.chat.completions.create(
        model="gpt-5.4", messages=[{"role": "user", "content": prompt}]
    )
    message_content = completion.choices[0].message.content.strip()
    st.write("AI response:")
    st.write(message_content)
    return


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--one_big_table_name", type=str, default="one_big_qa_table_with_embeddings"
    )
    args = parser.parse_args()

    main(
        one_big_table_name=args.one_big_table_name,
    )
