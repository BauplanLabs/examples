"""
This is a simple app reading the artifacts prepared by the data pipeline and stored
by bauplan in your S3. To run the app, simply execute:

streamlit run explore_analysis.py

Check the code for the arguments you can pass to the script.
"""

import streamlit as st
import plotly.express as px
import bauplan

bauplan_client = bauplan.Client()

### Utility Functions ###

@st.cache_data()
def query_as_arrow(_client: bauplan.Client, sql: str, namespace: str):
    """
    This function uses the query method to query a table in bauplan. This is
    handy as a separate function because we can cache the results and avoid
    querying the same data multiple times.

    It returns None if the query fails.
    """

    try:
        return _client.query(sql, ref="main", namespace=namespace)
    except Exception as e:
        print(e)

    return None


def plot_bar_chart(statements: list, means: list):
    fig = px.bar(
        x=means,
        y=statements,
        orientation="h",
        labels={"x": "USD", "y": ""},
        title="Mean (USD) per Statement",
        color=means,
        color_continuous_scale="Tealgrn",
    )
    fig.update_layout(
        showlegend=False,
        coloraxis_showscale=False,
        yaxis={"categoryorder": "total ascending"},
        margin=dict(l=20, r=20, t=40, b=20),
        height=max(400, len(statements) * 35),
    )
    fig.update_traces(
        texttemplate="$%{x:,.0f}",
        textposition="outside",
    )
    st.plotly_chart(fig, use_container_width=True)


### The Streamlit App Begins Here ###


def main(analysis_table_name: str, namespace: str):
    st.title("Explore the data extracted from the PDFs!")
    
    # Check that the table exists before querying.
    full_table_name = f"{namespace}.{analysis_table_name}"
    if not bauplan_client.has_table(full_table_name, ref="main"):
        st.error(
            f"Table '{full_table_name}' not found on branch 'main'. Run the pipeline first."
        )
        st.stop()

    sql_query = f"SELECT statement, usd FROM {analysis_table_name}"
    _table = query_as_arrow(bauplan_client, sql_query, namespace)
    if _table is None:
        st.write("Something went wrong! Please try again!")
        st.stop()

    plot_bar_chart(
        statements=_table["statement"].to_pylist(), means=_table["usd"].to_pylist()
    )

    return


if __name__ == "__main__":
    
    # Parse the arguments.
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--analysis_table_name", type=str, default="sec_10_q_analysis")
    parser.add_argument("--namespace", type=str, default="my_pdfs")
    args = parser.parse_args()
    
    # Start the app.
    main(args.analysis_table_name, args.namespace)
