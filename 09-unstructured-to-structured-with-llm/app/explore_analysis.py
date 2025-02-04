"""

This is a simple app reading the artifacts prepared by the data pipeline and stored
by bauplan in your S3. To run the app, simply execute:

streamlit run explore_analysis.py

Check the code for the arguments you can pass to the script.

"""


import streamlit as st
import sys
import matplotlib.pyplot as plt
import bauplan


### GLOBAL CLIENTS ###

bauplan_client = bauplan.Client()


### UTILITY FUNCTIONS ###

@st.cache_data()
def query_as_arrow(
    _client: bauplan.Client,
    sql: str,
    namespace: str
):
    """
    This function uses the query method to query a table in bauplan. This is 
    handy as a separate function because we can cache the results and avoid
    querying the same data multiple times.
    
    It returns None if the query fails.
    """

    try:
        return _client.query(sql, ref='main', namespace=namespace)
    except Exception as e:
        print(e)
        
    return None


def plot_bar_chart(
    statements: list,
    means: list
):
    """
    Plot a bar chart!
    """
    fig, ax = plt.subplots()
    ax.bar(statements, means)
    ax.set_title('Mean (USD) per statement')
    st.pyplot(plt)
    
    return


### THE STREAMLIT APP BEGINS HERE ###

def main(
    analysis_table_name: str,
    namespace: str
):
    st.title('Explore the data extracted from the PDFs!')
    # debug line to ensure correct Python interpreter
    print(sys.executable)
    st.write('Mean values per statement:')
    sql_query = f"SELECT statement, usd FROM {analysis_table_name}"
    _table = query_as_arrow(bauplan_client, sql_query, namespace)
    if _table is None:
        st.write('Something went wrong! Please try again!')
        st.stop()
        
    plot_bar_chart(
        statements=_table['statement'].to_pylist(),
        means=_table['usd'].to_pylist()
    )
    
    return
        

if __name__ == "__main__":
    # parse the arguments
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--analysis_table_name', type=str, default='sec_10_q_analysis')
    parser.add_argument('--namespace', type=str, default='my_pdfs')
    args = parser.parse_args()
    # start the app
    main(
        args.analysis_table_name,
        args.namespace
    )
