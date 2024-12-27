"""

This is a simple app reading the artifacts prepared by the data pipeline and saved in
bauplan.

To run the app, simply execute:

streamlit run explore_matches.py -- --bauplan_user_name <YOUR_USERNAME>

The app assumes you have run the pipeline in a branch: please select the right branch to query the data
and get started!

Check the code for the additional arguments you can pass to the script. Note that streamlit 
run parses parameters slightly differently, so you need to use the -- separator to pass 
arguments to the script.

"""


import streamlit as st
import sys
import pandas as pd
import matplotlib.pyplot as plt
import bauplan


### GLOBAL CLIENTS ###
# we instantiate them once

bauplan_client = bauplan.Client()


### UTILITY FUNCTIONS ###

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


def plot_bar_chart(
    brands: list,
    counts: list
):
    """
    Plot a bar chart!
    """
    fig, ax = plt.subplots()
    ax.bar(brands, counts)
    ax.set_title('# of matches by brand')
    st.pyplot(plt)
    
    return


### THE STREAMLIT APP BEGINS HERE ###

def main(
    bauplan_user_name: str,
    one_big_table_name: str
):
    st.title('Explore wrong product matches made by the LLM!')
    # debug line to ensure correct Python interpreter
    print(sys.executable)
    all_branches = list(_.name for _ in bauplan_client.get_branches(user=bauplan_user_name))
    target_branch = st.selectbox(f'Pick the branch with {one_big_table_name}:', all_branches, index=None)
    st.write(f'You selected: {target_branch}')
    if target_branch is None:
        st.write('Please select a branch to continue!')
        st.stop()
    # plot a bar chart with the brand distribution
    st.write('Top 10 brands (walmart) in the final table:')
    sql_query = f"""
    SELECT
        walmart_brand, COUNT(*) as _C
    FROM
        {one_big_table_name}
    GROUP BY
        1
    ORDER BY
        2 DESC
    -- LIMIT 10 to avoid cluttering the chart
    LIMIT 10
    """
    brand_table = query_as_arrow(bauplan_client, sql_query, target_branch)
    if brand_table is  None:
        st.write('Something went wrong! Please try again!')
        st.stop()
        
    plot_bar_chart(
        brands=brand_table['walmart_brand'].to_pylist(),
        counts=brand_table['_C'].to_pylist()  
    )
    # display examples in which the label is not the same as the prediction
    sql_query = f"""
    SELECT 
        amazon_product, walmart_product, prediction, label
    FROM 
        {one_big_table_name}
    WHERE
        prediction != label
    """
    table = query_as_arrow(bauplan_client, sql_query, target_branch)
    if table is  None:
        st.write('Something went wrong! Please check your branch and try again!')
        st.stop()
        
    st.write('Here are some examples where the prediction is different from the label:')
    st.dataframe(table.slice(length=3).to_pandas(), width=1200)
    
    return
        

if __name__ == "__main__":
    # parse the arguments
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--bauplan_user_name', type=str, default='jacopo')
    parser.add_argument('--one_big_table_name', type=str, default='product_llm_matches')
    args = parser.parse_args()
    # start the app
    main(
        bauplan_user_name=args.bauplan_user_name,
        one_big_table_name=args.one_big_table_name
    )
