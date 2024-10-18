"""

This file contains a Streamlit app to visualize the table 'top_pickup_location' created with a Bauplan pipeline
contained in this project. This simple script shows how to use bauplan SDK to embed querying functionalities in a webapp.

"""

# General Streamlit / Visualization imports
import streamlit as st
import sys
import matplotlib.pyplot as plt
# we import the bauplan Python SDK, with pre-built functions
# this function allow us to use bauplan as an interactive query engine
# we can run query against a table in the data catalog and put the data in a Pandas dataframe
import bauplan

# what is the target table to query?
TABLE_NAME = 'top_pickup_locations'  # this is the table materialized by the pipeline in the same example!


@st.cache_data()
def query_as_dataframe(
        _client: bauplan.Client,
        sql: str,
        branch: str,
):
    """
    This function uses the query method to query a table in the data catalog
    and return as DataFrame
    """

    try:
        df = _client.query(sql, branch_name=branch).to_pandas()
        return df
    except:
        print("something wen wrong with retrieving the data")


def plot_bar_chart(df):
    """
    This function plots a bar chart from the table top_pickup_location
    """
    plt.figure(figsize=(11, 11))
    plt.barh(df['Zone'], df['number_of_trips'], color='skyblue', edgecolor='white')
    plt.ylabel('Zone')
    plt.xlabel('Number of Trips')
    plt.title('Number of Trips per Zone')
    plt.tight_layout()
    st.pyplot(plt)


def check_branch_and_table(
        _client: bauplan.Client,
        branch: str,
        table: str
):
    """
    This function uses the bauplan methods get_branches and get_branch to validate that the input branch is valid
    """
    selected_branch = branch.strip()

    # check if a branch is specified
    if not selected_branch:
        return False
    # use to sdk to check if the branch exists
    elif selected_branch not in [branch.name for branch in _client.get_branches()]:
        return False
    # use the sdk to check if the table exists in the branch
    elif table not in [b.name for b in _client.get_branch(selected_branch) if b.kind == 'TABLE']:
        return False

    return True


# THE STREAMLIT APP BEGINS HERE
def main():
    # instantiate a bauplan client to use the SDK
    client = bauplan.Client()
    # webapp title
    st.title('A simple data app to visualize taxi rides and locations in NY')
    # Debug line to ensure correct Python interpreter
    print(sys.executable)

    # define a text input field where the user can indicate her active branch
    selected_branch = st.text_input("What branch are you looking for?", " ")
    # use bauplan sdk to check if the table exists in the selected branch
    if check_branch_and_table(client, selected_branch, TABLE_NAME):
        # use bauplan sdk to retrieve the data from the data catalog as a Pandas DataFrame
        df = query_as_dataframe(client, f"SELECT * FROM {TABLE_NAME}", selected_branch)
        if df is not None and not df.empty:
            st.dataframe(df, width=1200)
            plot_bar_chart(df)
        else:
            st.write('Something went wrong! Please check your branch and try again!')
    else:
        st.write('Please make sure you have typed a valid branch and the table exists in that branch!')


if __name__ == "__main__":
    main()
