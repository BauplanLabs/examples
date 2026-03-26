import bauplan


def _request_prediction_from_open_ai(
    company: str, year: int, quarter: int, text: str, oai_client
) -> str:
    """ """
    # Structure of the response.
    from pydantic import BaseModel

    class FinancialStatement(BaseModel):
        statement: str
        time_label: str
        usd: int
        year: int

    class FinancialStatements(BaseModel):
        statements: list[FinancialStatement]

    system_p = f"""
        As an expert financial analyst in the tech sector, please analyze this document and extract the required
        information.
        
        The text below is the beginning of the 10-Q report for {company} in {year} Q {quarter}, expressed in markdown.
        Make sure to extract years as integers, and amounts as integers even if they are expressed in USD as strings such as 40,667.
        
        You need to extract: Net income, Net product sales, Net service sales. You are an expert in data structures, so you should convert the extracted data into the given structure. 
        
        For example, a FinancialStatement object should look like this:
        
        {{
            "statement": "Net income",
            "time_label": "Three Months Ended September 30",
            "usd": 40000,
            "year": 2022
        }}
        
    """

    completion = oai_client.beta.chat.completions.parse(
        model="gpt-5.4-mini",
        messages=[
            {"role": "system", "content": system_p.strip()},
            {"role": "user", "content": text.strip()},
        ],
        max_tokens=1500,
        response_format=FinancialStatements,
    )
    return completion.choices[0].message.parsed


def _pdf_to_markdown(bucket, pdf_path):
    import tempfile

    # Instantiate the clients.
    # Since we are running in the Bauplan alpha
    # and reading from a connected bucket,
    # we don't need to specify credentials - of course, you could instantiate
    # the client with your own credentials using Bauplan secrets
    import boto3

    s3 = boto3.client("s3")
    from markitdown import MarkItDown

    md = MarkItDown()

    with tempfile.NamedTemporaryFile() as tmp_file:
        print(f"\n>>>> Processing {pdf_path.split('/')[-1]}")
        s3.download_fileobj(bucket, pdf_path, tmp_file)
        result = md.convert(tmp_file.name)
        
        # Cut the text after the forward-looking statements.
        return result.text_content.split("Forward-Looking Statements")[0]


# We need boto3 to get the PDFs from S3
# and markitdown to convert the PDFs to text.
@bauplan.python("3.10", pip={"boto3": "1.35.86", "markitdown": "0.0.1a3"})
@bauplan.model(internet_access=True)
def sec_10_q_markdown(data=bauplan.Model("my_pdf_metadata")):
    """
    This function reads the metadata and the PDFs from S3 and converts them to markdown.
    The final table is therefore the same as the input table without bucket and path, with an additional column:

    | id | company | year | quarter | markdown_text  |
    |----|---------|------|---------|----------------|
    | 1  | Amazon  | 2021 | 1       |  ...           |
    """
    import concurrent.futures

    # Get lists from the Arrow columns, to iterate over them.
    bucket_name = data["bucket"].to_pylist()
    object_key = data["pdf_path"].to_pylist()
    
    # We will store the markdown text in a list.
    values = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=12) as executor:
        futures = {}
        for ctr, (bucket, pdf_path) in enumerate(zip(bucket_name, object_key)):
            futures[executor.submit(_pdf_to_markdown, bucket, pdf_path)] = ctr
        for future in concurrent.futures.as_completed(futures):
            try:
                values.append((future.result(), futures[future]))
            except Exception as ex:
                raise ex

    values, _ = zip(*sorted(values, key=lambda x: x[1]))
    
    # Add the markdown text to the data.
    data = data.append_column("markdown_text", [values])
    
    # Remove the bucket and path columns.
    data = data.drop_columns(["bucket", "pdf_path"])

    return data


@bauplan.python("3.11", pip={"openai": "2.29.0"})
# Make sure to persist the data as an Iceberg-backed table.
@bauplan.model(internet_access=True, materialization_strategy="REPLACE")
def sec_10_q_tabular_dataset(
    data=bauplan.Model("sec_10_q_markdown"),
    open_ai_key=bauplan.Parameter("openai_api_key"),
):
    """
    This function reads the markdown text of each document and uses the LLM to extract information
    in a tabular format. We leverage the structured outputs feature of the LLM to extract the required
    information into a Pydantic model (see https://platform.openai.com/docs/guides/structured-outputs).

    The extracted rows are then enriched with the report metadata and returned:

    | year | statement | time_label     | usd | report_company | report_year | report_quarter |
    |------|-----------|----------------|-----|----------------|-------------|----------------|
    | 2021 | Net income| Three Months.. | 400 | Amazon         | 2021        | 1              |
    """

    from openai import OpenAI
    import time
    import pyarrow as pa

    # From the Arrow table, get the lists to iterate over them.
    companies = data["company"].to_pylist()
    years = data["year"].to_pylist()
    quarters = data["quarter"].to_pylist()
    text = data["markdown_text"].to_pylist()
    start_time = time.time()
    results = []
    oai_client = OpenAI(api_key=open_ai_key)
    for company, year, quarter, t in zip(companies, years, quarters, text):
        
        # Use the LLM to extract the required information.
        generated_result = _request_prediction_from_open_ai(
            company, year, quarter, t, oai_client
        )
        
        # Parse the JSON response to get the rows.
        rows = generated_result.model_dump(mode="json")["statements"]
        
        # Add the original metadata regarding the report to each row.
        for row in rows:
            row["report_company"] = company
            row["report_year"] = year
            row["report_quarter"] = quarter
        results.extend(rows)
    end_time = time.time()
    
    # Print the time taken to process the documents.
    print(
        f"LLM loop time: {end_time - start_time} s, avg. {(end_time - start_time) / len(results)} s"
    )

    # Return an Arrow table.
    return pa.Table.from_pylist(results)


@bauplan.python("3.11", pip={"polars": "1.38.1"})
@bauplan.model(materialization_strategy="REPLACE")
def sec_10_q_analysis(data=bauplan.Model("sec_10_q_tabular_dataset")):
    """
    This function reads the tabular dataset prepared by the previous step and performs some analysis
    using Polars.

    As an example, we will calculate the mean of the USD values for each statement and company:

    | report_company | statement | mean_usd |
    |----------------|-----------|----------|
    | Amazon         | Net income| 400      |
    """
    import polars as pl

    # Convert the Arrow table to a Polars DataFrame (zero-copy).
    df = pl.from_arrow(data)
    
    # Group by company and statement, and calculate the mean of the USD values.
    df = df.group_by("report_company", "statement").agg(pl.col("usd").mean())

    return df.to_arrow()
