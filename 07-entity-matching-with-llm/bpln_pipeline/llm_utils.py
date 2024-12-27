"""

Utility functions for interacting with OpenAI - in particular, functions call OpenAI client and 
prompt a model to make a decision about whether two products are matching or not.

"""


def match_with_llm(
    _product_a_list: list,
    _product_b_list: list,
    _llm_client,
) -> list:
    """
    
    Function to encapsulate the logic of iterating over the input table and 
    call an external API (e.g. OpenAI) to generate a response for each row.
    
    We return a list of predictions, one for each row in the input table.
    
    """
    
    # this prompt is from the original implementation: https://github.com/jacopotagliabue/foundation-models-for-dbt-entity-matching/blob/main/src/original/serverless/handler.py
    # but can be customized to fit the specific use case / improve the quality of the predictions.
    prompt = """
    Product A is Title: canon mp41dhii printing calculator Brand: canon. Product B is Title: canon mp41dhii 14-digit gloview lcd two-color printing desktop calculator black red Brand: canon. Are Product A and Product B equivalent? Yes, they are.

    Product A is Title: epson t020201 color ink cartridge Brand: epson. Product B is Title: Title: epson t001011 color inkjet cartridge Brand: epson. Are Product A and Product B equivalent? No, they aren't.

    Product A is {}. Product B is {}. Are Product A and Product B the same product? Only respond with yes or no. 
    """
    predictions = []
    for product_a, product_b in zip(_product_a_list, _product_b_list):
        prompt = prompt.format(product_a, product_b)
        # we feed the client and the prompt to OpenAI here, 
        # but this could be any other API or service - or even multiple services
        # that get evaluated in sequence for inconsistency.
        prediction = _request_prediction_from_open_ai(
            prompt=prompt,
            oai_client=_llm_client
        )
        predictions.append(prediction)
    
    return predictions


def _request_prediction_from_open_ai(
    prompt: str,
    oai_client
) -> bool:
    """
    
    Wrap completion request to OpenAI in a function and convert the response to a boolean.
    We re-use the same code from the MDS implementation, but we could customize it to fit
    the new capabilities of OpenAI's API (for example, structured outputs).
    
    The original implementation is here:
    
    https://github.com/jacopotagliabue/foundation-models-for-dbt-entity-matching/blob/main/src/original/serverless/handler.py
    
    Note that you could try and parallelize this function to speed up the predictions!
    """
    completion = oai_client.chat.completions.create(
       model="gpt-4-turbo",
        messages=[{"role": "user", "content": prompt}],
        max_tokens=10,
    )
    message_content = completion.choices[0].message.content.strip()
    # we clean the response to get a boolean value
    cleaned_token = message_content.strip().lower().replace(',', '').replace('.', '').replace(';', '').split(' ')[0]
    if cleaned_token == 'yes':
        return True
    elif cleaned_token == 'no':
        return False
    else:
        print("!!! Unexpected answer: {}".format(message_content))

    return False