import bauplan
import requests
import json  # <-- important

@bauplan.model(internet_access=True, materialization_strategy='REPLACE')
@bauplan.python('3.11', pip={'requests': '2.31.0'})
def sales_phantom_launch_agent(
    data=bauplan.Model('sales_phantom'),
    phantom_api_key=bauplan.Parameter('phantom_api_key'),
    linkedin_session_cookie=bauplan.Parameter('linkedin_session_cookie')
):
    """
    This model launches a PhantomBuster agent to scrape LinkedIn post engagements.
    """

    url = "https://api.phantombuster.com/api/v2/agents/launch"

    linkedin_post_url = "https://www.linkedin.com/posts/mattiapavoni_thegreattennis-serverless-python-activity-7318275741681360896-S-19"
    agent_id = "6593221848572583"

    # ⚡ Build the *actual* argument dictionary
    argument_payload = {
        "pushResultToCRM": False,
        "inputType": "linkedinPostUrl",
        "postEngagersToExtract": ["commenters", "likers"],
        "shouldOnlyRetrievePostsPublishedFromDate": False,
        "linkedinPostUrl": linkedin_post_url,
        "sessionCookie": linkedin_session_cookie,
        "userAgent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36"
    }

    # ⚡ Wrap the payload correctly: 'argument' must be a stringified JSON
    payload = {
        "argument": json.dumps(argument_payload),
        "id": agent_id
    }

    headers = {
        "Content-Type": "application/json",
        "X-Phantombuster-Key": phantom_api_key
    }

    # Launch the PhantomBuster agent
    print("\n>>>> Launching PhantomBuster agent...\n")
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()

    print("\n>>>> PhantomBuster agent launched successfully.\n")
    print(response.content)

    return data
