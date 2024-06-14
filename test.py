import json
import os

import requests
from dotenv import load_dotenv
from requests.auth import HTTPBasicAuth

load_dotenv()
API_TOKEN = os.getenv("API_TOKEN")

# Replace with your Jira instance URL, username, and API token
JIRA_URL = "https://passby.atlassian.net"
USERNAME = "matias@passby.com"

import json

# This code sample uses the 'requests' library:
# http://docs.python-requests.org
import requests
from requests.auth import HTTPBasicAuth

url = f"{JIRA_URL}/rest/api/3/issue"

auth = HTTPBasicAuth("matias@passby.com", API_TOKEN)

headers = {"Accept": "application/json", "Content-Type": "application/json"}

summary = "New issue created via REST API"
description = "This is a description of the issue."


def _get_issue_id(issue_type: str) -> str:
    """
    Get the issue ID by issue type.

    Args:
        issue_type (str): The issue type.

    Returns:
        str: The ID of the issue type.
    """
    issue_types_url = f"{JIRA_URL}/rest/api/3/issuetype"
    response = requests.get(issue_types_url, auth=auth)
    issue_types = json.loads(response.text)

    for issue in issue_types:
        if issue["name"] == issue_type:
            return issue["id"]

    raise ValueError(f"Issue type '{issue_type}' not found.")


# Function to get assignee ID by email
def _get_assignee_id(email: str) -> str:
    """
    Get the assignee ID by email.

    Args:
        email (str): The email of the assignee.

    Returns:
        str: The ID of the assignee.
    """
    users_url = f"{JIRA_URL}/rest/api/3/user/search?query={email}"
    response = requests.get(users_url, auth=auth)
    users = json.loads(response.text)

    for user in users:
        if user["emailAddress"] == email:
            return user["accountId"]

    raise ValueError(f"Assignee with email '{USERNAME}' not found.")


# Get assignee ID by email
assignee_id = _get_assignee_id(USERNAME)

issue_type = _get_issue_id("Data Request")


def _return_payload(
    assignee_id: str, issue_type: str, summary: str, description: str
) -> str:
    """
    Returns the payload for the API request.

    Args:
        assignee_id (str): The ID of the assignee.
        issue_type (str): The ID of the issue type.
        summary (str): The summary of the issue.
        description (str): The description of the issue.

    Returns:
        str: The payload for the API request.
    """
    description_content = {
        "type": "doc",
        "version": 1,
        "content": [
            {
                "type": "paragraph",
                "content": [{"type": "text", "text": f"{description}"}],
            }
        ],
    }

    payload = json.dumps(
        {
            "fields": {
                "assignee": {"id": assignee_id},
                "issuetype": {"id": f"{issue_type}"},
                "project": {
                    "key": "DATA"  # Replace PROJECT_KEY with the actual project key
                },
                "summary": f"{summary}",  # Update the summary field with the desired title
                "description": description_content,  # Update the description field with the desired content
            },
            "update": {},
        }
    )

    return payload


payload = _return_payload(assignee_id, issue_type, summary, description)

response = requests.request("POST", url, data=payload, headers=headers, auth=auth)

print(
    json.dumps(
        json.loads(response.text), sort_keys=True, indent=4, separators=(",", ": ")
    )
)
