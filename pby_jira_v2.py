import json
import os

import requests
from airflow.models import BaseOperator
from requests.auth import HTTPBasicAuth

PROJECT = "DATA"
PBY_JIRA_URL = "https://passby.atlassian.net"
ADMIN_USERNAME = "matias@passby.com"
API_TOKEN = os.getenv("API_TOKEN")


class PassbyJiraCreateIssueOperator(BaseOperator):
    """
    A custom Airflow operator for creating issues in Jira using the Passby Jira API.
    """

    def __init__(
        self,
        summary: str,
        description: str,
        issue_type: str,
        assignee: str,
        *args,
        **kwargs,
    ):
        """
        Initializes the PassbyJiraCreateIssueOperator.

        Args:
            summary (str): The summary of the issue.
            description (str): The description of the issue.
            issue_type (str): The type of the issue.
            assignee (str): The assignee of the issue.
            *args: Variable length argument list.
            **kwargs: Arbitrary keyword arguments.
        """
        self.summary = summary
        self.description = description

        # Load the API token from the environment variables
        api_token = os.getenv("API_TOKEN")
        if not api_token:
            raise ValueError("API_TOKEN environment variable not set")

        self.api_token = api_token

        # Validate the assignee
        if "@" not in assignee:
            raise ValueError("Invalid assignee. Assignee should be an email address")

        self.assignee = assignee
        self.issue_type = issue_type

        super(PassbyJiraCreateIssueOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        """
        Executes the operator.

        Args:
            context (dict): The context dictionary.

        Raises:
            ValueError: If the API_TOKEN environment variable is not set.
        """
        url = f"{PBY_JIRA_URL}/rest/api/3/issue"

        auth = HTTPBasicAuth(ADMIN_USERNAME, API_TOKEN)

        headers = {"Accept": "application/json", "Content-Type": "application/json"}

        issue_type_id = self._get_issue_id(self.issue_type, auth)

        assignee_id = self._get_assignee_id(self.assignee, auth)

        payload = self._return_payload(
            assignee_id, issue_type_id, self.summary, self.description
        )

        # Make the API request
        response = requests.request(
            "POST", url, data=payload, headers=headers, auth=auth
        )

        context["task_instance"].xcom_push(
            key="jira_issue_id", value=response.json()["id"]
        )

    @staticmethod
    def _get_issue_id(issue_type: str, auth: HTTPBasicAuth) -> str:
        """
        Get the issue ID by issue type.

        Args:
            issue_type (str): The issue type.
            auth (HTTPBasicAuth): The HTTPBasicAuth object.
        Returns:
            str: The ID of the issue type.
        """
        issue_types_url = f"{PBY_JIRA_URL}/rest/api/3/issuetype"
        response = requests.get(issue_types_url, auth=auth)
        issue_types = json.loads(response.text)

        for issue in issue_types:
            if issue["name"] == issue_type:
                return issue["id"]

        raise ValueError(f"Issue type '{issue_type}' not found.")

    @staticmethod
    def _get_assignee_id(email: str, auth: HTTPBasicAuth) -> str:
        """
        Get the assignee ID by email.

        Args:
            email (str): The email of the assignee.
            auth (HTTPBasicAuth): The HTTPBasicAuth object.
        Returns:
            str: The ID of the assignee.
        """
        users_url = f"{PBY_JIRA_URL}/rest/api/3/user/search?query={email}"
        response = requests.get(users_url, auth=auth)
        users = json.loads(response.text)

        for user in users:
            if user["emailAddress"] == email:
                return user["accountId"]

        raise ValueError(f"Assignee with email '{email}' not found.")

    @staticmethod
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
