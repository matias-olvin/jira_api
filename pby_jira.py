import json
import os

import requests
from airflow.models import BaseOperator
from requests.auth import HTTPBasicAuth

ACCEPTABLE_ISSUE_TYPES = ["Bug", "Task", "dev DEng", "dev DS", "prod", "Data Request"]


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
        self.jira_url = "https://passby.atlassian.net"
        self.username = "matias@passby.com"
        self.project = "DATA"
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

        if issue_type not in ACCEPTABLE_ISSUE_TYPES:
            raise ValueError(
                f"Invalid issue type. Acceptable values: {ACCEPTABLE_ISSUE_TYPES}"
            )

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
        # The URL of the REST API endpoint
        url = f"{self.jira_url}/rest/api/3/issue"

        auth = HTTPBasicAuth("matias@passby.com", self.api_token)

        # The headers and data for the new issue
        headers, data = self._return_headers_and_data(
            project=self.project,
            summary=self.summary,
            description=self.description,
            issue_type=self.issue_type,
            assignee=self.assignee,
        )

        # Make the API request
        response = requests.request(
            "POST",
            url,
            headers=headers,
            data=json.dumps(data),
            auth=auth,
        )

        context["task_instance"].xcom_push(
            key="jira_issue_id", value=response.json()["id"]
        )

    # Function to get assignee ID by email
    def _get_assignee_id(email: str):
        users_url = f"{JIRA_URL}/rest/api/3/user/search?query={email}"
        response = requests.get(users_url, auth=auth)
        users = json.loads(response.text)
        
        for user in users:
            if user['emailAddress'] == email:
                return user['accountId']
        
        return None

    @staticmethod
    def _return_headers_and_data(
        project: str, summary: str, description: str, issue_type: str, assignee: str
    ):
        """
        Returns the headers and data for the API request.

        Args:
            project (str): The project key.
            summary (str): The summary of the issue.
            description (str): The description of the issue.
            issue_type (str): The type of the issue.
            assignee (str): The assignee of the issue.

        Returns:
            tuple: A tuple containing the headers and data for the API request.
        """
        # The headers for the API request
        headers = {"Accept": "application/json", "Content-Type": "application/json"}

        # The data for the new issue
        data = {
            "fields": {
                "project": {"key": project},  # Replace with your project key
                "summary": summary,
                "description": description,
                "issuetype": {"name": issue_type},  # Replace with your issue type
                "assignee": {
                    "name": assignee  # Replace with the username of the assignee
                },
            }
        }

        return headers, data
