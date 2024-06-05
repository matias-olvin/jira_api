import json

import requests
from airflow.models import BaseOperator
from requests.auth import HTTPBasicAuth
from airflow.models import Variable

PROJECT = "DATA"
PBY_JIRA_URL = "https://passby.atlassian.net"
ADMIN_USERNAME = "matias@passby.com"
COOL_TEXT = """
    ____     ___    _____   _____           ____ __  __
   / __ \   /   |  / ___/  / ___/          / __ )\ \/ /
  / /_/ /  / /| |  \__ \   \__ \          / __  | \  / 
 / ____/  / ___ | ___/ /  ___/ /         / /_/ /  / /  
/_/      /_/  |_|/____/  /____/   ______/_____/  /_/   
                                 /_____/               

"""
API_TOKEN = Variable.get("pby_jira_api_token")


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

        # Validate the assignee
        if "@" not in assignee:
            raise ValueError("Invalid assignee. Assignee should be an email address")
        else:
            self.assignee = assignee
        
        self.issue_type = issue_type

        super(PassbyJiraCreateIssueOperator, self).__init__(*args, **kwargs)

        self.ui_color = "#DEDEDE"

    def execute(self, context):
        """
        Executes the operator.

        Args:
            context (dict): The context dictionary.

        Raises:
            ValueError: If the API_TOKEN environment variable is not set.
        """

        print(COOL_TEXT)

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

        if response.status_code != 201:
            raise ValueError(
                f"Failed to create issue: {response.status_code} - {response.text}"
            )

        print(f"Successfully created issue: {response.text}")

        # push to xcom
        self.xcom_push(context, key="response", value=response.text)

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

        issue_types_list_string = "\n".join([issue["name"] for issue in issue_types])

        error_message = f"""Issue type '{issue_type}' not found. Available issue types: {issue_types_list_string}"""

        raise ValueError(error_message)

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
