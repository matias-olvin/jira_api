# Jira API Custom Operator

The code found in `pby_jira.py` defines a custom Airflow operator for creating issues in Jira using the Passby Jira API.

## PassbyJiraCreateIssueOperator

The `PassbyJiraCreateIssueOperator` class inherits from the `BaseOperator` class of Airflow. It has several methods:

- `__init__`: Initializes the operator with the summary, description, issue type, and assignee of the issue. It validates the assignee to ensure it's an email address.

- `execute`: This is the main method that gets called when the operator runs. It constructs the URL, headers, and payload for the API request, makes the request, checks the response, and pushes the response to XCom.

- `_get_issue_id`: This method retrieves the issue ID corresponding to the issue type from the Jira API.

- `_get_assignee_id`: This method retrieves the assignee ID corresponding to the assignee email from the Jira API.

- `_return_payload`: This method constructs the payload for the API request.

