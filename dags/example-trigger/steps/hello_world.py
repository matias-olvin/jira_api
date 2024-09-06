from airflow.models import DAG, TaskInstance
from airflow.operators.bash import BashOperator
from sifflet_provider.operators.rule import SiffletRunRuleOperator


def register(start: TaskInstance, dag: DAG) -> TaskInstance:
    hello_world = BashOperator(
        task_id="hello_world",
        bash_command="{% include './include/bash/hello_world.sh' %}",
    )

    sifflet_rule = SiffletRunRuleOperator(
        task_id="sifflet_rule",
        rule_ids=[
            "0a5510a5-87e6-46f0-a45a-2a12c9e88b34",
        ],
    )

    start >> hello_world >> sifflet_rule

    return hello_world
