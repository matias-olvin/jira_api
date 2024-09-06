import requests
from airflow.exceptions import (
    AirflowException,
    AirflowFailException,
    AirflowSkipException,
)


def check_model_exists(name: str, alias: str) -> str:
    response = requests.post(
        "https://europe-west1-storage-prod-olvin-com.cloudfunctions.net/check-model-alias-exists",
        json={"model_name": name, "model_alias": alias},
    )
    if response.status_code == 404:
        raise AirflowSkipException(f"Model alias not found: {response.text}")
    elif response.status_code == 500:
        raise AirflowException(f"Error: {response.text}")
    elif response.status_code != 200:
        raise AirflowException(f"Model check failed: {response.text}")


def check_table_exists(table_id: str, **context) -> str:
    response = requests.post(
        "https://europe-west1-storage-prod-olvin-com.cloudfunctions.net/check-table-exists",
        json={"table_id": table_id},
    )
    if response.status_code == 404:
        raise AirflowException("Table not found")
    elif response.status_code == 500:
        raise AirflowException(f"Error: {response.text}")
    elif response.status_code != 200:
        raise AirflowException(f"Table check failed: {response.text}")
    
def check_ip(almanac_ip: str, almanac_instance: str, **context) -> str:
    response = requests.post(
        "https://europe-west1-storage-prod-olvin-com.cloudfunctions.net/check-ip",
        json={"almanac_ip": almanac_ip, "almanac_instance": almanac_instance},
    )
    if response.status_code == 404:
        raise AirflowException(f"Public IP is different in {almanac_instance} Cloud SQL instance and Airflow")
    elif response.status_code == 500:
        raise AirflowException(f"Error: {response.text}")
    elif response.status_code != 200:
        raise AirflowException(f"Staging IP check failed: {response.text}")
