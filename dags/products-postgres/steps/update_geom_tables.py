from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
    BigQueryValueCheckOperator,
)


def register(dag, start, postgres_dataset):
    """
    Register tasks on the dag.

    Args:
        dag (airflow.models.DAG): DAG instance to register tasks on.
        start (airflow.models.TaskInstance): Task instance all tasks will
            registered downstream from.

    Returns:
        airflow.models.TaskInstance: The last task node in this section.
    """
    update_almanac_geometry_start = DummyOperator(
        task_id=f"update_almanac_geometry_start_{postgres_dataset}"
    )
    start >> update_almanac_geometry_start

    update_almanac_geometry_end = DummyOperator(task_id=f"update_almanac_geometry_end_{postgres_dataset}")

    update_CityRaw_table = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id=f"update_CityRaw_table_{postgres_dataset}",
        configuration={
            "query": {
                "query": (
                            '{% with postgres_dataset="'
                            f"{postgres_dataset}"
                            '"%}{% include "./bigquery/update_geom_tables/update_city_table.sql" %}{% endwith %}'
                        ),
                "useLegacySql": "False",
            }
        },
        dag=dag,
        location="EU",
    )

    update_ZipCodeRaw_table = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id=f"update_ZipCodeRaw_table_{postgres_dataset}",
        configuration={
            "query": {
                "query": (
                            '{% with postgres_dataset="'
                            f"{postgres_dataset}"
                            '"%}{% include "./bigquery/update_geom_tables/update_zipcode_table.sql" %}{% endwith %}'
                        ),
                "useLegacySql": "False",
            }
        },
        dag=dag,
        location="EU",
    )

    update_CityPatternsActivityRaw_table = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id=f"update_CityPatternsActivityRaw_table_{postgres_dataset}",
        configuration={
            "query": {
                "query": (
                            '{% with postgres_dataset="'
                            f"{postgres_dataset}"
                            '"%}{% include "./bigquery/update_geom_tables/update_citypatternsactivity_table.sql" %}{% '
                            'endwith %} '
                        ),
                "useLegacySql": "False",
            }
        },
        dag=dag,
        location="EU",
    )

    update_ZipCodePatternsActivityRaw_table = BigQueryInsertJobOperator(
gcp_conn_id="google_cloud_olvin_default",        
task_id=f"update_ZipCodePatternsActivityRaw_table_{postgres_dataset}",
        configuration={
            "query": {
                "query": (
                            '{% with postgres_dataset="'
                            f"{postgres_dataset}"
                            '"%}{% include "./bigquery/update_geom_tables/update_zipcodepatternsactivity_table.sql" '
                            '%}{% endwith %} '
                        ),
                "useLegacySql": "False",
            }
        },
        dag=dag,
        location="EU",
    )

    check_CityPatternsActivityRaw_table = BigQueryValueCheckOperator(
gcp_conn_id="google_cloud_olvin_default",
        sql=(
                '{% with postgres_dataset="'
                f"{postgres_dataset}"
                '"%}{% include "./bigquery/update_geom_tables/check_citypatternsactivity_table.sql" %}{% endwith %}'
            ),
        pass_value=0,
        task_id=f"check_CityPatternsActivityRaw_table_{postgres_dataset}",
        dag=dag,
        location="EU",
        use_legacy_sql=False,
        depends_on_past=False,
        wait_for_downstream=False,
        # on_failure_callback=utils.task_fail_slack_alert(
        #     "U03BANPLXJR",  # Carlos
        #     channel="prod"
        # )
    )

    check_ZipCodePatternsActivityRaw_table = BigQueryValueCheckOperator(
gcp_conn_id="google_cloud_olvin_default",
        sql=(
                '{% with postgres_dataset="'
                f"{postgres_dataset}"
                '"%}{% include "./bigquery/update_geom_tables/check_zipcodepatternsactivity_table.sql" %}{% endwith %}'
            ),
        pass_value=0,
        task_id=f"check_ZipCodePatternsActivityRaw_table_{postgres_dataset}",
        dag=dag,
        location="EU",
        use_legacy_sql=False,
        depends_on_past=False,
        wait_for_downstream=False,
        # on_failure_callback=utils.task_fail_slack_alert(
        #     "U03BANPLXJR",  # Carlos
        #     channel="prod"
        # )
    )

    update_almanac_geometry_start >> [update_CityRaw_table, update_ZipCodeRaw_table]
    (
        update_CityRaw_table
        >> update_CityPatternsActivityRaw_table
        >> check_CityPatternsActivityRaw_table
    )
    (
        update_ZipCodeRaw_table
        >> update_ZipCodePatternsActivityRaw_table
        >> check_ZipCodePatternsActivityRaw_table
    )
    [
        check_CityPatternsActivityRaw_table,
        check_ZipCodePatternsActivityRaw_table,
    ] >> update_almanac_geometry_end
    # update_CityRaw_table >> update_CityActivity_table
    # update_ZipCodeRaw_table >> update_ZipCodeActivity_table
    # [update_CityActivity_table, update_ZipCodeActivity_table] >> update_almanac_geometry_end

    return update_almanac_geometry_end
