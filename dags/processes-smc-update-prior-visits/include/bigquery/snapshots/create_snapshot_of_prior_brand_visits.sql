DECLARE date_str DATE;

DECLARE sql STRING;

SET
    date_str = DATE_ADD("{{ ds }}", INTERVAL 18 WEEK);

DROP SNAPSHOT TABLE IF EXISTS `{{ var.value.env_project }}.{{ ti.xcom_pull(task_ids='push_dataset_name_to_xcom') }}.{{ params['prior_brand_visits_table'] }}_snapshot`;

SET
    sql = CONCAT(
        "CREATE SNAPSHOT TABLE IF NOT EXISTS `{{ var.value.env_project }}.{{ ti.xcom_pull(task_ids='push_dataset_name_to_xcom') }}.{{ params['prior_brand_visits_table'] }}_snapshot` CLONE `{{ var.value.env_project }}.{{ params['ground_truth_volume_dataset'] }}.{{ params['prior_brand_visits_table'] }}` OPTIONS (expiration_timestamp = TIMESTAMP '",
        date_str,
        "')"
    );

EXECUTE IMMEDIATE sql;