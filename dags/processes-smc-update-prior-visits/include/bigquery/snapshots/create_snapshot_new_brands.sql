DECLARE date_str DATE;

DECLARE sql STRING;

CREATE TABLE IF NOT EXISTS
    `{{ var.value.env_project }}.{{ ti.xcom_pull(task_ids='push_dataset_name_to_xcom') }}.{{ params['new_brands_table'] }}` (
        name STRING,
        pid STRING,
        sub_category STRING,
        median_category_visits INTEGER,
        category_median_visits_range FLOAT64,
        median_brand_visits INTEGER
    );

SET
    date_str = DATE_ADD("{{ ds }}", INTERVAL 18 WEEK);

DROP SNAPSHOT TABLE IF EXISTS `{{ var.value.env_project }}.{{ ti.xcom_pull(task_ids='push_dataset_name_to_xcom') }}.{{ params['new_brands_table'] }}_snapshot`;

SET
    sql = CONCAT(
        "CREATE SNAPSHOT TABLE IF NOT EXISTS `{{ var.value.env_project }}.{{ ti.xcom_pull(task_ids='push_dataset_name_to_xcom') }}.{{ params['new_brands_table'] }}_snapshot` CLONE `{{ var.value.env_project }}.{{ ti.xcom_pull(task_ids='push_dataset_name_to_xcom') }}.{{ params['new_brands_table'] }}` OPTIONS (expiration_timestamp = TIMESTAMP '",
        date_str,
        "')"
    );

EXECUTE IMMEDIATE sql;