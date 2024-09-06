ASSERT (
    SELECT COUNT(*)
    FROM `{{ var.value.env_project }}.{{ dag_run.conf['dataset_postgres_template'] }}.{{ params['visits_to_malls_monthly_table'] }}`
    WHERE fk_sgcenters IS NULL OR local_date IS NULL
) = 0 AS "nulls found in {{ var.value.env_project }}.{{ dag_run.conf['dataset_postgres_template'] }}.{{ params['visits_to_malls_monthly_table'] }}"