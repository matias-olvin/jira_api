ASSERT (
    SELECT COUNT(*)
            FROM (
                SELECT 
                    COUNT(*) AS mall_local_date_count
                FROM 
                    `{{ var.value.env_project }}.{{ dag_run.conf['dataset_postgres_template'] }}.{{ params['visits_to_malls_daily_table'] }}`
                GROUP BY
                    fk_sgcenters, local_date
            )
            WHERE mall_local_date_count > 1
) = 0 AS "Duplicates found in {{ var.value.env_project }}.{{ dag_run.conf['dataset_postgres_template'] }}.{{ params['visits_to_malls_daily_table'] }}"