ASSERT (
    SELECT COUNT(*)
            FROM (
                SELECT pid
                FROM 
                    `{{ var.value.env_project }}.{{ params['smc_postgres_dataset'] }}.{{ params['sgplaceraw_table'] }}`
                WHERE 
                    pid IS NULL
            )
) = 0 AS "Error: pID Nulls found in {{ params['smc_postgres_dataset'] }}.{{ params['sgplaceraw_table'] }}"
