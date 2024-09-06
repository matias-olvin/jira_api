ASSERT (
    SELECT COUNT(*)
        FROM 
            `{{ var.value.env_project }}.{{ params['smc_postgres_dataset'] }}.{{ params['sgbrandraw_table'] }}`
        WHERE 
            pid IS NULL
) = 0 AS "Error: pid Nulls found in {{ params['smc_postgres_dataset'] }}.{{ params['sgbrandraw_table'] }}"
