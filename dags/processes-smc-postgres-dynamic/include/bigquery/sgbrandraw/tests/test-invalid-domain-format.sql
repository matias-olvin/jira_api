ASSERT (
    SELECT
        COUNT(*)
    FROM
        `{{ var.value.env_project }}.{{ params['smc_postgres_dataset'] }}.{{ params['sgbrandraw_table'] }}`
    WHERE domain LIKE "[%]"
) = 0 AS "Error:  invalid domain format in {{ params['smc_postgres_dataset'] }}.{{ params['sgbrandraw_table'] }}"
