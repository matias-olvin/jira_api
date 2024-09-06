ASSERT (
    SELECT COUNT(*)
        FROM (
            SELECT 
                COUNT(*) AS id_count
            FROM 
                `{{ var.value.env_project }}.{{ params['smc_postgres_dataset'] }}.{{ params['sgbrandraw_table'] }}`
            GROUP BY 
                pid
        )
        WHERE
            id_count > 1
) = 0 AS "Error: duplicate pIDs found in {{ params['smc_postgres_dataset'] }}.{{ params['sgbrandraw_table'] }}"
