ASSERT (SELECT COUNT(*)
    FROM (
        SELECT 
            COUNT(*) AS duplicate_count
        FROM 
            `{{ var.value.env_project }}.{{ params['smc_sg_places_dataset'] }}.{{ params['brands_dynamic_table'] }}`
        GROUP BY 
            pid,
            naics_code
    )
    WHERE 
        duplicate_count > 1
) = 0 AS "Error: pid duplicates found in {{ params['smc_sg_places_dataset'] }}.{{ params['brands_dynamic_table'] }}"
