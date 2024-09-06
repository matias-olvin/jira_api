ASSERT (SELECT COUNT(*)
        FROM (
            SELECT pid
            FROM 
                `{{ var.value.env_project }}.{{ params['smc_sg_places_dataset'] }}.{{ params['places_dynamic_table'] }}`
            WHERE 
                pid IS NULL
        )
) = 0  AS "Error: one or more pid has value NULL in {{ params['smc_sg_places_dataset'] }}.{{ params['places_dynamic_table'] }}"
