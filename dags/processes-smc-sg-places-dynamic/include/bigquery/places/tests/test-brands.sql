ASSERT (
    SELECT COUNT(*) AS ct
    FROM(
        SELECT fk_sgbrands
        FROM `{{ var.value.env_project }}.{{ params['smc_sg_places_dataset'] }}.{{ params['places_dynamic_table'] }}`
        WHERE
            fk_sgbrands NOT IN(
            SELECT pid
            FROM `{{ var.value.env_project }}.{{ params['smc_sg_places_dataset'] }}.{{ params['brands_dynamic_table'] }}`
            )
          AND NOT CONTAINS_SUBSTR(fk_sgbrands, ',') -- Filter for car dealers that have multiple brands separated by commas
          AND fk_sgbrands IS NOT NULL
        GROUP BY fk_sgbrands
    )
) = 0 AS "Error: brands found in {{ params['smc_sg_places_dataset'] }}.{{ params['places_dynamic_table'] }} not present in {{ params['smc_sg_places_dataset'] }}.{{ params['brands_dynamic_table'] }}"
