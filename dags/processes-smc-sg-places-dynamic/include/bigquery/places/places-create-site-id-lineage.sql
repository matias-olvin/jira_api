CREATE OR REPLACE TABLE `{{ var.value.env_project }}.{{ params['smc_sg_places_dataset'] }}.{{ params['site_id_lineage_table'] }}`
AS (
    WITH
        current_pois AS (
            SELECT 
                site_id,
                ARRAY_AGG(pid) AS current_pois
            FROM 
                `{{ var.value.env_project }}.{{ params['smc_sg_places_dataset'] }}.{{ params['places_dynamic_table'] }}`
            WHERE
                closing_date IS NULL
            GROUP BY 
                site_id
        ),
        previous_pois AS (
            SELECT
            site_id,
            ARRAY_AGG(pid) AS previous_pois
            FROM 
                `{{ var.value.env_project }}.{{ params['smc_sg_places_dataset'] }}.{{ params['places_dynamic_table'] }}`
            WHERE 
                closing_date IS NOT NULL
            GROUP BY 
                site_id
        )

    SELECT
        site_id,
        CASE 
            WHEN ARRAY_LENGTH(current_pois) > 0 THEN true
            ELSE false
        END AS active,
        current_pois,
        previous_pois
    FROM (
        SELECT 
            COALESCE(
                current_pois.site_id,
                previous_pois.site_id
            ) AS site_id,
            current_pois.current_pois,
            previous_pois.previous_pois
        FROM
            current_pois
        FULL OUTER JOIN
            previous_pois
        ON 
            current_pois.site_id = previous_pois.site_id
    )
)
