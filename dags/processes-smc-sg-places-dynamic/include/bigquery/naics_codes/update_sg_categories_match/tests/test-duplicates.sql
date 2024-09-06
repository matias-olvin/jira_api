ASSERT (
    SELECT
        COUNT(*)
    FROM
        (
            SELECT
                COUNT(*) AS _count
            FROM
                `{{ var.value.env_project }}.{{ params['sg_base_tables_staging_dataset'] }}.{{ params['categories_match_table'] }}_staging`
            GROUP BY
                naics_code
        )
    WHERE
        _count > 1
) = 0 AS "Error: naics_code duplicates found in {{ var.value.env_project }}.{{ params['sg_base_tables_staging_dataset'] }}.{{ params['categories_match_table'] }}_staging";