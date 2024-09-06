ASSERT (
    SELECT
        COUNT(*) AS duplicate_count
    FROM
        (
            SELECT
                local_date,
                store_id
            FROM
                `{{ var.value.env_project }}.{{ params['public_feeds_staging_dataset'] }}.{{ params['store_visits_daily_table'] }}_temp`
            GROUP BY
                local_date,
                store_id
            HAVING
                COUNT(*) > 1
        )
) = 0 AS 'Duplicate local_date and store_id pairs found in {{ var.value.env_project }}.{{ params["public_feeds_staging_dataset"] }}.{{ params["store_visits_daily_table"] }}_temp';