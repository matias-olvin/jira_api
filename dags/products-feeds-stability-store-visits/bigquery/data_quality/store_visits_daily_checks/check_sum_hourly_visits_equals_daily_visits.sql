ASSERT (
    SELECT
        COUNT(*)
    FROM
        (
            SELECT DISTINCT
                store_id,
                local_date,
                (
                    SELECT
                        SUM(CAST(item AS INT64))
                    FROM
                        UNNEST (
                            SPLIT(REGEXP_REPLACE(hourly_visits, r'[\[\]]', ''))
                        ) AS item
                ) - daily_visits AS difference
            FROM
                `{{ var.value.env_project }}.{{ params['public_feeds_staging_dataset'] }}.{{ params['store_visits_daily_table'] }}_temp`
        )
    WHERE
        difference > 10
        OR difference < -10
) = 0 AS 'Sum of hourly visits does not equal daily visits in {{ var.value.env_project }}.{{ params["public_feeds_staging_dataset"] }}.{{ params["store_visits_daily_table"] }}_temp';