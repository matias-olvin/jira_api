ASSERT (
    SELECT
        COUNT(*)
    FROM
        (
            SELECT DISTINCT
                (
                    ARRAY_LENGTH(
                        SPLIT(REGEXP_REPLACE(hourly_visits, r'[\[\]]', ''))
                    )
                ) number_of_hours_in_day
            FROM
                `{{ var.value.env_project }}.{{ params['public_feeds_staging_dataset'] }}.{{ params['store_visits_daily_table'] }}_temp`
        )
    WHERE
        number_of_hours_in_day != 24
) = 0 AS 'More than 24 hours in hourly_visits column in {{ var.value.env_project }}.{{ params["public_feeds_staging_dataset"] }}.{{ params["store_visits_daily_table"] }}_temp'