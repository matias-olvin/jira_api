ASSERT (
    SELECT
        COUNT(*)
    FROM
        `{{ var.value.env_project }}.{{ params['public_feeds_staging_dataset'] }}.{{ params['store_visits_daily_table'] }}_temp`
    WHERE
        store_id IS NULL
        OR name IS NULL
        OR brand IS NULL
        OR street_address IS NULL
        OR city IS NULL
        OR state IS NULL
        OR zip_code IS NULL
) = 0 AS 'Nulls found in descriptor columns in {{ var.value.env_project }}.{{ params["public_feeds_staging_dataset"] }}.{{ params["store_visits_daily_table"] }}_temp'