CREATE OR REPLACE TABLE
    `{{ var.value.env_project }}.{{ params['public_feeds_staging_dataset'] }}.{{ params['store_visits_daily_table'] }}_temp`
PARTITION BY
    local_date
CLUSTER BY
    store_id AS
WITH
    -- Helper to generate indices for array offset based on the day of the week from the week_starting
    date_helper AS (
        SELECT
            store_id,
            name,
            brand,
            street_address,
            city,
            state,
            zip_code,
            DATE_ADD(week_starting, INTERVAL idx DAY) AS local_date,
            idx,
            SPLIT(REGEXP_REPLACE(daily_visits, r'[\[\]]', ''), ',') AS daily_visits_array,
            sunday_hourly,
            monday_hourly,
            tuesday_hourly,
            wednesday_hourly,
            thursday_hourly,
            friday_hourly,
            saturday_hourly
        FROM
            `{{ var.value.env_project }}.{{ params['public_feeds_staging_dataset'] }}.{{ params['store_visits_temp_table'] }}`,
            UNNEST (GENERATE_ARRAY(0, 6)) AS idx -- Generate days from 0 (Sunday) to 6 (Saturday)
    ),
    -- Extracts the correct daily visit and hourly visits data for each date
    daily_data AS (
        SELECT
            store_id,
            name,
            brand,
            street_address,
            city,
            state,
            zip_code,
            local_date,
            CASE
                WHEN idx = 0 THEN SAFE_CAST(daily_visits_array[SAFE_OFFSET(0)] AS INT64)
                WHEN idx = 1 THEN SAFE_CAST(daily_visits_array[SAFE_OFFSET(1)] AS INT64)
                WHEN idx = 2 THEN SAFE_CAST(daily_visits_array[SAFE_OFFSET(2)] AS INT64)
                WHEN idx = 3 THEN SAFE_CAST(daily_visits_array[SAFE_OFFSET(3)] AS INT64)
                WHEN idx = 4 THEN SAFE_CAST(daily_visits_array[SAFE_OFFSET(4)] AS INT64)
                WHEN idx = 5 THEN SAFE_CAST(daily_visits_array[SAFE_OFFSET(5)] AS INT64)
                WHEN idx = 6 THEN SAFE_CAST(daily_visits_array[SAFE_OFFSET(6)] AS INT64)
                ELSE NULL
            END AS daily_visits,
            CASE idx
                WHEN 0 THEN sunday_hourly
                WHEN 1 THEN monday_hourly
                WHEN 2 THEN tuesday_hourly
                WHEN 3 THEN wednesday_hourly
                WHEN 4 THEN thursday_hourly
                WHEN 5 THEN friday_hourly
                WHEN 6 THEN saturday_hourly
            END AS hourly_visits
        FROM
            date_helper
    )
SELECT
    store_id,
    name,
    brand,
    street_address,
    city,
    state,
    zip_code,
    local_date,
    daily_visits,
    hourly_visits
FROM
    daily_data;