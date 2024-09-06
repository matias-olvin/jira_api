EXPORT DATA OPTIONS(
    uri = '{{ ti.xcom_pull(task_ids="uris_xcom_push_new_brands")["query_input_uri"] }}',
    FORMAT = 'CSV',
    overwrite = TRUE,
    header = TRUE,
    compression = 'GZIP'
) AS
WITH
    -- Store in a CTE table all sub categories we do have
    median_visits_per_sub_category_excluding_missing AS (
        SELECT DISTINCT
            (sub_category),
            median_category_visits,
            category_median_visits_range,
            NULL AS median_brand_visits
        FROM
            `{{ var.value.env_project }}.{{ ti.xcom_pull(task_ids='push_dataset_name_to_xcom') }}.{{ params['prior_brand_visits_table'] }}`
        WHERE
            sub_category IS NOT NULL
    ),
    median_missing AS (
        SELECT
            * EXCEPT (updated_at)
        FROM
            `{{ var.value.env_project }}.{{ ti.xcom_pull(task_ids='push_dataset_name_to_xcom') }}.{{ params['categories_to_append_table'] }}`
        WHERE
            updated_at >= CAST("{{ var.value.smc_start_date }}" AS DATE)
    )
    -- We left join brand info with median_visits using sub_category 
    -- If subcategory does not exist in current_prior_brand_visits or in categories_to_append, we set default values
SELECT
    b.name,
    b.pid,
    b.sub_category,
    COALESCE(
        median.median_category_visits,
        median_missing.median_category_visits,
        10000
    ) AS median_category_visits,
    COALESCE(
        median.category_median_visits_range,
        median_missing.category_median_visits_range,
        10
    ) AS category_median_visits_range,
    median.median_brand_visits
FROM
    `{{ var.value.env_project }}.{{ ti.xcom_pull(task_ids='push_dataset_name_to_xcom') }}.{{ params['missing_brands_table'] }}` b
    LEFT JOIN median_visits_per_sub_category_excluding_missing median USING (sub_category)
    LEFT JOIN median_missing median_missing USING (sub_category);