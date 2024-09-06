CREATE OR REPLACE TABLE
    `{{ var.value.env_project }}.{{ params['manually_add_pois_dataset'] }}.{{ params['sgplace_activity_table'] }}` AS
SELECT -- ANY SCHEMA CHANGES TO SGPLACEACTIVITY NEEDS TO BE ACCOUNTED FOR HERE
    fk_sgplaces,
    CASE WHEN has_hourly_visits THEN 'active'
         ELSE CASE WHEN has_monthly_visits THEN 'limited_data'
                   ELSE 'no_data'
                   END
         END AS activity,
    1. AS confidence_level,
    FALSE AS home_locations,
    IFNULL(has_cross_shopping, FALSE) AS connections,
    FALSE AS trade_area_activity,
    IFNULL(has_cross_shopping, FALSE) AS cross_shopping_activity
FROM
    `{{ var.value.env_project }}.{{ params['manually_add_pois_dataset'] }}.{{ params['manually_add_pois_input_table'] }}`
LEFT JOIN(
  SELECT DISTINCT pid AS fk_sgplaces, TRUE AS has_cross_shopping
  FROM `{{ var.value.env_project }}.{{ params['manually_add_pois_dataset'] }}.{{ params['sgplace_visitor_brand_destination_table'] }}`
)
USING(fk_sgplaces)
LEFT JOIN(
  SELECT DISTINCT fk_sgplaces, TRUE AS has_hourly_visits
  FROM `{{ var.value.env_project }}.{{ params['manually_add_pois_dataset'] }}.{{  params['sgplace_hourly_visits_raw_table'] }}`
)
USING(fk_sgplaces)
LEFT JOIN(
  SELECT DISTINCT fk_sgplaces, TRUE AS has_monthly_visits
  FROM `{{ var.value.env_project }}.{{ params['manually_add_pois_dataset'] }}.{{ params['sgplace_monthly_visits_raw'] }}`
)
USING(fk_sgplaces)
WHERE
    added_date >= CAST(
        '{{ var.value.manually_add_pois_deadline_date }}' AS DATE
    )
    AND fk_sgbrands IN (
        SELECT pid
        FROM `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['sgbrandraw_table'] }}`
    );