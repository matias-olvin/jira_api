CREATE OR REPLACE TABLE `{{ var.value.env_project }}.{{ params['materialzed_views_dataset'] }}.{{ params['average_visits_table'] }}` AS
SELECT
    fk_sgplaces
    , SUM(visits) / DATE_DIFF(
        MAX(local_date), 
        MIN(local_date), 
        DAY
    ) AS avg_visits
FROM (
    SELECT 
        fk_sgplaces
        , local_date
        , visits
    FROM `{{ var.value.env_project }}.{{ params['poi_visits_scaled_dataset'] }}.*`
)
GROUP BY 
    fk_sgplaces 
    , local_date