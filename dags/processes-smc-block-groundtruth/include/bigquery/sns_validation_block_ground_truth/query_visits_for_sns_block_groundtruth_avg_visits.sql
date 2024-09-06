CREATE OR REPLACE TABLE
    `{{ var.value.env_project }}.{{ params['accessible_by_sns_dataset'] }}.{{ params['visits_daily_block_groundtruth_table'] }}_avg_visits_{{ params['sns_gtvm_column'] }}`
AS (
    SELECT
        fk_sgplaces,
        sum(visit_score_steps.{{ params['sns_gtvm_column'] }}) / nullif(date_diff(max(local_date), min(local_date), day), 0) avg_visits
    FROM `{{ var.value.env_project }}.{{ params['accessible_by_sns_dataset'] }}.{{ params['visits_daily_block_groundtruth_table'] }}`
    WHERE 
        local_date >= '2020-10-01' OR 
        local_date < '2020-03-01'
    GROUP BY fk_sgplaces
)