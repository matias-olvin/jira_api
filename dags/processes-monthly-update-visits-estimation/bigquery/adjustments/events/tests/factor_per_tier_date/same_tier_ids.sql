
SELECT COUNTIF(factor_table IS NULL OR ref_date_table IS NULL)
FROM (
    SELECT tier_id, TRUE AS factor_table
    FROM
        `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['event_factor_tier_table'] }}`
    GROUP BY tier_id
)
FULL OUTER JOIN (
    SELECT tier_id, TRUE AS ref_date_table
    FROM
        `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['tier_events_table'] }}`
    GROUP BY tier_id
)
USING(tier_id)