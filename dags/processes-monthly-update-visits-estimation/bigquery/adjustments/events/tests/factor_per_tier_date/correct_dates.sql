
SELECT COUNTIF(identifier IS NULL)
FROM (
    SELECT local_date, b.identifier
    FROM (
        SELECT local_date
        FROM
            `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['event_factor_tier_table'] }}`
        GROUP BY local_date
        )
    LEFT JOIN (
        SELECT local_date, identifier
        FROM
            `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['events_holidays_poi_table'] }}`
        GROUP BY local_date, identifier
    ) b
    USING(local_date)
)