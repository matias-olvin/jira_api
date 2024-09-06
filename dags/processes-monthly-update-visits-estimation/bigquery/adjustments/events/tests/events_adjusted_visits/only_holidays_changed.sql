
SELECT COUNTIF(is_holiday IS NULL)
FROM (
    SELECT local_date
    FROM (
        SELECT a.visits/NULLIF(b.visits, 0) AS factor, local_date
        FROM
            `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['adjustments_events_output_table'] }}` a
        INNER JOIN
            `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['quality_output_table'] }}` b
            USING(fk_sgplaces, local_date)
    )
    WHERE factor <> 1 AND factor IS NOT NULL
    GROUP BY local_date
)
LEFT JOIN (
    SELECT local_date, TRUE AS is_holiday
    FROM
        `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['events_holidays_poi_table'] }}`
    GROUP BY local_date
)
USING(local_date)