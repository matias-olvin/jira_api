SELECT COUNTIF(count_duplicates <> 0)
FROM (
  SELECT tier_id, local_date, count_ref_dates - IFNULL(count_base,0) as count_duplicates
  FROM (
    SELECT
      tier_id,
      local_date,
      COUNT(*) as count_ref_dates
    FROM
        `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['tier_events_table'] }}`
    GROUP BY
      tier_id,
      local_date
  )
  LEFT JOIN (
    SELECT tier_id, COUNT(*) AS count_base
    FROM
        `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['class_pois_sns_table'] }}`
    INNER JOIN
        `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['list_sns_pois_table'] }}`
    USING(fk_sgplaces)
    GROUP BY tier_id
  )
  USING(tier_id)
)