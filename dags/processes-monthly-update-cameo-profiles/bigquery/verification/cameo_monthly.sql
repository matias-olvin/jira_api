INSERT
`{{ var.value.env_project }}.{{ params['cameo_staging_dataset'] }}.{{ params['cameo_monitoring_check_table'] }}`

SELECT
    DATE("{{ ds }}") AS run_date,
    total - empty AS pois_with_cameo,
    empty AS pois_without_cameo,
    empty/total AS perc_missing,
    case when empty/total > 0.2 then TRUE else FALSE end AS warning,
    case when empty/total > 0.25 then TRUE else FALSE end AS terminate
FROM (
  SELECT count(*) as empty
  FROM
    `{{ var.value.env_project }}.{{ params['postgres_dataset'] }}.{{ params['cameo_monthly_table'] }}`
  WHERE local_date = DATE_TRUNC("{{ ds }}", MONTH)
    AND length(cameo_scores) < 3
)
join (
  SELECT count(*) as total
  FROM
    `{{ var.value.env_project }}.{{ params['postgres_dataset'] }}.{{ params['cameo_monthly_table'] }}`
  WHERE local_date = DATE_TRUNC("{{ ds }}", MONTH)
)
ON TRUE;

SELECT terminate
FROM
`{{ var.value.env_project }}.{{ params['cameo_staging_dataset'] }}.{{ params['cameo_monitoring_check_table'] }}`
WHERE run_date = DATE("{{ ds }}")
