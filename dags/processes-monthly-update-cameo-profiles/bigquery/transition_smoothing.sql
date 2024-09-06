INSERT
`{{ params['storage-prod'] }}.{{ params['cameo_staging_dataset'] }}.{{ params['smoothed_transition_table'] }}`
  SELECT fk_sgplaces, CAMEO_USA, local_date,
CAST(ROUND(weighted_visit_score) AS INT64) AS weighted_visit_score
 FROM
  `{{ params['storage-prod'] }}.{{ params['cameo_staging_dataset'] }}.{{ params['cameo_ema_24_table'] }}`
  WHERE local_date >= DATE_TRUNC("{{ ds }}", MONTH) AND local_date < DATE_TRUNC(DATE_ADD("{{ ds }}", INTERVAL 1 MONTH), MONTH)