ASSERT
  (
  SELECT
    COUNT(*)
  FROM
    `{{ var.value.env_project }}.{{ params['smc_sg_places_dataset'] }}.{{ params['sg_places_filter_table'] }}` t1
  LEFT JOIN
    `{{ var.value.env_project }}.{{ params['smc_sg_places_dataset'] }}.{{ params['places_dynamic_table'] }}` t2
  ON
    t1.fk_sgplaces=t2.pid
  WHERE
    t2.fk_sgbrands IS NOT NULL) = (
  SELECT
    COUNT(*)
  FROM
    `{{ var.value.env_project }}.{{ params['smc_sg_places_dataset'] }}.{{ params['sg_places_filter_table'] }}`
  WHERE
    current_filter=TRUE)