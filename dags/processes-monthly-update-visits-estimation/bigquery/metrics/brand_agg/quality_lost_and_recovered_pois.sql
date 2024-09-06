DELETE FROM
    `{{ var.value.env_project }}.{{ params['metrics_dataset'] }}.{{ params['brand_quality_lost_table'] }}`
WHERE run_date=DATE('{{ds}}')
;

INSERT INTO
    `{{ var.value.env_project }}.{{ params['metrics_dataset'] }}.{{ params['brand_quality_lost_table'] }}`

WITH

spurious_pois AS(
  SELECT DISTINCT fk_sgplaces
  FROM
    `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['spurious_places_table'] }}`
  WHERE NOT previous
),

spurious_and_recovered AS(
  SELECT fk_sgplaces, IFNULL(recovered, FALSE) AS recovered
  FROM spurious_pois
  LEFT JOIN(
    SELECT DISTINCT fk_sgplaces, TRUE as recovered
    FROM
      `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['quality_output_previous_table'] }}`
    LEFT JOIN(
      SELECT distinct fk_sgplaces, previous
      FROM
        `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['spurious_places_table'] }}`
      WHERE previous
    )
    USING(fk_sgplaces)
    WHERE previous IS NULL
  )
  USING(fk_sgplaces)
),

spurious_pois_with_brand AS(
  SELECT *
  FROM spurious_and_recovered
  INNER JOIN(
    SELECT pid AS fk_sgplaces, fk_sgbrands
    FROM
      `{{ var.value.env_project }}.{{ params['postgres_dataset'] }}.{{ params['sgplaceraw_table'] }}`
  )
  USING(fk_sgplaces)
),

count_spurious AS(
  SELECT fk_sgbrands, COUNT(*) AS num_spurious, COUNTIF(recovered) AS num_recovered
  FROM spurious_pois_with_brand
  GROUP BY fk_sgbrands
)

SELECT DATE('{{ds}}') AS run_date, brand, fk_sgbrands, IFNULL(num_spurious, 0) AS num_spurious, IFNULL(num_recovered,0) AS num_recovered
FROM(
  SELECT name AS brand, pid AS fk_sgbrands
  FROM
    `{{ var.value.env_project }}.{{ params['postgres_dataset'] }}.{{ params['sgbrandraw_table'] }}`
)
LEFT JOIN count_spurious
USING(fk_sgbrands)