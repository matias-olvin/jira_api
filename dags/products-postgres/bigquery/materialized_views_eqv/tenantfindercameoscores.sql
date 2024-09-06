CREATE OR REPLACE TABLE
  `{{ var.value.env_project }}.{{ params['postgres_mv_dataset'] }}.{{ params['tenantfindercameoscores_table'] }}`
AS

WITH

extract_cameo_scores AS(
 SELECT
    fk_sgplaces,
    local_date,
    LEFT(cameo_scores, STRPOS(cameo_scores, ':')-1) as CAMEO_USA,
    CAST(REVERSE(LEFT(REVERSE(cameo_scores), STRPOS(REVERSE(cameo_scores),':')-1)) AS INT64) as visit_score
  FROM(
    SELECT fk_sgplaces, REPLACE(REPLACE(REPLACE(cameo_scores,'{',''),'}',''),'"','') as cameo_scores, local_date
    FROM(
      SELECT fk_sgplaces, REPLACE(REPLACE(REPLACE(cameo_scores,'{','[{'),'}','}]'),',','},{') AS cameo_scores_monthly, local_date
      FROM
      `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['SGPlaceCameoMonthlyRaw_table'] }}`
      WHERE local_date > DATE_SUB(DATE('2024-06-01'), INTERVAL 12 MONTH)
      )
    CROSS JOIN UNNEST(JSON_EXTRACT_ARRAY(cameo_scores_monthly)) AS cameo_scores
    WHERE cameo_scores like('%:%')
  )
),

agg_cameo_scores AS(
  SELECT fk_sgplaces, CAMEO_USA, SUM(visit_score) AS visit_score
  FROM extract_cameo_scores
  GROUP BY fk_sgplaces, CAMEO_USA
),

format_cameo_scores AS(
  SELECT fk_sgplaces, REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(TO_JSON_STRING(ARRAY_AGG(STRUCT(CAST(CAMEO_USA AS STRING) AS CAMEO_USA, visit_score AS visit_score))), ',"visit_score"', ''), '"CAMEO_USA":', ''), '},{', ','), ']', ''), '[', '') AS cameo_scores
  FROM agg_cameo_scores
  GROUP BY fk_sgplaces
)

SELECT *
FROM format_cameo_scores
INNER JOIN(
  SELECT pid AS fk_sgplaces, fk_sgbrands, region, longitude, latitude
  FROM
    `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['sgplaceraw_table'] }}`
  WHERE pid IN(
    SELECT fk_sgplaces
    FROM
      `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['sgplaceactivity_table'] }}`
    WHERE activity IN('active', 'limited_data')
  )
  AND naics_code <> 531120
)
USING(fk_sgplaces)