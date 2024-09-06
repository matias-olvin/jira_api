DECLARE reference_date DATE;
SET reference_date = DATE_TRUNC(DATE_SUB(CAST("{{ ds }}" AS DATE), INTERVAL 1 MONTH), MONTH);


CREATE OR REPLACE TABLE `{{ params['bigquery-project'] }}.{{ params['postgres-rt-dataset'] }}.{{ params['sgplaceranking-table'] }}` AS (
  WITH monthly_quarter_yearly_visits AS (
    SELECT
      fk_sgplaces
      , SUM(IF((local_date <= (DATE_TRUNC(DATE_SUB(CAST(reference_date AS DATE), INTERVAL 0 MONTH), MONTH))) AND (local_date >= DATE_SUB((DATE_TRUNC(DATE_SUB(CAST(reference_date AS DATE), INTERVAL 0 MONTH), MONTH)), INTERVAL 0 MONTH)), visits, NULL)) AS last_month_visits
      , SUM(IF((local_date <= (DATE_TRUNC(DATE_SUB(CAST(reference_date AS DATE), INTERVAL 0 MONTH), MONTH))) AND (local_date >= DATE_SUB((DATE_TRUNC(DATE_SUB(CAST(reference_date AS DATE), INTERVAL 0 MONTH), MONTH)), INTERVAL 2 MONTH)), visits, NULL)) AS last_quarter_visits
      , SUM(IF((local_date <= (DATE_TRUNC(DATE_SUB(CAST(reference_date AS DATE), INTERVAL 0 MONTH), MONTH))) AND (local_date >= DATE_SUB((DATE_TRUNC(DATE_SUB(CAST(reference_date AS DATE), INTERVAL 0 MONTH), MONTH)), INTERVAL 11 MONTH)), visits, NULL)) AS last_year_visits
    FROM `{{ params['bigquery-project'] }}.{{ params['postgres-rt-dataset'] }}.{{ params['sgplacemonthlyvisitsraw-table'] }}`
    GROUP BY fk_sgplaces
      HAVING last_month_visits > 0 AND last_quarter_visits > 0 AND last_year_visits > 0

  )
  , adding_region_cbsa_brand AS (
    SELECT *
    FROM monthly_quarter_yearly_visits
    INNER JOIN (
      SELECT
        pid AS fk_sgplaces
        , region
        , fk_sgbrands
      FROM `{{ params['bigquery-project'] }}.{{ params['postgres-batch-dataset'] }}.{{ params['sgplaceraw-table'] }}`
    ) USING (fk_sgplaces)
    LEFT JOIN (
      SELECT
        pid AS fk_sgplaces
        , cbsa_fips_code
      FROM `{{ params['bigquery-project'] }}.{{ params['postgres-batch-dataset'] }}.{{ params['sgplaceraw-table'] }}`
      INNER JOIN `{{ var.value.env_project }}.{{ params['area-geometries-dataset'] }}.{{ params['geo-us-boundaries-cbsa-table'] }}`
        ON ST_WITHIN(ST_GEOGPOINT(longitude, latitude), cbsa_geom)
    ) USING (fk_sgplaces)
  )
  , adding_ranking AS (
    SELECT
      reference_date AS local_date
      , fk_sgplaces
      , last_month_visits
      , ROW_NUMBER() OVER (PARTITION BY fk_sgbrands ORDER BY last_month_visits DESC) AS nat_rank_last_month
      , ROW_NUMBER() OVER (PARTITION BY fk_sgbrands, region ORDER BY last_month_visits DESC) AS state_rank_last_month
      , IF(cbsa_fips_code IS NULL, NULL, ROW_NUMBER() OVER (PARTITION BY fk_sgbrands, cbsa_fips_code ORDER BY last_month_visits DESC)) AS CBSA_rank_last_month
      , last_quarter_visits
      , ROW_NUMBER() OVER (PARTITION BY fk_sgbrands ORDER BY last_quarter_visits DESC) AS nat_rank_last_quarter
      , ROW_NUMBER() OVER (PARTITION BY fk_sgbrands, region ORDER BY last_quarter_visits DESC) AS state_rank_last_quarter
      , IF(cbsa_fips_code IS NULL, NULL, ROW_NUMBER() OVER (PARTITION BY fk_sgbrands, cbsa_fips_code ORDER BY last_quarter_visits DESC)) AS CBSA_rank_last_quarter
      , last_year_visits
      , ROW_NUMBER() OVER (PARTITION BY fk_sgbrands ORDER BY last_year_visits DESC) AS nat_rank_last_year
      , ROW_NUMBER() OVER (PARTITION BY fk_sgbrands, region ORDER BY last_year_visits DESC) AS state_rank_last_year
      , IF(cbsa_fips_code IS NULL, NULL, ROW_NUMBER() OVER (PARTITION BY fk_sgbrands, cbsa_fips_code ORDER BY last_year_visits DESC)) AS CBSA_rank_last_year
      , COUNT(*) OVER (PARTITION BY fk_sgbrands) AS total_nat
      , COUNT(*) OVER (PARTITION BY fk_sgbrands, region) AS total_state
      , COUNT(*) OVER (PARTITION BY fk_sgbrands, cbsa_fips_code) AS total_CBSA
    FROM adding_region_cbsa_brand
  )
  , adding_percentile AS (
    SELECT
      local_date
      , fk_sgplaces
      , CAST(last_month_visits AS int64) AS last_month_visits
      , TO_JSON_STRING([nat_rank_last_month, total_nat]) AS nat_rank_last_month
      , CAST(100 * (1 - ((2 * nat_rank_last_month - 1) / (2 * total_nat))) AS int64) AS nat_perc_rank_last_month
      , TO_JSON_STRING([state_rank_last_month, total_state]) AS state_rank_last_month
      , CAST(100 * (1 - ((2 * state_rank_last_month - 1) / (2 * total_state))) AS int64) AS state_perc_rank_last_month
      , IF(CBSA_rank_last_month IS NULL, NULL, TO_JSON_STRING([CBSA_rank_last_month, total_CBSA])) AS CBSA_rank_last_month
      , CAST(100 * (1 - ((2 * CBSA_rank_last_month - 1) / (2 * total_CBSA))) AS int64) AS CBSA_perc_rank_last_month
      , CAST(last_quarter_visits AS int64) AS last_quarter_visits
      , TO_JSON_STRING([nat_rank_last_quarter, total_nat]) AS nat_rank_last_quarter
      , CAST(100 * (1 - ((2 * nat_rank_last_quarter - 1) / (2 * total_nat))) AS int64) AS nat_perc_rank_last_quarter
      , TO_JSON_STRING([state_rank_last_quarter, total_state]) AS state_rank_last_quarter
      , CAST(100 * (1 - ((2 * state_rank_last_quarter - 1) / (2 * total_state))) AS int64) AS state_perc_rank_last_quarter
      , IF(CBSA_rank_last_quarter IS NULL, NULL, TO_JSON_STRING([CBSA_rank_last_quarter, total_CBSA])) AS CBSA_rank_last_quarter
      , CAST(100 * (1 - ((2 * CBSA_rank_last_quarter - 1) / (2 * total_CBSA))) AS int64) AS CBSA_perc_rank_last_quarter
      , CAST(last_year_visits AS int64) AS last_year_visits
      , TO_JSON_STRING([nat_rank_last_year, total_nat]) AS nat_rank_last_year
      , CAST(100 * (1 - ((2 * nat_rank_last_year - 1) / (2 * total_nat))) AS int64) AS nat_perc_rank_last_year
      , TO_JSON_STRING([state_rank_last_year,total_state]) AS state_rank_last_year
      , CAST(100 * (1 - ((2 * state_rank_last_year - 1) / (2 * total_state))) AS int64) AS state_perc_rank_last_year
      , IF(CBSA_rank_last_year IS NULL, NULL, TO_JSON_STRING([CBSA_rank_last_year, total_CBSA])) AS CBSA_rank_last_year
      , CAST(100 * (1 - ((2 * CBSA_rank_last_year - 1) / (2 * total_CBSA))) AS int64) AS CBSA_perc_rank_last_year
    FROM adding_ranking
  )
  SELECT *
  FROM adding_percentile
)