CREATE OR REPLACE TABLE
--  `storage-prod-olvin-com.postgres_materialize_views.DateRange`
  `{{ var.value.env_project }}.{{ params['postgres_mv_dataset'] }}.{{ params['daterange_table'] }}`
AS




SELECT
  LOWER("{{ params['sgmarket_benchmarking_table'] }}") AS table_name,
  MIN(local_date) AS min_date,
  MAX(local_date) AS max_date
FROM
--`storage-prod-olvin-com.postgres_batch_dataset.SGMarketBenchmarkingRaw`
  `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['sgmarket_benchmarking_table'] }}`

UNION ALL

SELECT
  LOWER("{{ params['sgcitymarket_benchmarking_table'] }}") AS table_name,
  MIN(local_date) AS min_date,
  MAX(local_date) AS max_date
FROM
--`storage-prod-olvin-com.postgres_batch_dataset.SGCityMarketBenchmarkingRaw`
  `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['sgcitymarket_benchmarking_table'] }}`

UNION ALL

SELECT
  LOWER("{{ params['sgstatemarket_benchmarking_table'] }}") AS table_name,
  MIN(local_date) AS min_date,
  MAX(local_date) AS max_date
FROM
--`storage-prod-olvin-com.postgres_batch_dataset.SGStateMarketBenchmarkingRaw`
  `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['sgstatemarket_benchmarking_table'] }}`

UNION ALL
-- Brand benchmarking tables

SELECT
  LOWER("{{ params['sgbrand_benchmarking_table'] }}") AS table_name,
  MIN(local_date) AS min_date,
  MAX(local_date) AS max_date
FROM
--`storage-prod-olvin-com.postgres_batch_dataset.SGBrandBenchmarkingRaw`
  `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['sgbrand_benchmarking_table'] }}`

UNION ALL

SELECT
  LOWER("{{ params['sgbrandcity_benchmarking_table'] }}") AS table_name,
  MIN(local_date) AS min_date,
  MAX(local_date) AS max_date
FROM
--`storage-prod-olvin-com.postgres_batch_dataset.SGBrandCityBenchmarkingRaw`
  `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['sgbrandcity_benchmarking_table'] }}`

UNION ALL

SELECT
  LOWER("{{ params['sgbrandstate_benchmarking_table'] }}") AS table_name,
  MIN(local_date) AS min_date,
  MAX(local_date) AS max_date
FROM
--`storage-prod-olvin-com.postgres_batch_dataset.SGBrandStateBenchmarkingRaw`
  `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['sgbrandstate_benchmarking_table'] }}`

UNION ALL
-- Malls tables

SELECT
  LOWER("{{ params['sgcenters_benchmarking_table'] }}") AS table_name,
  MIN(local_date) AS min_date,
  MAX(local_date) AS max_date
FROM
--`storage-prod-olvin-com.postgres_batch_dataset.SGCentersBenchmarkingRaw`
  `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['sgcenters_benchmarking_table'] }}`

UNION ALL

SELECT
  LOWER("{{ params['visits_to_malls_monthly_table'] }}") AS table_name,
  MIN(local_date) AS min_date,
  MAX(local_date) AS max_date
FROM
--`storage-prod-olvin-com.postgres_batch_dataset.SGMallsMonthlyVisitsRaw`
  `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['visits_to_malls_monthly_table'] }}`

UNION ALL
-- Place tables

SELECT
  LOWER("{{ params['SGPlaceBenchmarkingRaw_table'] }}") AS table_name,
  MIN(local_date) AS min_date,
  MAX(local_date) AS max_date
FROM
--`storage-prod-olvin-com.postgres_batch_dataset.SGPlaceBenchmarkingRaw`
  `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['SGPlaceBenchmarkingRaw_table'] }}`

UNION ALL

SELECT
  LOWER("{{ params['SGPlacePatternVisitsRaw_table'] }}") AS table_name,
  MIN(local_date) AS min_date,
  MAX(local_date) AS max_date
FROM
--`storage-prod-olvin-com.postgres_batch_dataset.SGPlacePatternVisitsRaw`
  `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['SGPlacePatternVisitsRaw_table'] }}`

UNION ALL

SELECT
  LOWER("{{ params['SGPlaceHomeZipCodeYearly_table'] }}") AS table_name,
  MIN(local_date) AS min_date,
  MAX(local_date) AS max_date
FROM
--`storage-prod-olvin-com.postgres_batch_dataset.SGPlaceHomeZipCodeYearly`
  `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['SGPlaceHomeZipCodeYearly_table'] }}`

UNION ALL

SELECT
  LOWER("{{ params['SGPlaceTradeAreaRaw_table'] }}") AS table_name,
  MIN(local_date) AS min_date,
  MAX(local_date) AS max_date
FROM
--`storage-prod-olvin-com.postgres.SGPlaceTradeAreaRaw`
  `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['SGPlaceTradeAreaRaw_table'] }}`

UNION ALL

-- Materialize views

SELECT
  LOWER("{{ params['cityzipcodesmonthlyvisits_table'] }}") AS table_name,
  MIN(local_date) AS min_date,
  MAX(local_date) AS max_date
FROM
--`storage-prod-olvin-com.postgres_materialize_views.CityZipcodesMonthlyVisits`
  `{{ var.value.env_project }}.{{ params['postgres_mv_dataset'] }}.{{ params['cityzipcodesmonthlyvisits_table'] }}`

-- Brand Visits
UNION ALL

SELECT
  LOWER("{{ params['brandhourlyvisits_table'] }}") AS table_name,
  MIN(local_date) AS min_date,
  MAX(local_date) AS max_date
FROM
--`storage-prod-olvin-com.postgres_materialize_views.SGBrandHourlyVisitsRaw`
  `{{ var.value.env_project }}.{{ params['postgres_mv_dataset'] }}.{{ params['brandhourlyvisits_table'] }}`

UNION ALL

SELECT
  LOWER("{{ params['branddailyvisits_table'] }}") AS table_name,
  MIN(local_date) AS min_date,
  MAX(local_date) AS max_date
FROM(
  SELECT
    DATE_ADD(local_date, INTERVAL row_number DAY) as local_date,
    CAST(visits as INT64) visits,
    fk_sgbrands,
  FROM (
    SELECT
      local_date,
      fk_sgbrands,
      JSON_EXTRACT_ARRAY(visits) AS visit_array, -- Get an array from the JSON string
    FROM
--`storage-prod-olvin-com.postgres_materialize_views.SGBrandDailyVisitsRaw`
  `{{ var.value.env_project }}.{{ params['postgres_mv_dataset'] }}.{{ params['branddailyvisits_table'] }}`
  )
  CROSS JOIN
    UNNEST(visit_array) AS visits -- Convert array elements to row
    WITH OFFSET AS row_number -- Get the position in the array as another column
)

UNION ALL

SELECT
  LOWER("{{ params['brandmonthlyvisits_table'] }}") AS table_name,
  MIN(local_date) AS min_date,
  MAX(local_date) AS max_date
FROM
--`storage-prod-olvin-com.postgres_materialize_views.SGBrandMonthlyVisitsRaw`
  `{{ var.value.env_project }}.{{ params['postgres_mv_dataset'] }}.{{ params['brandmonthlyvisits_table'] }}`

-- BrandState Visits
UNION ALL

SELECT
  LOWER("{{ params['brandstatehourlyvisits_table'] }}") AS table_name,
  MIN(local_date) AS min_date,
  MAX(local_date) AS max_date
FROM
--`storage-prod-olvin-com.postgres_materialize_views.SGBrandStateHourlyVisitsRaw`
  `{{ var.value.env_project }}.{{ params['postgres_mv_dataset'] }}.{{ params['brandstatehourlyvisits_table'] }}`

UNION ALL

SELECT
  LOWER("{{ params['brandstatedailyvisits_table'] }}") AS table_name,
  MIN(local_date) AS min_date,
  MAX(local_date) AS max_date
FROM(
  SELECT
    DATE_ADD(local_date, INTERVAL row_number DAY) as local_date,
    CAST(visits as INT64) visits,
    fk_sgbrands,
    state,
  FROM (
    SELECT
      local_date,
      fk_sgbrands,
      state,
      JSON_EXTRACT_ARRAY(visits) AS visit_array, -- Get an array from the JSON string
    FROM
--`storage-prod-olvin-com.postgres_materialize_views.SGBrandStateDailyVisitsRaw`
  `{{ var.value.env_project }}.{{ params['postgres_mv_dataset'] }}.{{ params['brandstatedailyvisits_table'] }}`
  )
  CROSS JOIN
    UNNEST(visit_array) AS visits -- Convert array elements to row
    WITH OFFSET AS row_number -- Get the position in the array as another column
)

UNION ALL

SELECT
  LOWER("{{ params['brandstatemonthlyvisits_table'] }}") AS table_name,
  MIN(local_date) AS min_date,
  MAX(local_date) AS max_date
FROM
--`storage-prod-olvin-com.postgres_materialize_views.SGBrandStateMonthlyVisitsRaw`
  `{{ var.value.env_project }}.{{ params['postgres_mv_dataset'] }}.{{ params['brandstatemonthlyvisits_table'] }}`

-- Place Cameo
UNION ALL

SELECT
  LOWER("{{ params['SGPlaceCameoMonthlyRaw_table'] }}") AS table_name,
  MIN(local_date) AS min_date,
  MAX(local_date) AS max_date
FROM
--`storage-prod-olvin-com.postgres_materialize_views.SGPlaceCameoMonthlyRaw`
  `{{ var.value.env_project }}.{{ params['postgres_mv_dataset'] }}.{{ params['SGPlaceCameoMonthlyRaw_table'] }}`
WHERE length(cameo_scores) > 2

-- Place Visits
UNION ALL

SELECT
  LOWER("{{ params['SGPlaceHourlyVisitsRaw_table'] }}") AS table_name,
  MIN(local_date) AS min_date,
  MAX(local_date) AS max_date
FROM
--`storage-prod-olvin-com.postgres_materialize_views.SGPlaceHourlyVisitsRaw`
  `{{ var.value.env_project }}.{{ params['postgres_mv_dataset'] }}.{{ params['SGPlaceHourlyVisitsRaw_table'] }}`

UNION ALL

SELECT
  LOWER("{{ params['SGPlaceDailyVisitsRaw_table'] }}") AS table_name,
  MIN(local_date) AS min_date,
  MAX(local_date) AS max_date
FROM(
  SELECT
    DATE_ADD(local_date, INTERVAL row_number DAY) as local_date,
    CAST(visits as INT64) visits,
    fk_sgplaces,
  FROM (
    SELECT
      local_date,
      fk_sgplaces,
      JSON_EXTRACT_ARRAY(visits) AS visit_array, -- Get an array from the JSON string
    FROM
--`storage-prod-olvin-com.postgres_materialize_views.SGPlaceDailyVisitsRaw`
  `{{ var.value.env_project }}.{{ params['postgres_mv_dataset'] }}.{{ params['SGPlaceDailyVisitsRaw_table'] }}`
  )
  CROSS JOIN
    UNNEST(visit_array) AS visits -- Convert array elements to row
    WITH OFFSET AS row_number -- Get the position in the array as another column
)

UNION ALL

SELECT
  LOWER("{{ params['SGPlaceMonthlyVisitsRaw_table'] }}") AS table_name,
  MIN(local_date) AS min_date,
  MAX(local_date) AS max_date
FROM
--`storage-prod-olvin-com.postgres_materialize_views.SGPlaceMonthlyVisitsRaw`
  `{{ var.value.env_project }}.{{ params['postgres_mv_dataset'] }}.{{ params['SGPlaceMonthlyVisitsRaw_table'] }}`

UNION ALL

SELECT
  LOWER("{{ params['SGPlaceHomeCensusBlockGroupsYearly_table'] }}") AS table_name,
  MIN(local_date) AS min_date,
  MAX(local_date) AS max_date
FROM
--`storage-prod-olvin-com.postgres_batch.SGPlaceTradeAreaRaw`
  `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['SGPlaceHomeCensusBlockGroupsYearly_table'] }}`

UNION ALL

SELECT 'RefreshedAt', CURRENT_DATE() AS min_date, CURRENT_DATE() AS max_date