CREATE OR REPLACE TABLE
  `{{ var.value.env_project }}.{{ params['smc_regressors_dataset'] }}.{{ params['covid_dictionary_zipcode_table'] }}` AS
WITH
  zipcodes_table AS (
    SELECT
      pid AS fk_zipcodes,
      primary_city,
      county,
      -- if state is DC we use maryland
      IF(state = 'DC', 'MD', state) AS state
    FROM
      `{{ var.value.env_project }}.{{ params['area_geometries_dataset'] }}.{{ params['zipcodes_table'] }}`
  ),
  zipcodes_dictionary AS (
    SELECT
      fk_zipcodes,
      'covid_deaths' AS identifier_deaths,
      CONCAT('mobility_retail_and_recreation_', state) AS identifier_mobility,
      CONCAT('state_', county, '_44_45') AS identifier_county_restrictions,
      CONCAT('state_', primary_city, '_44_45') AS identifier_city_restrictions,
      CONCAT('state_', state, '_44_45') AS identifier_state_restrictions
    FROM
      zipcodes_table
  )
SELECT
  *
FROM
  zipcodes_dictionary;