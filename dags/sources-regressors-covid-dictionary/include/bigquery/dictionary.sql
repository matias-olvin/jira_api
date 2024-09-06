CREATE OR REPLACE TABLE
  `{{ var.value.env_project }}.{{ params['smc_regressors_dataset'] }}.{{ params['covid_dictionary_table'] }}` AS
WITH
  category_match AS (
    SELECT
      naics_code,
      olvin_category
    FROM
      `{{ var.value.env_project }}.{{ params['sg_base_tables_dataset'] }}.{{ params['naics_code_subcategories_table'] }}`
  ),
  places AS (
    SELECT
      pid AS fk_sgplaces,
      naics_code AS naics_code,
      -- if state is DC we use maryland
      IF(region = 'DC', 'MD', region) AS state_code
    FROM
      `{{ var.value.env_project }}.{{ params['smc_sg_places_dataset'] }}.{{ params['places_dynamic_table'] }}`
  ),
  category_mobility AS (
    SELECT
      olvin_category,
      covid_regressor
    FROM
      `{{ var.value.env_project }}.{{ params['covid_factors_dataset'] }}.{{ params['category_factor_table'] }}`
  ),
  poi_representation AS (
    SELECT
      fk_sgplaces,
      -- IFNULL(olvin_category,
      --   33000) AS olvin_category,
      olvin_category,
      state_code
    FROM
      places
      LEFT JOIN category_match USING (naics_code)
  ),
  joining_list_regressors AS (
    SELECT
      fk_sgplaces,
      state_code,
      TRUE AS covid_deaths_bool,
      SUBSTR(covid_regressor, 0, LENGTH(covid_regressor) - 29) AS covid_mobility,
      state_code AS covid_mobility_location,
      TRUE AS covid_restrictions_bool,
      state_code AS covid_restrictions_location
    FROM
      poi_representation
      LEFT JOIN category_mobility USING (olvin_category)
  ),
  covid_restrictions_dictionary AS (
    SELECT
      *
    FROM
      `{{ var.value.env_project }}.{{ params['smc_regressors_staging_dataset'] }}.{{ params['covid_restrictions_dictionary_table'] }}`
  ),
  covid_dictionary_unformatted AS (
    SELECT
      *
    FROM
      joining_list_regressors
      INNER JOIN covid_restrictions_dictionary USING (fk_sgplaces)
  ),
  covid_dictionary AS (
    SELECT
      fk_sgplaces,
      'covid_deaths' AS identifier_deaths,
      CONCAT(
        'mobility_',
        covid_mobility,
        '_',
        covid_mobility_location
      ) AS identifier_mobility,
      CONCAT(
        'county_',
        SUBSTR(county, 0, LENGTH(county) - 7),
        '_',
        general_code
      ) AS identifier_county_restrictions,
      CONCAT('city_', primary_city, '_', general_code) AS identifier_city_restrictions,
      CONCAT('state_', state, '_', general_code) AS identifier_state_restrictions,
    FROM
      covid_dictionary_unformatted
  )
SELECT
  *
FROM
  covid_dictionary;