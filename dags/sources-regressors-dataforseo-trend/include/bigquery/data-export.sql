EXPORT DATA
  OPTIONS (
    uri = 'gs://{{ params["dataforseo-bucket"] }}/{{ params["ENDPOINT"] }}/ingestion_date={{ ds }}/{{ params["PREFIX"] }}/*.csv'
    , format = 'CSV'
    , OVERWRITE = TRUE
    , header = TRUE
    , field_delimiter = ','
  ) AS (
    WITH
      all_brands AS (
        SELECT
          pid
          , name AS brand
        FROM
          `{{ var.value.env_project }}.{{ params['postgres_dataset'] }}.{{ params['sgbrandraw_table'] }}`
      )
      , brand_web_5 AS (
        SELECT
          pid
          , brand AS keywords
          , "web" AS type
          , DATE_SUB(CURRENT_DATE(), INTERVAL 5 YEAR) AS date_from
          , CURRENT_DATE() AS date_to
        FROM all_brands
      )
      , brand_web_10 AS (
        SELECT
          pid
          , brand AS keywords
          , "web" AS type
          , DATE_SUB(CURRENT_DATE(), INTERVAL 10 YEAR) AS date_from
          , DATE_SUB(CURRENT_DATE(), INTERVAL 5 YEAR) AS date_to
        FROM all_brands
      )
      , brand_near_me_web_5 AS (
        SELECT
          pid
          , CONCAT(brand, " near me") AS keywords
          , "web" AS type
          , DATE_SUB(CURRENT_DATE(), INTERVAL 5 YEAR) AS date_from
          , CURRENT_DATE() AS date_to
        FROM all_brands 
      )
      , brand_near_me_web_10 AS (
        SELECT
          pid
          , CONCAT(brand, " near me") AS keywords
          , "web" AS type
          , DATE_SUB(CURRENT_DATE(), INTERVAL 10 YEAR) AS date_from
          , DATE_SUB(CURRENT_DATE(), INTERVAL 5 YEAR) AS date_to
        FROM all_brands 
      )
      , brand_froogle_5 AS (
        SELECT
          pid
          , brand AS keywords
          , "froogle" AS type
          , DATE_SUB(CURRENT_DATE(), INTERVAL 5 YEAR) AS date_from
          , CURRENT_DATE() AS date_to
        FROM all_brands
      )
      , brand_froogle_10 AS (
        SELECT
          pid
          , brand AS keywords
          , "froogle" AS type
          , DATE_SUB(CURRENT_DATE(), INTERVAL 10 YEAR) AS date_from
          , DATE_SUB(CURRENT_DATE(), INTERVAL 5 YEAR) AS date_to
        FROM all_brands
      )
      
    SELECT *
    FROM brand_web_5
    UNION ALL
    SELECT *
    FROM brand_web_10
    UNION ALL
    SELECT *
    FROM brand_near_me_web_5
    UNION ALL
    SELECT *
    FROM brand_near_me_web_10
    UNION ALL
    SELECT *
    FROM brand_froogle_5
    UNION ALL
    SELECT *
    FROM brand_froogle_10
  );
