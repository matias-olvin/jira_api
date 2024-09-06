CREATE OR REPLACE TABLE
    `{{ params['storage-prod'] }}.{{ params['postgres_dataset'] }}.{{ params['cameo_monthly_table'] }}`
    PARTITION BY local_date CLUSTER BY fk_sgplaces
AS

SELECT
      fk_sgplaces,
      DATE_ADD(local_date, INTERVAL row_number MONTH) as local_date,
      CONCAT('{',cameo_month,'}') as cameo_scores
FROM
    `{{ params['storage-prod'] }}.{{ params['postgres_dataset'] }}.{{ params['cameo_table'] }}`
CROSS JOIN UNNEST(SPLIT(REPLACE(REPLACE(cameo_scores,'[{',''),'}]',''),'},{')) as cameo_month
  WITH OFFSET as row_number