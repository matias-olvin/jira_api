DELETE
FROM
  `storage-prod-olvin-com.postgres_metrics.activity`
WHERE
  run_date = '{{ ds }}';

INSERT INTO
  `storage-prod-olvin-com.postgres_metrics.activity` ( run_date,
    places,
    zipcodes )
WITH
  active_places AS (
  SELECT
    SUM(CASE
        WHEN activity IN ('active', 'watch_list') THEN 1
      ELSE
      0
    END
      ) AS total
  FROM
    `storage-prod-olvin-com.postgres.SGPlaceActivity` ),
  active_zipcodes AS (
  SELECT
    SUM(CASE
        WHEN activity IN ('active', 'watch_list') THEN 1
      ELSE
      0
    END
      ) AS total
  FROM
    `storage-prod-olvin-com.postgres.ZipCodeActivity` )
SELECT
  DATE('{{ ds }}') AS run_date,
  active_places.total,
  active_zipcodes.total
FROM
  active_places,
  active_zipcodes
  
