DELETE FROM `storage-prod-olvin-com.postgres_metrics.trend`
WHERE
    env = 'prod'
    AND pipeline = 'postgres'
    AND step = 'raw'
    AND granularity = 'hour'
    AND run_date = DATE('{{ ds }}');

INSERT INTO
    `storage-prod-olvin-com.postgres_metrics.trend`
SELECT
    *
FROM
    `sns-vendor-olvin-poc.accessible_by_olvin.postgres_metrics-trend`
WHERE
    env = 'prod'
    AND pipeline = 'postgres'
    AND step = 'raw'
    AND granularity = 'hour'
    AND run_date = DATE('{{ ds }}');