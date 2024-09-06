DECLARE date_str DATE;

DECLARE sql STRING;

SET
    date_str = DATE_ADD("{{ next_ds }}", INTERVAL 2 WEEK);

DROP SNAPSHOT TABLE IF EXISTS `{{ var.value.env_project }}.{{ params['public_feeds_staging_dataset'] }}.{{ params['store_visits_daily_snapshot_table'] }}`;

SET
    sql = CONCAT(
        "CREATE SNAPSHOT TABLE IF NOT EXISTS `{{ var.value.env_project }}.{{ params['public_feeds_staging_dataset'] }}.{{ params['store_visits_daily_snapshot_table'] }}` CLONE `{{ var.value.env_project }}.{{ params['public_feeds_dataset'] }}.{{ params['store_visits_daily_table'] }}` OPTIONS (expiration_timestamp = TIMESTAMP '",
        date_str,
        "')"
    );

EXECUTE IMMEDIATE sql;

DELETE FROM `{{ var.value.env_project }}.{{ params['public_feeds_dataset'] }}.{{ params['store_visits_daily_table'] }}`
WHERE
    local_date >= DATE_TRUNC('{{ ds }}', MONTH);

INSERT INTO
    `{{ var.value.env_project }}.{{ params['public_feeds_dataset'] }}.{{ params['store_visits_daily_table'] }}`
SELECT
    *
FROM
    `{{ var.value.env_project }}.{{ params['public_feeds_staging_dataset'] }}.{{ params['store_visits_daily_table'] }}_temp`
WHERE
    -- this line is not necessary but it is left as a precaution
    local_date >= DATE_TRUNC('{{ ds }}', MONTH);