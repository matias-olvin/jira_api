DECLARE start_covid DATE DEFAULT '2020-03-01';

DECLARE end_covid DATE DEFAULT '2020-09-30';

CREATE
OR REPLACE TABLE `{{ var.value.sns_project }}.{{ params['olvin_smc_gtvm_dataset'] }}.{{ params['olvin_gtvm_target_table'] }}` AS
SELECT
    fk_sgplaces,
    total_visits / (
        GREATEST (
            0,
            (
                DATE_DIFF (max_date, GREATEST (min_date, end_covid), DAY) + 1
            )
        ) + GREATEST (
            0,
            (
                DATE_DIFF (LEAST (max_date, start_covid), min_date, DAY) + 1
            )
        )
    ) as visits_per_day,
FROM
    (
        SELECT
            fk_sgplaces,
            max(local_date) as max_date,
            min(local_date) as min_date,
            SUM(visits) as total_visits,
        FROM
            `{{ var.value.sns_project }}.{{ params['olvin_staging_dataset'] }}.{{ params['olvin_raw_traffic_table'] }}`
        WHERE
            fk_sgplaces IN (
                SELECT DISTINCT
                    fk_sgplaces
                FROM
                    `{{ var.value.sns_project }}.{{ params['olvin_poi_matching_dataset'] }}.{{ params['olvin_poi_matching_table'] }}`
                WHERE
                    accuracy IN ("Highest", "High")
                    AND (consistency_batch_daily_feed = "Highest" OR consistency_batch_daily_feed IS NULL)
                    AND (
                        date_range_hourly IS NOT NULL
                        OR (
                            date_range_hourly_group IS NOT NULL
                            AND date_density_group IN ("Highest", "High")
                        )
                    )
            )
            AND (
                local_date > '2020-09-30'
                OR local_date < '2020-03-01'
            )
        group by
            fk_sgplaces
            
        HAVING(SUM(visits)>0)
    ); 