CREATE
OR REPLACE TABLE `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['adjustments_closing_days_logic'] }}` 
AS
WITH get_count AS (
    SELECT
        local_date,
        t2.tier_id,
        total_pois,
        COUNT(visits) count_visits,
    FROM
        (
            SELECT
                *,
                1 AS visits
            FROM
                -- NEED TO GET THIS TABLE
                `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['adjustments_days_with_zero_visits'] }}` 
                -- visits_estimation.adjustments_days_with_zero_visits
                --`sns-vendor-olvin-poc.accessible_by_olvin_dev.closing_days`
                
            WHERE
                local_date <= '2020-03-01'
                OR local_date >= '2020-10-16'
        ) t1
        INNER JOIN (
            select
                *,
                COUNT(DISTINCT fk_sgplaces) OVER (PARTITION BY tier_id) total_pois
            from
                (
                    select
                        *
                    from
                    `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['class_pois_sns_table'] }}` 
                        -- `storage-dev-olvin-com.visits_estimation.class_pois_sns`
                    wHERE fk_sgplaces in (select fk_sgplaces from 
                     `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['list_sns_pois_table'] }}`
                    -- `storage-dev-olvin-com.visits_estimation.list_sns_pois`
                    )
  
                )
        ) t2 USING (fk_sgplaces)

    GROUP BY
        1,
        2,
        3
),
stats AS (
    SELECT
        tier_id,
        AVG(count_visits) mean,
        STDDEV(count_visits) stddev
    FROM
        get_count
    GROUP BY
        1
),
labeled_data AS (
    SELECT
        t1.local_date,
        t1.tier_id,
        t1.count_visits,
        t1.total_pois,
        t2.mean,
        t2.stddev,
        ABS(count_visits - mean) AS distance,
        CASE
            WHEN count_visits > mean + 3 * stddev THEN 1
            ELSE 0
        END AS outlier_label,
        CASE
            WHEN count_visits / total_pois > 0.8 THEN 1
            ELSE 0
        END AS outlier_label_percent
    FROM
        get_count t1
        INNER JOIN stats t2 
        ON t1.tier_id = t2.tier_id
),
 holidays as (
    SELECT
        *,
        count(*) OVER (partition by identifier) count_identifier_type
    from
        `{{ var.value.env_project }}.{{ params['events_dataset'] }}.{{ params['holidays'] }}` 
        -- `storage-dev-olvin-com.events.holidays`
),
closing_days_output as (
    SELECT
        local_date,
        tier_id
    FROM
        labeled_data
    where
        outlier_label_percent = 1
),
join_holidays_to_closing_days as (
    select
        *
    from
        holidays
        inner join closing_days_output using (local_date)
),
find_maximum_date_per_holiday as (
    select
        tier_id,
        identifier,
        count_identifier_type,
        count(*) count_
    from
        join_holidays_to_closing_days
    group by
        tier_id,
        identifier,
        count_identifier_type
),
filter_if_greater_than_two as (
    select
        *
    from
        find_maximum_date_per_holiday
    where
        count_ > 2
),
join_back_to_holidays_table as (
    select
        *
    from
        filter_if_greater_than_two
        inner join holidays using (identifier)
)
select
    identifier,
    tier_id,
    local_date,
    True as closing_factor
from
    join_back_to_holidays_table
