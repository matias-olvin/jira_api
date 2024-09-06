CREATE OR REPLACE TABLE `{{ var.value.env_project }}.{{ params['smc_daily_estimation_dataset'] }}.{{ params['poi_list_table'] }}`
AS
select
    distinct fk_sgplaces
from
    `{{ params['sns_project'] }}.{{ params['accessible_by_olvin_dataset'] }}.{{ params['smc_poi_matching_dataset']}}_{{ params['prod_matching_extended_table'] }}`
WHERE
    date_range_hourly = 'Highest' and
    hours_per_day_metric = 'Highest' and
    accuracy IN ("Highest", "High") and
    dates_density in ('High', 'Highest') and
    (consistency_batch_daily_feed = 'Highest' OR consistency_batch_daily_feed IS NULL)