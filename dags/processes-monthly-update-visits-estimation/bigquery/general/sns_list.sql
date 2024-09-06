create or replace table
-- `olvin-sns-local.visits_estimation.list_sns_pois`
    `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['list_sns_pois_table'] }}`

as
  select
    distinct fk_sgplaces
  from
    -- `sns-vendor-olvin-poc.poi_matching.prod_matching_extended` a
    `{{ params['sns_project'] }}.{{ var.value.accessible_by_olvin }}.{{ params['v_prod_matching_extended_table'] }}`  a
  inner join 
    (
      select *
      from
        -- `sns-vendor-olvin-poc.poi_matching.raw_traffic_metadata` 
    `{{ params['sns_project'] }}.{{ var.value.accessible_by_olvin }}.{{ params['v_raw_traffic_metadata_table'] }}`  a
      where max_date >  DATE_SUB(CURRENT_DATE(), INTERVAL (CAST("{{params['sns_latency_days']}}" as int64) + 2)  DAY)
      )  b
    on  a.sensormatic_poi_id = b.site_id
  WHERE
    ( date_range_hourly IN ("Highest", "High")
        OR
            (
            date_range_hourly_group IN ("Highest", "High") AND date_density_group IN ("Highest", "High")
            )
        )
    and hours_per_day_metric = 'Highest'
    and accuracy IN ("Highest", "High")
    and dates_density in ('High', 'Highest')
    and (consistency_batch_daily_feed = 'Highest' OR consistency_batch_daily_feed IS NULL)