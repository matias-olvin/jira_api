DROP materialized view if exists `{{ var.value.env_project }}.{{ params['validations_kpi_dataset'] }}.{{ params['weekly_correlations_mv'] }}`;

create materialized view `{{ var.value.env_project }}.{{ params['validations_kpi_dataset'] }}.{{ params['weekly_correlations_mv'] }}` OPTIONS(enable_refresh=False,allow_non_incremental_definition = true, max_staleness = INTERVAL "1" HOUR) as 
select
    distinct
      id as id_w,
      run_date as run_date_w,
      PERCENTILE_CONT(correlation[SAFE_OFFSET(0)].poi_correlation, 0.5) over (partition  by id, run_date) as median_correlation_weekly,
  from (
      select *
      from
        `{{ var.value.env_project }}.{{ params['postgres_metrics_dataset'] }}.{{ params['trend_metrics_table'] }}`
      where
        class_id = 'fk_sgbrands'
        and correlation[SAFE_OFFSET(0)].days = 182
        and granularity = 'week'
        and pipeline = 'postgres'
        and step = 'raw'
    )