CREATE OR REPLACE TABLE 
    `{{ var.value.env_project }}.{{ params['smc_geoscaling_metrics_dataset'] }}.{{ params['validation_timeseries_dow_category_table'] }}`
COPY 
    `{{ params['sns_project'] }}.{{ params['accessible_by_olvin_dataset'] }}.{{ params['validation_timeseries_dow_category_table'] }}`
;

CREATE OR REPLACE TABLE 
    `{{ var.value.env_project }}.{{ params['smc_geoscaling_metrics_dataset'] }}.{{ params['validation_timeseries_dow_brand_table'] }}`
COPY 
    `{{ params['sns_project'] }}.{{ params['accessible_by_olvin_dataset'] }}.{{ params['validation_timeseries_dow_brand_table'] }}`
;

CREATE OR REPLACE TABLE 
    `{{ var.value.env_project }}.{{ params['smc_geoscaling_metrics_dataset'] }}.{{ params['validation_timeseries_trend_brand_table'] }}`
COPY 
    `{{ params['sns_project'] }}.{{ params['accessible_by_olvin_dataset'] }}.{{ params['validation_timeseries_trend_brand_table'] }}`
;

CREATE OR REPLACE TABLE 
    `{{ var.value.env_project }}.{{ params['smc_geoscaling_metrics_dataset'] }}.{{ params['validation_timeseries_trend_category_table'] }}`
COPY 
    `{{ params['sns_project'] }}.{{ params['accessible_by_olvin_dataset'] }}.{{ params['validation_timeseries_trend_category_table'] }}`
;

CREATE OR REPLACE TABLE 
    `{{ var.value.env_project }}.{{ params['smc_geoscaling_metrics_dataset'] }}.{{ params['validation_timeseries_dow_global_table'] }}`
COPY 
    `{{ params['sns_project'] }}.{{ params['accessible_by_olvin_dataset'] }}.{{ params['validation_timeseries_dow_global_table'] }}`
;

CREATE OR REPLACE TABLE 
    `{{ var.value.env_project }}.{{ params['smc_geoscaling_metrics_dataset'] }}.{{ params['validation_timeseries_trend_global_table'] }}`
COPY 
    `{{ params['sns_project'] }}.{{ params['accessible_by_olvin_dataset'] }}.{{ params['validation_timeseries_trend_global_table'] }}`
;
