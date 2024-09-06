LOAD DATA OVERWRITE `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['yearly_seasonality_adjustment_brand_ratios_table'] }}`
FROM
    FILES (
        FORMAT = 'PARQUET',
        uris = [
            "gs://{{ params['visits_estimation_bucket'] }}/ground_truth/supervised/yearly-seasonality/run_date={{ ds }}/output/*.parquet"
        ]
    );