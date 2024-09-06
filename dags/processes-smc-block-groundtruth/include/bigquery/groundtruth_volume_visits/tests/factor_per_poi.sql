DECLARE the_date DATE DEFAULT CURRENT_DATE();
DECLARE the_warning BOOL  DEFAULT(FALSE);
DECLARE the_terminate BOOL  DEFAULT(FALSE);

DECLARE the_count DEFAULT(
    SELECT
        count(*)
    FROM
        -- `storage-prod-olvin-com.ground_truth_volume_dataset.factor_per_poi`
        `{{ var.value.env_project }}.{{ params['smc_ground_truth_volume_dataset'] }}.{{ params['ground_truth_model_factor_per_poi_table'] }}`
);

DECLARE the_distinct_count DEFAULT(
    SELECT
        count(distinct fk_sgplaces)
    FROM
        -- `storage-prod-olvin-com.ground_truth_volume_dataset.factor_per_poi`
        `{{ var.value.env_project }}.{{ params['smc_ground_truth_volume_dataset'] }}.{{ params['ground_truth_model_factor_per_poi_table'] }}`
);

DECLARE more_or_less_than_1000 DEFAULT(
    SELECT
        count(*)
    FROM
        -- `storage-prod-olvin-com.ground_truth_volume_dataset.factor_per_poi`
        `{{ var.value.env_project }}.{{ params['smc_ground_truth_volume_dataset'] }}.{{ params['ground_truth_model_factor_per_poi_table'] }}`
    where gtvm_factor > 1000 or gtvm_factor < 0.001
);

DECLARE more_or_less_than_100 DEFAULT(
    SELECT
        count(distinct fk_sgplaces)
    FROM
        -- `storage-prod-olvin-com.ground_truth_volume_dataset.factor_per_poi`
        `{{ var.value.env_project }}.{{ params['smc_ground_truth_volume_dataset'] }}.{{ params['ground_truth_model_factor_per_poi_table'] }}`
    where gtvm_factor > 100 or gtvm_factor < 0.01

);

DECLARE negative_value DEFAULT(
    SELECT
        count(distinct fk_sgplaces)
    FROM
        -- `storage-prod-olvin-com.ground_truth_volume_dataset.factor_per_poi`
        `{{ var.value.env_project }}.{{ params['smc_ground_truth_volume_dataset'] }}.{{ params['ground_truth_model_factor_per_poi_table'] }}`
    where  gtvm_factor < 0

);


if the_count <> the_distinct_count
    then 
        SET the_terminate = TRUE;
end if ;

if negative_value > 0
    then 
        SET the_terminate = TRUE;
end if ;

if more_or_less_than_1000 > 10
    then 
        SET the_terminate = TRUE;
end if ;

if more_or_less_than_1000 > 0
    then 
        SET the_warning = TRUE;
end if ;

if more_or_less_than_100 > 0
    then 
        SET the_warning = TRUE;
end if ;


INSERT INTO `{{ var.value.env_project }}.{{ params['smc_ground_truth_volume_dataset'] }}.{{ params['monitoring_factor_per_poi_table']}}` 
-- INSERT INTO `storage-prod-olvin-com.smc_ground_truth_volume_model.monitoring_factor_per_poi`
(run_date,
size,
distinct_places,
more_or_less_than_1000,
more_or_less_than_100,
negative_value,
terminate,
warning)

VALUES 
(the_date, the_count, the_distinct_count, more_or_less_than_1000, more_or_less_than_100, negative_value, the_terminate, the_warning)
