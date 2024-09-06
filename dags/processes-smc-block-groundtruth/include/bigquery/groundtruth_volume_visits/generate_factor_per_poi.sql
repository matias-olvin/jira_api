CREATE OR REPLACE TABLE `{{ var.value.env_project }}.{{ params['smc_ground_truth_volume_dataset'] }}.{{ params['ground_truth_model_factor_per_poi_table'] }}`
AS
select fk_sgplaces, gtvm_factor
from
  (select *,
    count(*) over () as count_fk_sgplaces,
    count(distinct fk_sgplaces) over () as count_distinct_fk_sgplaces
  from

    (
    SELECT
      *, ifnull(output.visits/nullif(input.visits,0), 1) gtvm_factor,

    FROM
      -- `storage-prod-olvin-com.smc_ground_truth_volume_model.model_input_visits`
    `{{ var.value.env_project }}.{{ params['smc_ground_truth_volume_dataset'] }}.{{ params['model_input_visits_table'] }}`
      input
    left join
    -- `storage-prod-olvin-com.smc_ground_truth_volume_model.gtvm_output`
    `{{ var.value.env_project }}.{{ params['smc_ground_truth_volume_dataset'] }}.{{ params['gtvm_output_table'] }}`

    output

    using(fk_sgplaces)
    )
  )

WHERE IF(
count_fk_sgplaces = count_distinct_fk_sgplaces,
TRUE,
ERROR(FORMAT("count_distinct_fk_sgplaces  %d and count_fk_sgplaces %d are not the same.", count_fk_sgplaces, count_distinct_fk_sgplaces))
)