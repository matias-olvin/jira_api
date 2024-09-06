DELETE `{{ var.value.env_project }}.{{ params['smc_metrics_dataset'] }}.{{ params['grid_volume_corr_table'] }}`
WHERE
    run_date = '{{ ds }}'
AND
    block='daily_estimation';

INSERT INTO `{{ var.value.env_project }}.{{ params['smc_metrics_dataset'] }}.{{ params['grid_volume_corr_table'] }}`
WITH
  -- POPULATION BY GRID
    grids_array_table_population as (
        SELECT
        pid,
        simplified_10_polygon,
        population,
        S2_COVERINGCELLIDS( simplified_10_polygon,
        min_level => CAST("9" as INT64),
        max_level => CAST("9" as INT64),
        max_cells => 400 ) AS grids_array
    FROM
        `{{ var.value.env_project }}.{{ params['area_geometries_dataset'] }}.{{ params['zipcodes_table'] }}`
        -- `storage-prod-olvin-com.area_geometries.zipcodes`

    ),
    unnesting_array_population as (
        SELECT
            *,
        FROM
            grids_array_table_population,
        UNNEST(grids_array) grid

    ),
    formatting_to_hex_population as (
        SELECT
            population,
            grid,
            `{{ var.value.env_project }}.{{ params['functions_dataset'] }}`.s2_id2token(grid) AS s2_token,
        FROM
            unnesting_array_population
    ),
  grid_info_population AS (
    select s2_token, sum(population) as population, log(nullif(sum(population), 0)) as log_population,
        bqcartoeu.s2.ST_BOUNDARY(grid) as polygon,
    from formatting_to_hex_population
    group by s2_token, grid

   ),
   
   joining_visits as (
        select *
        from (select s2_token, visits, local_month, block, visit_score_step, run_date
        from `{{ var.value.env_project }}.{{ params['smc_metrics_dataset'] }}.{{ params['grid_volume_vis_table'] }}`
        -- `storage-prod-olvin-com.smc_metrics.grid_volume_vis` 
        where run_date=DATE('{{ ds }}') and block = 'daily_estimation'
        )
        left join (select s2_token, population from grid_info_population ) using (s2_token)
   )
select distinct local_month, block, visit_score_step, corr(population, visits) as corr_population_visits,run_date

from joining_visits
group by local_month, block, visit_score_step,run_date;


