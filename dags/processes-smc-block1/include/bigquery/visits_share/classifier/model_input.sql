CREATE OR REPLACE TABLE `{{ var.value.env_project }}.{{ params['smc_visits_share_dataset'] }}.{{ params['model_input_complete_table'] }}` AS
with training_data_raw as (
    select * from
        `{{ var.value.env_project }}.{{ params['smc_visits_share_dataset'] }}.{{ params['training_data_raw_table'] }}`
        --`storage-prod-olvin-com.visits_share.us_training_data_raw`
),
neighbour_attribute as (
    select fk_sgplaces, neighbour_category_old, neighbour_category from
        `{{ var.value.env_project }}.{{ params['smc_visits_share_dataset'] }}.{{ params['filtering_places_table'] }}`
        --`storage-prod-olvin-com.visits_share.us_filtering_places`
        where neighbour_category is not null
),
filtering_columns as (
    select
        olvin_category_old,
        olvin_category,
        hour_week,
        fk_incomes,
        fk_life_stages,
        temperature,
        precip_intensity,
        christmas_factor,
        black_friday_factor,
        back_to_school_factor,
        neighbour_category_old,
        neighbour_category
    from
        training_data_raw
    inner join neighbour_attribute using (fk_sgplaces)
    where opening = 1
),
-- TO BE DONE: SAME ELEMENTS PER GROUP
imbalanced_sampling as 
    (
    select * except (count_category, total, unique_category_bool, random_number)
    from 
        (
        select
            *,
            row_number () over (partition by unique_category) as random_number
        from
            (select *,
                count(*) over(partition by olvin_category) count_category,
                count(*) over() total,
                IF(count(*) over(partition by olvin_category)/ count(*) over() < 0.01, false, true) as unique_category_bool,
                IF(count(*) over(partition by olvin_category)/ count(*) over() < 0.01, 'group', olvin_category) as unique_category,
            from filtering_columns)
        )
    where random_number < (total / 100)
    and unique_category is not null
    ),
adding_neighbourhood_unique_category as (
    select
        * except (neighbourhood_unique_category),
        IF(neighbourhood_unique_category is null, 'group', neighbourhood_unique_category) as neighbourhood_unique_category,
    from
        (select imbalanced_sampling.*, neighbourhood_unique_category
        from imbalanced_sampling
        left join (select distinct olvin_category as neighbour_category, unique_category as neighbourhood_unique_category from imbalanced_sampling )
        using (neighbour_category))
    
)

select *
from adding_neighbourhood_unique_category
WHERE 
olvin_category is not null
and
neighbour_category is not null
and
unique_category is not null
and
neighbourhood_unique_category is not null