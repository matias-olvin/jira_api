delete from
  `{{ var.value.env_project }}.{{ params['smc_postgres_dataset'] }}.{{ params['sgbrandraw_table'] }}` 
where pid in (
  select fk_sgbrands
  from 
    `{{ var.value.env_project }}.{{ params['sg_places_dataset'] }}.{{ params['remove_brand_list_table'] }}`
)
