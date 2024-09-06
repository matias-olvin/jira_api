WITH  
  adding_poi_information as (
    select fk_sgplaces, local_date, cast(visits as FLOAT64) as visits_final, name, street_address, region
    from `{{ dag_run.conf['source_table'] }}`  
    left join
      (
        select 
          pid as fk_sgplaces, name, street_address,region
        from
          `{{ var.value.env_project }}.{{ params['places_dataset'] }}.{{ params['places_table'] }}`
        where pid in 
        (
          SELECT
              pid
            FROM
              `{{ var.value.env_project }}.{{ params['postgres_dataset'] }}.{{ params['sgplaceraw_table'] }}`
            WHERE
              fk_sgbrands IN (
              SELECT
                pid
              FROM
                `{{ var.value.env_project }}.{{ params['postgres_dataset'] }}.{{ params['sgbrandraw_table'] }}`
              WHERE
                name IN ("AT&T",
                  "Nike",
                  "Walmart",
                  "Starbucks",
                  "JCPenney") )
                  )
        --    `storage-prod-olvin-com.sg_places.20211101`
      ) using (fk_sgplaces)
  )

SELECT
  *,
  NULL AS visits_estimated_lower,
  NULL AS visits_observed,
  NULL AS visits_estimated_upper,
  NULL AS visits_estimated,
  "{{ dag_run.conf['step'] }}" as step,
  'all' as mode,
  DATE('{{ ds }}') as run_date
FROM
  adding_poi_information