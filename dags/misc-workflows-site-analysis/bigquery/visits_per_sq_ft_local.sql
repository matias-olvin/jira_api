CREATE OR REPLACE TABLE
`{{ var.value.storage_project_id }}.{{ dag_run.conf['site_name'] }}.{{ params['visits_per_sq_ft_local_table'] }}` as

WITH 
 place_ids as (
SELECT
fk_sgplaces
-- FROM (select ['222-222@5pj-5nd-wkz','222-222@5pj-5nd-w8v','224-222@5pj-5nd-syv','zzw-222@5pj-5nd-syv','223-222@5pj-5nd-syv','224-222@5pj-5nd-snq','223-222@5pj-5nd-snq','222-223@5pj-5nd-snq', 'zzw-223@5pj-5nd-sqz']
FROM ( SELECT {{ dag_run.conf['place_ids'] }}
as fk_sgplaces),
UNNEST(fk_sgplaces) fk_sgplaces

)

select * from `{{ var.value.storage_project_id }}.{{ dag_run.conf['site_name'] }}.{{ params['chain_ranking_local_table'] }}`
where fk_sgplaces in (select fk_sgplaces from place_ids)