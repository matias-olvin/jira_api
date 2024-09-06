
WITH

different_SGCenterRaw AS (

    SELECT
    pf.pid as fk_sgcenters
    FROM
        `{{ params['project'] }}.{{ params['postgres_dataset'] }}.{{  params['SGCenterRaw_table'] }}` p
--        `storage-prod-olvin-com.postgres.CityRaw` p
    INNER JOIN
        `{{ params['project'] }}.{{ params['postgres_batch_dataset'] }}.{{  params['SGCenterRaw_table'] }}` pf
--        `storage-prod-olvin-com.postgres_batch.CityRaw` pf
    USING(pid)
    WHERE  -- select different between postgres and postgres final
        (   p.active_places_count != pf.active_places_count
        OR p.monthly_visits_availability!=pf.monthly_visits_availability
        OR p.patterns_availability!=pf.patterns_availability)
),

-- select city and state (region) of blacklisted POIs
center_of_blacklist AS (

    SELECT
    fk_sgcenters
    FROM
        `{{ params['project'] }}.{{ params['postgres_batch_dataset'] }}.{{  params['sgplaceraw_table'] }}`
--        `storage-prod-olvin-com.postgres_batch.SGPlaceRaw`
    RIGHT JOIN
        `{{ params['project'] }}.{{ params['sg_places_dataset'] }}.{{ params['wrong_coords_blacklist_table'] }}`
--        `storage-prod-olvin-com.sg_places.wrong_coords_blacklist`
    USING(pid)

),


different_not_in_blacklist AS (
    SELECT
        dcr.fk_sgcenters AS fk_parents_different_postgres_vs_postgres_batch,
        cob.fk_sgcenters AS fk_parents_of_blacklist_child
    FROM different_SGCenterRaw dcr
    LEFT JOIN center_of_blacklist cob
    USING (fk_sgcenters)
    WHERE cob.fk_sgcenters IS NULL  -- This filters out rows that didn't find a match in city_of_blacklist
),
-- account for potential discrepancies caused by different malls being removed in visits to mall pipeline postgres vs postgres_batch
p_vs_pf_malls_to_remove_blacklist AS (
    SELECT DISTINCT fk_sgcenters
    FROM 
      `{{ var.value.env_project }}.{{ params['visits_to_malls_dataset'] }}.{{ params['visits_to_malls_malls_to_remove_table'] }}_postgres_batch` pf
    LEFT JOIN
      `{{ var.value.env_project }}.{{ params['visits_to_malls_dataset'] }}.{{ params['visits_to_malls_malls_to_remove_table'] }}_postgres` p USING(fk_sgcenters)
    WHERE p.fk_sgcenters IS NULL

    UNION ALL

    SELECT DISTINCT fk_sgcenters
    FROM
      `{{ var.value.env_project }}.{{ params['visits_to_malls_dataset'] }}.{{ params['visits_to_malls_malls_to_remove_table'] }}_postgres` p
    LEFT JOIN
      `{{ var.value.env_project }}.{{ params['visits_to_malls_dataset'] }}.{{ params['visits_to_malls_malls_to_remove_table'] }}_postgres_batch` pf USING(fk_sgcenters)
    WHERE pf.fk_sgcenters IS NULL
)

-- Now, use CASE to raise an error if there are discrepancies
SELECT COUNT(*)
FROM different_not_in_blacklist tb1
LEFT JOIN p_vs_pf_malls_to_remove_blacklist tb2 ON tb1.fk_parents_different_postgres_vs_postgres_batch=tb2.fk_sgcenters
WHERE tb2.fk_sgcenters IS NULL