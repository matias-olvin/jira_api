
WITH

different_ZipCodeRaw AS (

    SELECT
    pf.pid, -- this is the zip code
    FROM
        `{{ params['project'] }}.{{ params['postgres_dataset'] }}.{{  params['zipcoderaw_table'] }}` p
--        `storage-prod-olvin-com.postgres.CityRaw` p
    INNER JOIN
        `{{ params['project'] }}.{{ params['postgres_batch_dataset'] }}.{{  params['zipcoderaw_table'] }}` pf
--        `storage-prod-olvin-com.postgres_batch.CityRaw` pf
    USING(pid)
    WHERE  -- select different between postgres and postgres final
        (   p.active_places_count != pf.active_places_count
        OR p.monthly_visits_availability!=pf.monthly_visits_availability
        OR p.patterns_availability!=pf.patterns_availability)
),

-- select zipcode of blacklisted POIs
zipcode_of_blacklist AS (

    SELECT
    postal_code
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
        zob.postal_code AS zipcode_of_blacklist_poi,
        dzr.pid AS zipcode_different_postgres_vs_postgres_batch
    FROM different_ZipCodeRaw dzr
    LEFT JOIN zipcode_of_blacklist zob
    ON dzr.pid = CAST(zob.postal_code AS INT64)
    WHERE zob.postal_code IS NULL  -- This filters out rows that didn't find a match in city_of_blacklist
)

-- Now, use CASE to raise an error if there are discrepancies
SELECT COUNT(*)
FROM different_not_in_blacklist