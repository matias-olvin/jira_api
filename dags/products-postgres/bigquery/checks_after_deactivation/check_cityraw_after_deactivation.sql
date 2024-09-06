
WITH

different_CityRaw AS (

    SELECT
    pf.city,
    pf.state_abbreviation
    FROM
        `{{ params['project'] }}.{{ params['postgres_dataset'] }}.{{  params['cityraw_table'] }}` p
--        `storage-prod-olvin-com.postgres.CityRaw` p
    INNER JOIN
        `{{ params['project'] }}.{{ params['postgres_batch_dataset'] }}.{{  params['cityraw_table'] }}` pf
--        `storage-prod-olvin-com.postgres_batch.CityRaw` pf
    USING(city_id)
    WHERE  -- select different between postgres and postgres final
        (   p.active_places_count != pf.active_places_count
        OR p.monthly_visits_availability!=pf.monthly_visits_availability
        OR p.patterns_availability!=pf.patterns_availability)
),

-- select city and state (region) of blacklisted POIs
city_of_blacklist AS (

    SELECT
    city,
    region
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
        dcr.city,
        dcr.state_abbreviation
    FROM different_CityRaw dcr
    LEFT JOIN city_of_blacklist cob
    ON dcr.city = cob.city AND dcr.state_abbreviation = cob.region
    WHERE cob.city IS NULL  -- This filters out rows that didn't find a match in city_of_blacklist
)

-- Now, use CASE to raise an error if there are discrepancies
SELECT COUNT(*)
FROM different_not_in_blacklist