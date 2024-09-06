SELECT
    places.olvin_id AS fk_sgplaces,
    hourly.* EXCEPT (fk_sgplaces)
FROM
    `storage-prod-olvin-com.accessible-by-sns.SGPlaceHourlyVisitsRaw_bf` hourly
    JOIN `storage-prod-olvin-com.sg_places.places_history` places ON places.sg_id = hourly.fk_sgplaces;