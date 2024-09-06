CREATE OR REPLACE TABLE
  `{{ var.value.env_project }}.{{ params['postgres_mv_dataset'] }}.{{ params['sgplaceraw_table'] }}`
AS

SELECT
      p.pid,
      p.name,
      p.fk_sgbrands,
      p.top_category,
      p.sub_category,
      IFNULL(b.tenant_type, 'unbranded') AS tenant_type,
      p.naics_code,
      p.latitude,
      p.longitude,
      p.street_address,
      p.city,
      p.region,
      p.postal_code,
      p.category_tags,
      IFNULL(ST_ASTEXT(pd.polygon_wkt), manual.polygon) AS polygon,
      p.phone_number,
      p.iso_country_code,
      0.0 AS building_height, -- dummy value
      p.enclosed,
      p.is_synthetic,
      p.includes_parking_lot,
      p.fk_parents,
      p.polygon_area_sq_ft,
      IFNULL(pd.fk_parents_override, manual.fk_sgcenters) AS fk_parents_override,
      p.opening_date,
      p.closing_date,
      p.opening_status,
      p.fk_sgcenters,
      a.cross_shopping_activity,
      a.trade_area_activity,
      a.home_locations AS home_locations_activity,
      IFNULL(ta.has_trade_area, FALSE) AS cbg_trade_area_activity,
    -- TODO: update the table SGPlaceTradeAreaRaw_table for the actual CBG trade area table
      CASE
        -- treat `watch_list` as `active`
        WHEN a.activity IN ('active', 'watch_list') THEN 'active'
        ELSE a.activity
      END AS activity

FROM
  `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['sgplaceraw_table'] }}` p
INNER JOIN
  `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['sgplaceactivity_table'] }}` a
  ON a.fk_sgplaces = p.pid
LEFT JOIN
 `{{ var.value.env_project }}.{{ params['sg_places_dataset'] }}.{{ params['places_dynamic_table'] }}` pd
  ON pd.pid = p.pid
LEFT JOIN
 `{{ var.value.env_project }}.{{ params['manually_add_pois_dataset'] }}.{{ params['input_info_table'] }}` manual
  ON manual.fk_sgplaces = p.pid
LEFT JOIN
  `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['sgbrandraw_table'] }}` b
  ON b.pid = p.fk_sgbrands
LEFT JOIN (
  SELECT DISTINCT fk_sgplaces, TRUE AS has_trade_area
  FROM `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['SGPlaceHomeCensusBlockGroupsYearly_table'] }}`
) ta
ON p.pid = ta.fk_sgplaces  
ORDER BY p.fk_sgbrands, p.region, p.city, p.postal_code