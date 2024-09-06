declare curr_month date;
set curr_month = CAST(
  CONCAT(
    CAST(CURRENT_DATE() AS STRING format('YYYY')),
    '-',
    CAST(CURRENT_DATE() AS string format('MM')),
    '-',
    '01'
  ) AS DATE
);

CREATE OR REPLACE TABLE 
  `{{ params['project'] }}.{{ params['staging_data_dataset'] }}.{{ params['ideal_dynamic_places_table'] }}`
AS   
    SELECT
      olvin_id as pid,
      fk_sgbrands,
      name,
      brands,
      top_category,
      sub_category,
      naics_code,
      site_id,
      latitude,
      longitude,
      street_address,
      city,
      region,
      postal_code,
      open_hours,
      category_tags,
      polygon_wkt,
      polygon_class,
      enclosed,
      iso_country_code,
      region_id,
      long_lat_point,
      simplified_polygon_wkt,
      simplified_wkt_10_buffer,
      fk_parents_olvin as fk_parents,
      standalone_bool,
      child_bool,
      parent_bool,
      polygon_area_sq_ft,
      industry,
      timezone,
      phone_number,
      is_synthetic,
      includes_parking_lot,
      opened_on as opening_date,  -- Logic for opening/closing dates
      case 
        when (closed_on is null and last_seen < curr_month)
          then cast(last_seen as string)
        else closed_on
      end as closing_date
    FROM
        `{{ params['project'] }}.{{ params['staging_data_dataset'] }}.{{ params['all_places_table'] }}`
    WHERE
      sg_id = sg_id_active and
      active IS TRUE