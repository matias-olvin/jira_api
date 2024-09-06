DECLARE sg_table_suffix STRING;
SET sg_table_suffix = CONCAT(
    CAST(CURRENT_DATE() AS STRING format('YYYY')),
    CAST(CURRENT_DATE() AS string format('MM')),
    '01'
);

EXECUTE IMMEDIATE format(
    """
    CREATE or REPLACE TABLE `{{ params['project'] }}.{{ params['staging_data_dataset'] }}.sg_places-%s` 
    partition by range_bucket(region_id, GENERATE_ARRAY(0,200,1)) 
    cluster by long_lat_point, sg_id, naics_code, fk_sgbrands
    AS
    WITH
      set_timezone AS (
        SELECT
          places.*,
          tt.tzid as timezone
        FROM
          `{{ params['project'] }}.{{ params['staging_data_dataset'] }}.sg_places-%s` places
        left join
          `{{ params['project'] }}.{{ params['geometry_dataset'] }}.{{ params['timezones_table'] }}`  tt
        on
          ST_WITHIN(places.long_lat_point, tt.geometry)
      ),
      remove_null_parents AS (
        SELECT
          a.* EXCEPT(fk_parents),
          CASE WHEN b.sg_id IS NULL
              THEN NULL
              ELSE a.fk_parents
              END AS fk_parents
        FROM
          set_timezone a
        left join
          set_timezone b
        on
          a.fk_parents = b.sg_id
      )

      SELECT
        *
      FROM
        remove_null_parents
    """, 
    sg_table_suffix, 
    sg_table_suffix
)