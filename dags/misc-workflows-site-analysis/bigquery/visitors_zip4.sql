IF "{{ dag_run.conf['devices'] }}" = "visitors" THEN
    CREATE OR REPLACE TABLE
      `{{ var.value.storage_project_id }}.{{ dag_run.conf['site_name'] }}.{{ params['visitors_zip4_table'] }}`
    PARTITION BY local_date AS
    SELECT
        device_id,
        visit_score,
        zip_id,
        local_date,
    FROM
        (SELECT
            device_id,
            zip_id,
            local_date,
        FROM `storage-dev-olvin-com.{{ params['zipcodes_dataset'] }}.*`
        WHERE local_date >= "{{ dag_run.conf['start_date_string'] }}" AND local_date < "{{ dag_run.conf['end_date_string'] }}"
    )
    JOIN (SELECT device_id, local_date,
            visit_score,
        FROM `{{ var.value.storage_project_id }}.{{ dag_run.conf['site_name'] }}.{{ params['visitors_homes_table'] }}`)
    USING(device_id, local_date);
ELSEIF "{{ dag_run.conf['devices'] }}" = "workers" THEN
    CREATE OR REPLACE TABLE
    `{{ var.value.storage_project_id }}.{{ dag_run.conf['site_name'] }}.{{ params['visitors_zip4_table'] }}`
    PARTITION BY local_date AS

    WITH device_homes AS (
      SELECT
        local_date,
        device_id,
        home_point,
        fk_zipcodes,
        visit_score
    FROM
        (
          SELECT
            local_date,
            device_id,
            home_point,
            visit_score
          FROM
            `{{ var.value.storage_project_id }}.{{ dag_run.conf['site_name'] }}.{{ params['visitors_homes_table'] }}`
        )
        JOIN (
          SELECT
            pid AS fk_zipcodes,
            ST_GEOGFROMTEXT(polygon) AS polygon
          FROM
            `{{ var.value.storage_project_id }}.{{ params['area_geometries_dataset'] }}.{{ params['zipcodes_table'] }}`
        )
        ON ST_CONTAINS(polygon, home_point)
    ),
    zip_code_info AS (
      SELECT
        *
      EXCEPT(point),
        point AS zip_point
      FROM
        (
          SELECT
            zip_id,
            ANY_VALUE(point) AS point,
            ANY_VALUE(fk_zipcodes) AS fk_zipcodes
          FROM
            `storage-dev-olvin-com.static_demographics_data.zipcode_demographics`
          GROUP BY
            zip_id
        )
    ),
    grouped_met_areas AS (
      SELECT
        fk_zipcodes,
        ST_UNION_AGG(zip_point) AS zip_arr
      FROM
        zip_code_info
      GROUP BY
        fk_zipcodes
    ),
    join_zipcodes AS (
      SELECT
        device_homes.*
      EXCEPT(home_point),
        closest_zip
      FROM
        device_homes
        LEFT JOIN grouped_met_areas ON grouped_met_areas.fk_zipcodes = device_homes.fk_zipcodes,
        UNNEST([ST_CLOSESTPOINT(zip_arr,
            home_point)]) AS closest_zip
    )
    SELECT
        join_zipcodes.device_id,
        join_zipcodes.visit_score,
        zip_code_info.zip_id,
        join_zipcodes.local_date
    FROM
      join_zipcodes
      JOIN zip_code_info ON ST_EQUALS(
        join_zipcodes.closest_zip,
        zip_code_info.zip_point
      );
ELSE
    RAISE USING MESSAGE = "Value for dag_run.conf['devices'] must be 'visitors' or 'workers', not '{{ dag_run.conf['devices'] }}'.";
END IF;