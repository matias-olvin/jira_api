WITH
    hits_table AS (
      SELECT
        device_id,
        `{{ params['project'] }}.{{ params['functions_dataset'] }}`.point_level2token(long_lat_point, 8) AS s2_token,
        -- TO_HEX(CAST(( -- We want the final result in hexadecimal
        --     SELECT
        --       STRING_AGG(
        --         CAST(S2_CELLIDFROMPOINT(long_lat_point, 8) >> bit & 0x1 AS STRING), '' ORDER BY bit DESC) -- S2_CELLIDFROMPOINT returns an integer, convert to binary. 14 is the level of the cell
        --         FROM UNNEST(GENERATE_ARRAY(0, 63)) AS bit -- The standard is 64-bit long binary encoding
        --     ) AS BYTES FORMAT "BASE2" -- Tell BQ it is a binary string, BYTES format is required to use TO_HEX
        --   )
        -- ) AS s2_token,
        IFNULL(publisher_id, 0) AS publisher_id,
        CASE
        WHEN local_hour < 6 OR local_hour > 22 THEN "night"
        WHEN local_hour > 6
        AND local_hour < 22 THEN "day"
        ELSE
        NULL
    END
        AS part_of_day
      FROM
       `{{ params['project']  }}.{{ params['device_geohits_dataset'] }}.*` 
        -- `storage-prod-olvin-com.device_geohits.*`
    WHERE
        local_date = "{{ execution_date.subtract(days=var.value.latency_days_visits|int).strftime('%Y-%m-%d') }}"
        AND long_lat_point IS NOT NULL
        AND local_date IS NOT NULL
    ),
    inner_agg_s2_part AS (
        SELECT
        s2_token,
        ANY_VALUE(publisher_id) AS publisher_id,
        part_of_day,
        COUNT(*) AS device_geohits
        FROM
        hits_table
        GROUP BY
        s2_token,
        part_of_day,
        device_id
    ),
    inner_agg_s2 AS (
        SELECT
        s2_token,
        ANY_VALUE(publisher_id) AS publisher_id,
        COUNT(*) AS device_geohits
        FROM
        hits_table
        GROUP BY
        s2_token,
        device_id
    ),
    inner_agg_part AS (
        SELECT
        part_of_day,
        ANY_VALUE(publisher_id) AS publisher_id,
        COUNT(*) AS device_geohits
        FROM
        hits_table
        GROUP BY
        part_of_day,
        device_id
    ),
    inner_agg_none AS (
        SELECT
        ANY_VALUE(publisher_id) AS publisher_id,
        device_id,
        COUNT(*) AS device_geohits
        FROM
        hits_table
        GROUP BY
        device_id
    ),
    hits_table_all AS (
    SELECT
        s2_token,
        publisher_id,
        part_of_day,
        COUNT(*) AS d_devices_geohits,
        SUM(device_geohits) AS n_geohits,
        APPROX_QUANTILES(device_geohits, 100) AS device_geohits_quantiles
    FROM inner_agg_s2_part
    WHERE
        part_of_day IS NOT NULL
    GROUP BY
        s2_token,
        publisher_id,
        part_of_day
    UNION ALL
    SELECT
        NULL AS s2_token,
        publisher_id,
        part_of_day,
        COUNT(*) AS d_devices_geohits,
        SUM(device_geohits) AS n_geohits,
        APPROX_QUANTILES(device_geohits, 100) AS device_geohits_quantiles
    FROM inner_agg_part
    WHERE
        part_of_day IS NOT NULL
    GROUP BY
        publisher_id,
        part_of_day
    UNION ALL
    SELECT
        s2_token,
        NULL AS publisher_id,
        part_of_day,
        COUNT(*) AS d_devices_geohits,
        SUM(device_geohits) AS n_geohits,
        APPROX_QUANTILES(device_geohits, 100) AS device_geohits_quantiles
    FROM inner_agg_s2_part
    WHERE
        part_of_day IS NOT NULL
    GROUP BY
        s2_token,
        part_of_day
    UNION ALL
    SELECT
        NULL AS s2_token,
        NULL AS publisher_id,
        part_of_day,
        COUNT(*) AS d_devices_geohits,
        SUM(device_geohits) AS n_geohits,
        APPROX_QUANTILES(device_geohits, 100) AS device_geohits_quantiles
    FROM inner_agg_part
    WHERE
        part_of_day IS NOT NULL
    GROUP BY
        part_of_day
    UNION ALL
    SELECT
        s2_token,
        publisher_id,
        NULL AS part_of_day,
        COUNT(*) AS d_devices_geohits,
        SUM(device_geohits) AS n_geohits,
        APPROX_QUANTILES(device_geohits, 100) AS device_geohits_quantiles
    FROM inner_agg_s2
    GROUP BY
        s2_token,
        publisher_id
    UNION ALL
    SELECT
        NULL AS s2_token,
        publisher_id,
        NULL AS part_of_day,
        COUNT(*) AS d_devices_geohits,
        SUM(device_geohits) AS n_geohits,
        APPROX_QUANTILES(device_geohits, 100) AS device_geohits_quantiles
    FROM inner_agg_none
    GROUP BY
        publisher_id
    UNION ALL
    SELECT
        s2_token,
        NULL AS publisher_id,
        NULL AS part_of_day,
        COUNT(*) AS d_devices_geohits,
        SUM(device_geohits) AS n_geohits,
        APPROX_QUANTILES(device_geohits, 100) AS device_geohits_quantiles
    FROM inner_agg_s2
    GROUP BY
        s2_token
    UNION ALL
    SELECT
        NULL AS s2_token,
        NULL AS publisher_id,
        NULL AS part_of_day,
        COUNT(*) AS d_devices_geohits,
        SUM(device_geohits) AS n_geohits,
        APPROX_QUANTILES(device_geohits, 100) AS device_geohits_quantiles
    FROM inner_agg_none),
    final_table AS (
    SELECT
        *
    FROM
        hits_table_all
    )
    SELECT
    DATE("{{ execution_date.subtract(days=var.value.latency_days_visits|int).strftime('%Y-%m-%d') }}" ) AS local_date,
    * EXCEPT(device_geohits_quantiles),
    device_geohits_quantiles[OFFSET(25)] AS device_geohits_25,
    device_geohits_quantiles[OFFSET(50)] AS device_geohits_50,
    device_geohits_quantiles[OFFSET(75)] AS device_geohits_75,
    device_geohits_quantiles[OFFSET(90)] AS device_geohits_90,
    device_geohits_quantiles[OFFSET(98)] AS device_geohits_98,
    FROM
    final_table