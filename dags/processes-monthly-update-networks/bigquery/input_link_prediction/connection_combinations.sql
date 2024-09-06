-- listing for each device all the places visited with the corresponding visit_score (concatenated with a space)
WITH visits_data AS (
    SELECT 
      device_id, 
      ARRAY_AGG(CONCAT(fk_nodes, ' ', ROUND(visit_score, 5))) AS places
    FROM
      --`storage-dev-olvin-com.sg_networks_staging.full_visits`
      `{{ params['project'] }}.{{ params['networks_staging_dataset'] }}.{{ params['full_visits_table'] }}`
    WHERE
      use_in_connections = True
    GROUP BY
      device_id
  ), 
  
-- All combinations -- a,b and also b,a  
  create_combinations AS (
    SELECT 
        device_id,
        ARRAY(SELECT c FROM UNNEST([place_1, place_2]) AS c ORDER BY c) combinations
    FROM 
        visits_data, visits_data.places AS place_1 , visits_data.places as place_2
    WHERE
        place_1 != place_2
  ), 

  --  now we have for a,b and b,a only one
  filter_combinations AS (
    SELECT 
        device_id,
        array_to_string(combinations, ":") comb_id
    FROM
        create_combinations
    GROUP BY 1, 2
  ),
  
  -- source and destination
  expand_combinations AS (
    SELECT
        device_id,
        SPLIT(comb_id, ':')[OFFSET(0)] AS src_string,
        SPLIT(comb_id, ':')[OFFSET(1)] AS dst_string
    FROM
        filter_combinations
  ),
  
  reorder_nodes AS (
    SELECT
        device_id,
        CASE 
          WHEN src_string < dst_string THEN src_string
          ELSE dst_string
        END AS src,
        CASE 
          WHEN src_string < dst_string THEN dst_string
          ELSE src_string
        END AS dst,
    FROM
        expand_combinations
  ),

-- final table with one entry per pair a,b
connections_combinations_table AS 
    (
        SELECT
            device_id,
            src_node,
            src_score,
            dst_node,
            dst_score,
            src_score*dst_score AS weight,
            DATE("{{ execution_date.add(months=1).format('%Y-%m-01') }}") AS local_date
        FROM (
            SELECT
            device_id,
            SPLIT(src, ' ')[OFFSET(0)] AS src_node,
            CAST(SPLIT(src, ' ')[OFFSET(1)] AS FLOAT64) AS src_score,
            SPLIT(dst, ' ')[OFFSET(0)] AS dst_node,
            CAST(SPLIT(dst, ' ')[OFFSET(1)] AS FLOAT64) AS dst_score,
            FROM 
            reorder_nodes)
    )
SELECT *
FROM connections_combinations_table

      
