-- add these columns back to ranked_duplicates for debugging
-- sg_id_first,
-- sg_id_active,
-- name,
-- street_address,
-- first_seen,
UPDATE `{{ var.value.env_project }}.{{ params['staging_data_dataset'] }}.{{ params['all_places_table'] }}`
SET active = FALSE
WHERE sg_id IN (
  WITH ranked_duplicates AS (
    SELECT
      olvin_id,
      sg_id,
      active,
      last_seen,
      ROW_NUMBER() OVER (PARTITION BY olvin_id ORDER BY last_seen DESC) as row_num
    FROM `{{ var.value.env_project }}.{{ params['staging_data_dataset'] }}.{{ params['all_places_table'] }}`
    WHERE olvin_id IN (
      SELECT olvin_id
      FROM `{{ var.value.env_project }}.{{ params['staging_data_dataset'] }}.{{ params['all_places_table'] }}`
      GROUP BY olvin_id
      HAVING SUM(CASE WHEN active THEN 1 ELSE 0 END) > 1
    )
  )
  SELECT sg_id
  FROM ranked_duplicates
  WHERE row_num != 1
);