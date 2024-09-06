CREATE OR REPLACE TABLE
  `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{  params['sgplaceactivity_table'] }}`
AS

WITH
input AS (
    SELECT *
    FROM
      `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{  params['sgplaceactivity_table'] }}`
),

updated_activity_table AS(
    SELECT  fk_sgplaces,
            IF(remove, 'no_data', activity) AS activity,
            IF(remove, 1.0, confidence_level) AS confidence_level,
            IF(remove, FALSE, home_locations) AS home_locations,
            IF(remove, FALSE, connections) AS connections,
            IF(remove, FALSE, trade_area_activity) AS trade_area_activity,
            IF(remove, FALSE, cross_shopping_activity) AS cross_shopping_activity
    FROM
      `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{  params['sgplaceactivity_table'] }}`
    LEFT JOIN(
      SELECT pid AS fk_sgplaces, TRUE AS remove
      FROM
        `{{ var.value.env_project }}.{{ params['sg_places_dataset'] }}.{{ params['wrong_coords_blacklist_table'] }}`
    )
    USING(fk_sgplaces)
),

tests as (
  select *
  from updated_activity_table
  , (select count(*) as count_in from input)
  , (select count(*) as count_out from updated_activity_table)
)

select * except(count_in, count_out)
from tests
WHERE IF(
count_in = count_out,
TRUE,
ERROR(FORMAT("count_in  %d > count_out %d ", count_in, count_out))
)
