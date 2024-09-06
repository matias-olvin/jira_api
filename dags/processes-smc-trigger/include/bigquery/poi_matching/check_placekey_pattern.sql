DECLARE
  count_condition_1,
  count_condition_2,
  count_places int64;
SET
  count_condition_1 = (
  SELECT
    COUNT(sg_id)
  FROM (
    SELECT
      sg_id,
      LENGTH(SUBSTR(sg_id, 1, STRPOS(sg_id, '@') - 1)) - LENGTH(REPLACE(SUBSTR(sg_id, 1, STRPOS(sg_id, '@') - 1), '-', '')) AS dash_count_before_at,
    FROM
      `{{ var.value.env_project }}.{{ params['smc_sg_places_dataset'] }}.{{ params['places_dynamic_table'] }}`
    WHERE CONTAINS_SUBSTR(sg_id, '@')
    )
  WHERE
    dash_count_before_at=1
    );
SET
  count_condition_2 = (
  SELECT
    COUNT(sg_id)
  FROM (
    SELECT
      sg_id,
      STRPOS(sg_id, '@') - 1 AS char_count_before_at
    FROM
      `{{ var.value.env_project }}.{{ params['smc_sg_places_dataset'] }}.{{ params['places_dynamic_table'] }}`
    WHERE CONTAINS_SUBSTR(sg_id, '@')
    )
  WHERE
    char_count_before_at=7
    OR
    char_count_before_at=8
    OR
    char_count_before_at=10
  );
SET
  count_places = (
  SELECT
    COUNT(sg_id)
  FROM
    `{{ var.value.env_project }}.{{ params['smc_sg_places_dataset'] }}.{{ params['places_dynamic_table'] }}`
  WHERE sg_id <> 'manually_added'
  );
ASSERT
  count_condition_1=count_places;
-- The second condition might fail as there are ~650 places with 8 characters before @
ASSERT
  count_condition_2=count_places;