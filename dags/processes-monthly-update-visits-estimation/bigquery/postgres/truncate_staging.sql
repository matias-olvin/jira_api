IF EXISTS(
  SELECT
    *
  FROM ((
      SELECT
        COUNT(1) AS cnt
      FROM
        `{{ params['project'] }}.{{ params['visits_estimation_output_dataset'] }}.__TABLES_SUMMARY__`
      WHERE
        table_id = "{{ params['inference_output_poi_table'] }}"))
  WHERE
    cnt>0)
    then
    TRUNCATE TABLE `{{ params['project'] }}.{{ params['visits_estimation_output_dataset'] }}.{{ params['inference_output_poi_table'] }}`;
    end if;

IF EXISTS(
  SELECT
    *
  FROM ((
      SELECT
        COUNT(1) AS cnt
      FROM
        `{{ params['project'] }}.{{ params['inference_input_poi_dataset'] }}.__TABLES_SUMMARY__`
      WHERE
        table_id = "{{ params['inference_input_poi_table'] }}"))
  WHERE
    cnt>0)
    then
    TRUNCATE TABLE `{{ params['project'] }}.{{ params['inference_input_poi_dataset'] }}.{{ params['inference_input_poi_table'] }}`;
    end if;