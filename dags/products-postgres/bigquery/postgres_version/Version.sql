DECLARE prev_version,curr_version,sql_statement,_table STRING;
DECLARE prev_version_start_timestamp, prev_version_end_timestamp,curr_version_start_timestamp DATETIME;
DECLARE latest_actual_date DATE;

SET prev_version = "6.0.1";
SET curr_version = "6.0.2";
SET prev_version_start_timestamp = "2024-08-01T00:00:01";
SET prev_version_end_timestamp = "2024-09-01T00:00:00";
SET curr_version_start_timestamp = "2024-09-01T00:00:01";
SET latest_actual_date = (SELECT latest_ground_truth_date from `{{ var.value.env_project }}.{{ params['pipeline_tracking_dataset'] }}.{{ params['monthly_update_table'] }}` where execution_date=DATE("{{ ds }}") );

SET _table = "{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['Version_table'] }}";

SET sql_statement = (CONCAT('DELETE FROM `',_table,'` WHERE id="',curr_version,'"'));
EXECUTE IMMEDIATE (sql_statement);

-- DELETE FROM  `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['Version_table'] }}`
-- WHERE id="4.0.0"

SET sql_statement = (CONCAT('UPDATE `',_table,'` SET end_date=DATETIME("',prev_version_end_timestamp,'") WHERE id="',prev_version,'"'));
EXECUTE IMMEDIATE (sql_statement);

SET sql_statement = (CONCAT('UPDATE `',_table,'` SET start_date=DATETIME("',prev_version_start_timestamp,'") WHERE id="',prev_version,'"'));
EXECUTE IMMEDIATE (sql_statement);

-- UPDATE `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['Version_table'] }}` 
-- SET end_date = DATETIME("2024-02-02T00:00:00")
-- WHERE id ="4.0.0";

SET sql_statement = (CONCAT('INSERT INTO `',_table,'` SELECT "',curr_version,'" as id, DATETIME("',curr_version_start_timestamp,'") as start_date, DATETIME(null) as end_date, DATE("',latest_actual_date,'") as latest_actual_date'));
EXECUTE IMMEDIATE (sql_statement);

-- INSERT INTO
--   `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['Version_table'] }}` 
-- SELECT
--   "4.0.1" as id,
--   DATETIME("2024-02-02T00:00:01") as start_date,
--   DATETIME(NULL) as end_date;