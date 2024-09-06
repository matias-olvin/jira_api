DECLARE current_execution_date DATE DEFAULT (
    SELECT
        DATE_ADD(MAX(execution_date), INTERVAL 1 MONTH)
    FROM
        `{{ var.value.env_project }}.{{ params['test_compilation_dataset'] }}.{{ params['monthly_update_table'] }}`
    WHERE
        completed
);
DECLARE today DATE DEFAULT CURRENT_DATE();
DECLARE prod_date_in_table DATE DEFAULT(
    SELECT
        push_to_prod_date
    FROM
        `{{ var.value.env_project }}.{{ params['test_compilation_dataset'] }}.{{ params['monthly_update_table'] }}`
    WHERE
        execution_date = current_execution_date
);
-- Just in case we don't have available data on time
DECLARE prod_available_date DATE DEFAULT(
  SELECT CASE WHEN prod_date_in_table < today
         THEN today
         ELSE prod_date_in_table
         END
);

UPDATE `{{ var.value.env_project }}.{{ params['test_compilation_dataset'] }}.{{ params['monthly_update_table'] }}`
SET
    completed = TRUE,
    push_to_prod_date = prod_available_date,
    end_running_date = today
WHERE
    execution_date = current_execution_date;