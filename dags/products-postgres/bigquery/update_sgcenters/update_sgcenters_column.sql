-- Set monthly_visits_availability = TRUE for centers that appear in the SGMallsMonthlyVisitsRaw table
UPDATE `{{ var.value.env_project }}.{{ postgres_dataset }}.{{ params['SGCenterRaw_table'] }}` AS main
SET
  monthly_visits_availability = TRUE
WHERE
  pid IN (
    SELECT DISTINCT
      fk_sgcenters
    FROM
      `{{ var.value.env_project }}.{{ postgres_dataset }}.{{ params['visits_to_malls_monthly_table'] }}`
    WHERE
      local_date BETWEEN DATE_TRUNC(DATE_SUB("{{ ds }}", INTERVAL 2 MONTH), MONTH) AND DATE_TRUNC("{{ ds }}", MONTH)
  );

-- Set monthly_visits_availability = FALSE for centers that do not appear in the SGMallsMonthlyVisitsRaw table
UPDATE `{{ var.value.env_project }}.{{ postgres_dataset }}.{{ params['SGCenterRaw_table'] }}` AS main
SET
  monthly_visits_availability = FALSE
WHERE
  pid NOT IN (
    SELECT DISTINCT
      fk_sgcenters
    FROM
      `{{ var.value.env_project }}.{{ postgres_dataset }}.{{ params['visits_to_malls_monthly_table'] }}`
    WHERE
      local_date BETWEEN DATE_TRUNC(DATE_SUB("{{ ds }}", INTERVAL 2 MONTH), MONTH) AND DATE_TRUNC("{{ ds }}", MONTH)
  );