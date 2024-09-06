DECLARE d DATE DEFAULT CAST('{{ params["date_start"] }}' as DATE);

WHILE d < '{{ params["date_end"] }}' DO
    IF(NOT EXISTS(
      SELECT local_date from `{{ var.value.env_project }}.{{ params['smc_metrics_dataset'] }}.{{ params['day_stats_visits_scaled_table'] }}`
      WHERE local_date = d)) THEN
    CALL `{{ params['smc_day_stats_visits_scaled_procedure'] }}` (
        "{{ var.value.env_project }}",
        "{{ params['smc_metrics_dataset'] }}",
        "{{ params['day_stats_visits_scaled_table'] }}",
        "{{ params['smc_poi_visits_scaled_dataset'] }}",
        d
    );
    END IF;
  SET d = DATE_ADD(d, INTERVAL 1 DAY);
END WHILE;