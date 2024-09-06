DELETE FROM 
    `{{ params['project'] }}.{{ params['test_compilation_dataset'] }}.{{ params['test_results_table'] }}`
WHERE 
    test_name = 'raw to geohits' AND (
        run_date >= "{{ ds }}" AND
        run_date < DATE_ADD("{{ ds }}", INTERVAL 1 DAY)
    )