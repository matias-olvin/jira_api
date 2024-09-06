SELECT run_completion
FROM `{{ var.value.env_project }}.{{ params['accessible-by-sns-dataset'] }}.{{ params['daily-feed-runs-table'] }}`
WHERE local_date = DATE("{{ ti.xcom_pull(task_ids='local-date') }}")