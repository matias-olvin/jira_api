ALTER TABLE `{{ var.value.env_project }}.{{ params['accessible_by_sns_dataset'] }}.{{ params['postgres_batch_dataset'] }}-{{ params['ammend_table_name'] }}`
SET OPTIONS (
    labels = [
        (
            'dataset',
            '{{ params["postgres_batch_dataset"] }}'
        )
    ]
);