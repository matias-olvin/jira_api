select
    completed
from 
    `{{ params['sns_project'] }}.{{ params['accessible_by_olvin_dataset'] }}.{{ params['pipeline_status_table'] }}`
where 
    pipeline = "{{ params.pipeline }}" and
    update_date = DATE("{{ ds }}")