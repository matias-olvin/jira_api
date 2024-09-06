CALL `{{ params['home_send_procedure'] }}`(
    "{{ params['project'] }}.{{ params['postgres_dataset'] }}.{{ database_table }}",
    "{{ params['project'] }}",
    "{{ params['PlaceHome_dataset'] }}",
    "{{ params['ZipCode_table'] }}",
    "fk_zipcodes",
    "{{ params['Place_dataset'] }}",
    "{{ params['Activity_table'] }}",
    "{{ phzcs_param_1(execution_date) }}",
    "{{ params['project'] }}.{{ params['regressors_dataset'] }}.{{ params['covid_dictionary_poi_table'] }}",
    "{{ params['project'] }}.{{ params['regressors_dataset'] }}.{{ params['covid_table'] }}"
)