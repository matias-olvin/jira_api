CALL `{{ params['home_zipcode_procedure'] }}`(
    "{{ params['project'] }}",
    "{{ params['PlaceHome_dataset'] }}",
    "{{ params['ZipCode_table'] }}",
    "{{ params['poi_visits_scaled_dataset'] }}",
    "{{ phzc_param_1(execution_date) }}",
    "{{ phzc_param_2(execution_date) }}",
    "{{ params['device_zipcodes_dataset'] }}",
    "{{ params['static_demographics_data_dataset'] }}",
    "{{ params['zipcode_demographics_table'] }}",
    "{{ params['area_geometries_dataset'] }}",
    "{{ params['zipcodes_table'] }}",
    "{{ params['places_dataset'] }}",
    "{{ params['places_table'] }}"
)