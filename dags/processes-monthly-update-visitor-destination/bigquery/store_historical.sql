SELECT *,
       DATE_TRUNC(DATE_SUB( CAST("{{ ds.format('%Y-%m-%d') }}" AS DATE), INTERVAL 0 MONTH), MONTH) as local_date
FROM `{{ params['storage-prod'] }}.{{ params['visitor_brand_destinations_dataset'] }}.{{ params['observed_connections_table'] }}`