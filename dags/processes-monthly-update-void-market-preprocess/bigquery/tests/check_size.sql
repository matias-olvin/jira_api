DECLARE prev DEFAULT (SELECT FORMAT_DATE('%B', DATE_TRUNC(DATE_SUB( CAST("{{ ds.format('%Y-%m-%d') }}" AS DATE), INTERVAL 1 MONTH), MONTH)  ));

DECLARE the_prev_size DEFAULT(
     SELECT size
     FROM
        `{{ params['storage-prod'] }}.{{ params['visitor_brand_destinations_metrics_dataset'] }}.{{  params['monitoring_market_table'] }}`
     WHERE month_update = prev
     );

DECLARE the_date DATE DEFAULT "{{ ds.format('%Y-%m-%d') }}";
DECLARE the_warning BOOL  DEFAULT(FALSE);
DECLARE the_terminate BOOL  DEFAULT(FALSE);
DECLARE the_mon STRING DEFAULT (SELECT FORMAT_DATE('%B', DATE_TRUNC(DATE_SUB( CAST("{{ ds.format('%Y-%m-%d') }}" AS DATE), INTERVAL 0 MONTH), MONTH)  ));
DECLARE the_current_size DEFAULT(SELECT COUNT(*)
    FROM
        `{{ params['storage-prod'] }}.{{ params['postgres_dataset'] }}.{{ params['market_analysis_table']  }}`
);


if the_current_size > 1.1 * the_prev_size
    then
        SET the_terminate = TRUE;
end if ;

if the_current_size > the_prev_size
    then
        SET the_warning = TRUE;
end if ;

UPDATE
    `{{ params['storage-prod'] }}.{{ params['visitor_brand_destinations_metrics_dataset'] }}.{{ params['monitoring_market_table']}}`
SET
run_date = the_date,
size = the_current_size,
terminate = the_terminate,
warning = the_warning
 WHERE month_update = the_mon;



