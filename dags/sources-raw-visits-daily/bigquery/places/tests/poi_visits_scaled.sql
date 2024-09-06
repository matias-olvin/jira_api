DECLARE prev DEFAULT  CAST("{{ execution_date.subtract(days=(var.value.latency_days_visits|int+1)).strftime('%Y-%m-%d') }}" AS DATE);

DECLARE the_prev_size DEFAULT(
    SELECT size_ 
    FROM 
        `{{ params['project'] }}.{{ params['monitoring_dataset'] }}.{{ params['poi_visits_scaled_met_areas_dataset'] }}` 
    WHERE local_date = prev
);

DECLARE the_prev_visit_score_size DEFAULT(
    SELECT total_visit_score_
    FROM
        `{{ params['project'] }}.{{ params['monitoring_dataset'] }}.{{ params['poi_visits_scaled_met_areas_dataset']  }}` 
    WHERE local_date = prev
);

DECLARE the_prev_real_visit_score_size DEFAULT(
    SELECT total_real_visit_score_
    FROM
        `{{ params['project'] }}.{{ params['monitoring_dataset'] }}.{{ params['poi_visits_scaled_met_areas_dataset']  }}` 
    WHERE local_date = prev
);

DECLARE the_prev_current_unique_places DEFAULT(
    SELECT distinct_places_
    FROM
        `{{ params['project'] }}.{{ params['monitoring_dataset'] }}.{{ params['poi_visits_scaled_met_areas_dataset']  }}` 
    WHERE local_date = prev
);

DECLARE the_prev_unique_devices DEFAULT(
    SELECT distinct_devices_
    FROM
        `{{ params['project'] }}.{{ params['monitoring_dataset'] }}.{{ params['poi_visits_scaled_met_areas_dataset']  }}` 
    WHERE local_date = prev
);

DECLARE the_date DATE DEFAULT CAST("{{ execution_date.subtract(days=var.value.latency_days_visits|int).strftime('%Y-%m-%d') }}" AS DATE) ;
DECLARE the_warning BOOL  DEFAULT(FALSE);
DECLARE the_terminate BOOL  DEFAULT(FALSE);
DECLARE the_comment ARRAY<STRING> DEFAULT([""]);

DECLARE the_current_size DEFAULT(
    SELECT 
        COUNT(*) 
    FROM   
        `{{ params['project'] }}.{{ params['poi_visits_scaled_met_areas_dataset'] }}.{{ execution_date.subtract(days=var.value.latency_days_visits|int).strftime('%Y')}}`
    where local_date = the_date
--      
);
DECLARE the_current_visit_score_size DEFAULT(
    SELECT 
        SUM(visit_score) 
    FROM   
        `{{ params['project'] }}.{{ params['poi_visits_scaled_met_areas_dataset'] }}.{{ execution_date.subtract(days=var.value.latency_days_visits|int).strftime('%Y')}}`
    where local_date = the_date
--     `{{ params['project'] }}.{{ params['inference_input_poi_dataset'] }}.{{ element }}` 
);
DECLARE the_current_reaL_visit_score_size DEFAULT(
    SELECT 
        SUM(visit_score_real) 
    FROM   
        `{{ params['project'] }}.{{ params['poi_visits_scaled_met_areas_dataset'] }}.{{ execution_date.subtract(days=var.value.latency_days_visits|int).strftime('%Y')}}`
    where local_date = the_date
--     `{{ params['project'] }}.{{ params['inference_input_poi_dataset'] }}.{{ element }}` 
);

DECLARE the_current_unique_size DEFAULT(
    SELECT 
        count(*)
    FROM (
        select distinct 
            device_id, 
            visit_ts, 
            fk_sgplaces 
        FROM 
            `{{ params['project'] }}.{{ params['poi_visits_scaled_met_areas_dataset'] }}.{{ execution_date.subtract(days=var.value.latency_days_visits|int).strftime('%Y')}}`
        where local_date = the_date
    )
);
DECLARE the_current_unique_places DEFAULT(
    SELECT 
        count(distinct fk_sgplaces)
    FROM  
    `{{ params['project'] }}.{{ params['poi_visits_scaled_met_areas_dataset'] }}.{{ execution_date.subtract(days=var.value.latency_days_visits|int).strftime('%Y')}}`
    where local_date = the_date
);
DECLARE the_current_unique_devices DEFAULT(
    SELECT 
        count(distinct device_id)
    FROM 
      `{{ params['project'] }}.{{ params['poi_visits_scaled_met_areas_dataset'] }}.{{ execution_date.subtract(days=var.value.latency_days_visits|int).strftime('%Y')}}`
    where local_date = the_date
);

DECLARE the_current_nans DEFAULT(
    SELECT 
        count(*)
    FROM 
      `{{ params['project'] }}.{{ params['poi_visits_scaled_met_areas_dataset'] }}.{{ execution_date.subtract(days=var.value.latency_days_visits|int).strftime('%Y')}}`
    where local_date = the_date and visit_score is null
);

if the_current_size <> the_current_unique_size
    then 
        SET the_terminate = TRUE;
        SET the_comment = ARRAY_CONCAT(the_comment, ['duplicates']);
end if ;

if the_current_unique_devices > 1.2 * the_prev_unique_devices or  the_current_unique_devices < 0.8 *the_prev_unique_devices 
    then 
        SET the_terminate = TRUE;
        SET the_comment = ARRAY_CONCAT(the_comment, ['distinct_devices']);
end if ;

if the_current_unique_places > 1.3 * the_prev_current_unique_places  or the_current_unique_places < 0.7*the_prev_current_unique_places 
    then 
        SET the_terminate = TRUE;
        SET the_comment = ARRAY_CONCAT(the_comment, ['distinct_places']);
end if ;

if the_current_nans > 0
    then 
        SET the_terminate = TRUE;
        SET the_comment = ARRAY_CONCAT(the_comment, ['count_nans']);
end if ;

if the_current_visit_score_size > 1.8 * the_prev_visit_score_size or the_current_visit_score_size < 0.2 * the_prev_visit_score_size
    then 
        SET the_terminate = TRUE;
        SET the_comment = ARRAY_CONCAT(the_comment, ['visit_score']);
end if ;

if the_current_visit_score_size > 1.2 * the_prev_visit_score_size  or  the_current_visit_score_size < 0.8 * the_prev_visit_score_size
    then 
        SET the_warning = TRUE;
end if;

if the_current_reaL_visit_score_size > 1.8 * the_prev_real_visit_score_size or the_current_reaL_visit_score_size < 0.2 * the_prev_real_visit_score_size
    then 
        SET the_terminate = TRUE;
        SET the_comment = ARRAY_CONCAT(the_comment, ['visit_score']);
end if ;

if the_current_reaL_visit_score_size > 1.2 * the_prev_real_visit_score_size  or  the_current_reaL_visit_score_size < 0.8 * the_prev_real_visit_score_size
    then 
        SET the_warning = TRUE;
end if;


if the_current_size > 1.5 * the_prev_size or the_current_size < 0.5 * the_prev_size
    then 
        SET the_terminate = TRUE;
        SET the_comment = ARRAY_CONCAT(the_comment, ['size']);
end if ;

if the_current_size > 1.2 * the_prev_size or the_current_size < 0.8 * the_prev_size 
    then 
        SET the_warning = TRUE;
        
end if ;

MERGE `{{ params['project'] }}.{{ params['monitoring_dataset'] }}.{{ params['poi_visits_scaled_met_areas_dataset']  }}` T
USING (
    SELECT 
        the_date,
        the_current_size,
        the_current_visit_score_size,
        the_current_reaL_visit_score_size,
        the_current_unique_devices,
        the_current_unique_size,
        the_current_unique_places,
        the_current_nans,
        the_terminate,
        the_warning,
        ARRAY_TO_STRING(the_comment, ",")
) S
ON T.local_date = S.the_date
WHEN MATCHED THEN
    UPDATE SET 
        local_date = the_date,
        size_ = the_current_size,
        total_visit_score_ = the_current_visit_score_size,
        total_real_visit_score_ = the_current_reaL_visit_score_size,
        distinct_devices_ = the_current_unique_devices,
        count_nans_ = the_current_nans,
        distinct_size_ = the_current_unique_size,
        distinct_places_ = the_current_unique_places,
        terminate_ = the_terminate,
        warning_ = the_warning,
        comment_ = ARRAY_TO_STRING(the_comment, ",")
WHEN NOT MATCHED THEN 
    INSERT
    (
        local_date, 
        size_, 
        total_visit_score_, 
        total_real_visit_score_, 
        distinct_devices_, 
        distinct_size_, 
        distinct_places_, 
        count_nans_, 
        terminate_, 
        warning_, 
        comment_
    )
VALUES(
    the_date,
    the_current_size,
    the_current_visit_score_size,
    the_current_reaL_visit_score_size,
    the_current_unique_devices,
    the_current_unique_size,
    the_current_unique_places,
    the_current_nans,
    the_terminate,
    the_warning,
    ARRAY_TO_STRING(the_comment, ",")
)

