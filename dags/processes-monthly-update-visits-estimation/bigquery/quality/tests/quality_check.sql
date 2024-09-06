DECLARE prev DEFAULT (SELECT FORMAT_DATE('%B', DATE_TRUNC(DATE_SUB( CAST("{{ ds.format('%Y-%m-%d') }}" AS DATE), INTERVAL 1 MONTH), MONTH)  ));

DECLARE the_prev_total DEFAULT(
     SELECT ifnull(total_count,0)
     --FROM `storage-prod-olvin-com.sg_visits_estimation_output.monitoring_quality_check_poi`
     FROM 
        `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{params['quality_monitoring_table']}}`
     WHERE month_update = prev
     );

DECLARE the_date DATE DEFAULT "{{ ds.format('%Y-%m-%d') }}";
DECLARE the_warning BOOL  DEFAULT(FALSE);
DECLARE the_terminate BOOL  DEFAULT(FALSE);
DECLARE the_month STRING DEFAULT (SELECT FORMAT_DATE('%B', DATE_TRUNC(DATE_SUB( CAST("{{ ds.format('%Y-%m-%d') }}" AS DATE), INTERVAL 0 MONTH), MONTH)  ));
DECLARE the_current_total DEFAULT(SELECT COUNT( distinct fk_sgplaces) 
    FROM
     `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{  params['spurious_places_table']  }}`
    WHERE previous = false
);



DECLARE the_too_many_visits_count DEFAULT(SELECT count(distinct fk_sgplaces)
    FROM
     `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{  params['spurious_places_table']  }}`
    WHERE previous = false and cause = 'too_many_visits'
);
DECLARE the_too_little_visits_count DEFAULT(SELECT count(distinct fk_sgplaces)
    FROM
     `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{  params['spurious_places_table']  }}`
    WHERE previous = false and cause = 'too_little_visits'
);
DECLARE the_too_many_in_extreme_covid_count DEFAULT(SELECT count(distinct fk_sgplaces)
    FROM
     `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{  params['spurious_places_table']  }}`
    WHERE previous = false and cause = 'too_many_in_extreme_covid'
);
DECLARE the_too_many_in_mid_covid_count DEFAULT(SELECT count(distinct fk_sgplaces)
    FROM
     `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{  params['spurious_places_table']  }}`
    WHERE previous = false and cause = 'too_many_in_mid_covid'
);
DECLARE the_visits_remain_zero_after_covid_count DEFAULT(SELECT count(distinct fk_sgplaces)
    FROM
     `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{  params['spurious_places_table']  }}`
    WHERE previous = false and cause = 'visits_remain_zero_after_covid'
);
DECLARE the_future_visits_go_zero_count DEFAULT(SELECT count(distinct fk_sgplaces)
    FROM
     `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{  params['spurious_places_table']  }}`
    WHERE previous = false and cause = 'future_visits_go_zero'
);
DECLARE the_future_visits_go_high_count DEFAULT(SELECT count(distinct fk_sgplaces)
    FROM
     `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{  params['spurious_places_table']  }}`
    WHERE previous = false and cause = 'future_visits_go_high'
);
DECLARE the_monthly_double_increase_count DEFAULT(SELECT count(distinct fk_sgplaces)
    FROM
     `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{  params['spurious_places_table']  }}`
    WHERE previous = false and cause = 'monthly_double_increase'
);

DECLARE the_monthly_double_decrease_count DEFAULT(SELECT count(distinct fk_sgplaces)
    FROM
     `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{  params['spurious_places_table']  }}`
    WHERE previous = false and cause = 'monthly_decrease'
);

DECLARE the_constant_positive_trend DEFAULT(SELECT count(distinct fk_sgplaces)
    FROM
     `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{  params['spurious_places_table']  }}`
    WHERE previous = false and cause = 'constant_positive_trend'
);

DECLARE the_constant_negative_trend DEFAULT(SELECT count(distinct fk_sgplaces)
    FROM
     `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{  params['spurious_places_table']  }}`
    WHERE previous = false and cause = 'constant_negative_trend'
);



if (1.5 * the_prev_total < the_current_total) or (0.67 * the_prev_total > the_current_total)
    then 
        SET the_terminate = TRUE;
end if ;

if (1.2 * the_prev_total < the_current_total) or (0.83 * the_prev_total > the_current_total)
    then 
        SET the_warning = TRUE;
end if ;



UPDATE 
    `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['monitoring_quality_check_poi_table'] }}`
--UPDATE `storage-prod-olvin-com.sg_visits_estimation.monitoring_quality_check_poi`

SET 
run_date = the_date,
too_many_visits_count = the_too_many_visits_count,
too_little_visits_count = the_too_little_visits_count,
too_many_in_extreme_covid_count = the_too_many_in_extreme_covid_count,
too_many_in_mid_covid_count = the_too_many_in_mid_covid_count,
visits_remain_zero_after_covid_count = the_visits_remain_zero_after_covid_count,
future_visits_go_zero_count = the_future_visits_go_zero_count,
future_visits_go_high_count = the_future_visits_go_high_count,
monthly_double_increase_count = the_monthly_double_increase_count,
monthly_double_decrease_count = the_monthly_double_decrease_count,
constant_positive_trend = the_constant_positive_trend,
constant_negative_trend = the_constant_negative_trend,
total_count = the_current_total,
terminate = the_terminate,
warning = the_warning
 WHERE month_update = the_month;

