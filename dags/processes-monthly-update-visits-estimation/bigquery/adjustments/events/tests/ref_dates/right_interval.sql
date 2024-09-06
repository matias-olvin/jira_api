SELECT COUNT(*)
FROM
    `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['tier_events_table'] }}`
WHERE  DATE_DIFF(local_date, ref_dates.week4back, DAY) <> 28
    OR DATE_DIFF(local_date, ref_dates.week3back, DAY) <> 21
    OR DATE_DIFF(local_date, ref_dates.week2back, DAY) <> 14
    OR DATE_DIFF(local_date, ref_dates.week1back, DAY) <> 7
    OR DATE_DIFF(local_date, ref_dates.week1, DAY) <> -7
    OR DATE_DIFF(local_date, ref_dates.week2, DAY) <> -14
    OR DATE_DIFF(local_date, ref_dates.week3, DAY) <> -21
    OR DATE_DIFF(local_date, ref_dates.week4, DAY) <> -28