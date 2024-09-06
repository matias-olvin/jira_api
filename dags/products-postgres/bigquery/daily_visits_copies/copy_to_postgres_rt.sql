CREATE OR REPLACE TABLE `{{ var.value.env_project }}.{{ params['postgres_rt_dataset'] }}.{{ params['SGPlaceDailyVisitsRaw_table'] }}` COPY
 `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['SGPlaceDailyVisitsRaw_table'] }}`;

CREATE OR REPLACE TABLE `{{ var.value.env_project }}.{{ params['postgres_rt_dataset'] }}.{{ params['SGPlaceMonthlyVisitsRaw_table'] }}` COPY
 `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['SGPlaceMonthlyVisitsRaw_table'] }}`;