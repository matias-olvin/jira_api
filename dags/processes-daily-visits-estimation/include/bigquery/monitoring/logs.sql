-- Declare the date variable
DECLARE run_date_ DATE;

DECLARE analysis_date DATE;

DECLARE analysis_minus_1_date DATE;

DECLARE batch_start_date DATE;


-- Declare the date variable
DECLARE date_max_local_date DATE;

DECLARE latency_ INT64;

-- Set the value of the date variable
SET run_date_ = "{{ ds }}";

SET latency_ = {{ var.value.latency_daily_feed }};
-- Set the value of the analysis_date variable
SET analysis_date = DATE("{{ ti.xcom_pull(task_ids='local-date') }}");
SET analysis_minus_1_date =  DATE_SUB(analysis_date, INTERVAL 1 DAY);


-- Set the value of the date variable using a query
SET batch_start_date = (
  SELECT MAX(ds) 
  FROM `{{ var.value.sns_project }}.{{ params['visits-estimation-supervised-dataset'] }}_{{ params['stage'] }}.{{ params['batch-gt-scaled-groups-location-table'] }}` 
);




-- TABLES THAT WILL BE USED -- 
begin create temp table  
gt_visits_table as (
  SELECT local_date, fk_sgplaces, sum(visits) as visits
  FROM `{{ var.value.sns_project }}.{{ params['sns-raw-dataset'] }}.{{ params['sns-raw-traffic-formatted-table'] }}`
  where  local_date = analysis_date
  group by 1,2 

)
;end;

begin create temp table  

groups_tables_business as (
  select fk_sgplaces, tier_id
  from `{{ var.value.sns_project }}.{{ params['visits-estimation-supervised-dataset'] }}_{{ params['stage'] }}.{{ params['class-pois-sns-business-table'] }}`
  inner join `{{ var.value.sns_project }}.{{ params['visits-estimation-supervised-dataset'] }}_{{ params['stage'] }}.{{ params['list-sns-pois-table'] }}` using (fk_sgplaces)

)
;end;

begin create temp table  
nb_of_business_tier_pois_on_batch as (
  select tier_id, count(*) as count_pois_batch
  from groups_tables_business
  group by 1
)
;end;

begin create temp table  

groups_tables_location as (
  select fk_sgplaces, tier_id
  from `{{ var.value.sns_project }}.{{ params['visits-estimation-supervised-dataset'] }}_{{ params['stage'] }}.{{ params['class-pois-sns-location-table'] }}`
  inner join `{{ var.value.sns_project }}.{{ params['visits-estimation-supervised-dataset'] }}_{{ params['stage'] }}.{{ params['list-sns-pois-table'] }}` using (fk_sgplaces)

)
;end;

begin create temp table  
nb_of_location_tier_pois_on_batch as (
  select tier_id, count(*) as count_pois_batch
  from groups_tables_location
  group by 1
)
;end;


-- BELOW THE CODE GENERATED TO ALL THE TABLES NEEDED -- 
-- TEMP TABLES WILL BE CREATED -- When implemented, they should append a table

-- APPEND (delete if based on analysis_date/run_date_) IN CASE ALREADY RUN -- 
-- TABLE 1 -- Business data
DELETE FROM `{{ var.value.sns_project }}.{{ params['accessible-by-olvin-dataset'] }}.{{ params['visits-estimation-metrics-dataset'] }}-{{ params['sensors_monitoring_business_table'] }}`
WHERE latency=latency_
AND run_date="{{ ds }}"
AND stage = "{{ params['stage'] }}";

INSERT INTO `{{ var.value.sns_project }}.{{ params['accessible-by-olvin-dataset'] }}.{{ params['visits-estimation-metrics-dataset'] }}-{{ params['sensors_monitoring_business_table'] }}`
with 

business_groups_last_value as
(
  select 
    tier_id,
    local_date,
    run_date_ as run_date,
    count(if(visits=0,1, null)) as visits,
    count(*) as count_pois ,
    latency_ as latency,
    "{{ params['stage'] }}" AS stage
    
  
  from  
    gt_visits_table
  
  inner join groups_tables_business using (fk_sgplaces)
  group by 1, 2
)

select *
from business_groups_last_value

;

-- TABLE 2 -- Location data
DELETE FROM `{{ var.value.sns_project }}.{{ params['accessible-by-olvin-dataset'] }}.{{ params['visits-estimation-metrics-dataset'] }}-{{ params['sensors_monitoring_location_table'] }}`
WHERE latency=latency_
AND run_date="{{ ds }}"
AND stage = "{{ params['stage'] }}";

INSERT INTO `{{ var.value.sns_project }}.{{ params['accessible-by-olvin-dataset'] }}.{{ params['visits-estimation-metrics-dataset'] }}-{{ params['sensors_monitoring_location_table'] }}`
with 

location_groups_last_value as
(
  select 
    tier_id,
    local_date,
    run_date_ as run_date,
    count(if(visits=0,1, null)) as visits,
    count(*) as count_pois ,
    latency_ as latency,
    "{{ params['stage'] }}" AS stage
    
  
  from  
    gt_visits_table
  
  inner join groups_tables_location using (fk_sgplaces)
  group by 1, 2
)

select *
from location_groups_last_value
;


---------
-- MONITORING METRICS --
-- MONITORING TABLE 1 -- 
DELETE FROM `{{ var.value.sns_project }}.{{ params['accessible-by-olvin-dataset'] }}.{{ params['visits-estimation-metrics-dataset'] }}-{{ params['sensors_monitoring_location_metrics_table'] }}`
WHERE run_date="{{ ds }}"
AND stage = "{{ params['stage'] }}";

INSERT INTO `{{ var.value.sns_project }}.{{ params['accessible-by-olvin-dataset'] }}.{{ params['visits-estimation-metrics-dataset'] }}-{{ params['sensors_monitoring_location_metrics_table'] }}` 
with joining_location_info as (
  select *
  from
  (
    select tier_id, latency_, visits as visits_batch_date, count_pois as count_pois_batch_date
    from `{{ var.value.sns_project }}.{{ params['accessible-by-olvin-dataset'] }}.{{ params['visits-estimation-metrics-dataset'] }}-{{ params['sensors_monitoring_location_table'] }}` 
    where local_date = batch_start_date
    AND stage = "{{ params['stage'] }}"
  ) 


  left join

  (
    select tier_id, latency_, visits as visits_minus_1, count_pois as count_pois_minus_1
    from `{{ var.value.sns_project }}.{{ params['accessible-by-olvin-dataset'] }}.{{ params['visits-estimation-metrics-dataset'] }}-{{ params['sensors_monitoring_location_table'] }}` 
    where local_date = analysis_minus_1_date
    AND stage = "{{ params['stage'] }}"
  ) using (tier_id, latency_)


  left join

  (
    select tier_id, local_date, run_date, visits, count_pois, latency_
    from `{{ var.value.sns_project }}.{{ params['accessible-by-olvin-dataset'] }}.{{ params['visits-estimation-metrics-dataset'] }}-{{ params['sensors_monitoring_location_table'] }}` 
    where local_date = analysis_date
    AND stage = "{{ params['stage'] }}"
    
  ) using (tier_id, latency_)

  inner join nb_of_location_tier_pois_on_batch using (tier_id)

)
select
  run_date, local_date, tier_id,
  count_pois / nullif(count_pois_minus_1, 0) as ratio_count_loss_vs_minus_1,
  count_pois / nullif(count_pois_batch_date, 0) as ratio_count_loss_vs_batch_date,
  count_pois / nullif(count_pois_batch, 0) as ratio_count_loss_vs_batch_static,
  count_pois,
  visits / nullif(visits_minus_1, 0) as ratio_visits_loss_vs_minus_1,
  visits / nullif(visits_batch_date, 0) as ratio_visits_loss_vs_batch_date,
  visits,
  "{{ params['stage'] }}" AS stage
from joining_location_info ;


-- MONITORING TABLE 2 -- 
DELETE FROM `{{ var.value.sns_project }}.{{ params['accessible-by-olvin-dataset'] }}.{{ params['visits-estimation-metrics-dataset'] }}-{{ params['sensors_monitoring_business_metrics_table'] }}`
WHERE run_date="{{ ds }}"
AND stage = "{{ params['stage'] }}";

INSERT INTO `{{ var.value.sns_project }}.{{ params['accessible-by-olvin-dataset'] }}.{{ params['visits-estimation-metrics-dataset'] }}-{{ params['sensors_monitoring_business_metrics_table'] }}` 
with joining_business_info as (
  select *
  from
  (
    select tier_id, latency_, visits as visits_batch_date, count_pois as count_pois_batch_date
    from `{{ var.value.sns_project }}.{{ params['accessible-by-olvin-dataset'] }}.{{ params['visits-estimation-metrics-dataset'] }}-{{ params['sensors_monitoring_business_table'] }}` 
    where local_date = batch_start_date
    AND stage = "{{ params['stage'] }}"
  ) 


  left join

  (
    select tier_id, latency_, visits as visits_minus_1, count_pois as count_pois_minus_1
    from `{{ var.value.sns_project }}.{{ params['accessible-by-olvin-dataset'] }}.{{ params['visits-estimation-metrics-dataset'] }}-{{ params['sensors_monitoring_business_table'] }}` 
    where local_date = analysis_minus_1_date
    AND stage = "{{ params['stage'] }}"
  ) using (tier_id, latency_)


  left join

  (
    select tier_id, local_date, run_date, visits, count_pois, latency_
    from `{{ var.value.sns_project }}.{{ params['accessible-by-olvin-dataset'] }}.{{ params['visits-estimation-metrics-dataset'] }}-{{ params['sensors_monitoring_business_table'] }}` 
    where local_date = analysis_date
    AND stage = "{{ params['stage'] }}"
    
  ) using (tier_id, latency_)

  inner join nb_of_business_tier_pois_on_batch using (tier_id)

)
select
  run_date, local_date, tier_id,
  count_pois / nullif(count_pois_minus_1, 0) as ratio_count_loss_vs_minus_1,
  count_pois / nullif(count_pois_batch_date, 0) as ratio_count_loss_vs_batch_date,
  count_pois / nullif(count_pois_batch, 0) as ratio_count_loss_vs_batch_static,
  count_pois,
  visits / nullif(visits_minus_1, 0) as ratio_visits_loss_vs_minus_1,
  visits / nullif(visits_batch_date, 0) as ratio_visits_loss_vs_batch_date,
  visits,
  "{{ params['stage'] }}" AS stage
from joining_business_info ;

-- CREATE OR REPLACE TABLE `{{ var.value.env_project }}.{{ params['visits-estimation-metrics-dataset'] }}.{{ params['sensors_monitoring_business_table'] }}` COPY 
-- `{{ var.value.sns_project }}.{{ params['accessible-by-olvin-dataset'] }}.{{ params['visits-estimation-metrics-dataset'] }}-{{ params['sensors_monitoring_business_table'] }}`;

-- CREATE OR REPLACE TABLE `{{ var.value.env_project }}.{{ params['visits-estimation-metrics-dataset'] }}.{{ params['sensors_monitoring_location_table'] }}` COPY 
-- `{{ var.value.sns_project }}.{{ params['accessible-by-olvin-dataset'] }}.{{ params['visits-estimation-metrics-dataset'] }}-{{ params['sensors_monitoring_location_table'] }}`;

-- CREATE OR REPLACE TABLE `{{ var.value.env_project }}.{{ params['visits-estimation-metrics-dataset'] }}.{{ params['sensors_monitoring_business_metrics_table'] }}` COPY 
-- `{{ var.value.sns_project }}.{{ params['accessible-by-olvin-dataset'] }}.{{ params['visits-estimation-metrics-dataset'] }}-{{ params['sensors_monitoring_business_metrics_table'] }}`;

-- CREATE OR REPLACE TABLE `{{ var.value.env_project }}.{{ params['visits-estimation-metrics-dataset'] }}.{{ params['sensors_monitoring_location_metrics_table'] }}` COPY 
-- `{{ var.value.sns_project }}.{{ params['accessible-by-olvin-dataset'] }}.{{ params['visits-estimation-metrics-dataset'] }}-{{ params['sensors_monitoring_location_metrics_table'] }}`;