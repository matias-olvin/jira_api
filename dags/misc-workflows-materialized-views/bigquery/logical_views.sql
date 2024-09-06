CREATE OR REPLACE VIEW `{{ var.value.env_project }}.{{ params['accessible_by_sns_dataset'] }}.v-{{ params['area_geometries_dataset'] }}-{{ params['zipcodes_table'] }}` AS 
select * from `{{ var.value.env_project }}.{{ params['area_geometries_dataset'] }}.{{ params['zipcodes_table'] }}`;

CREATE OR REPLACE VIEW `{{ var.value.env_project }}.{{ params['accessible_by_sns_dataset'] }}.v-{{ params['daily_estimation_dataset'] }}-{{ params['grouped_daily_olvin_table'] }}` AS 
select * from `{{ var.value.env_project }}.{{ params['daily_estimation_dataset'] }}.{{ params['grouped_daily_olvin_table'] }}`;

CREATE OR REPLACE VIEW `{{ var.value.env_project }}.{{ params['accessible_by_sns_dataset'] }}.v-{{ params['daily_estimation_dataset'] }}-{{ params['poi_list_table'] }}` AS 
select * from `{{ var.value.env_project }}.{{ params['daily_estimation_dataset'] }}.{{ params['poi_list_table'] }}`;

CREATE OR REPLACE VIEW `{{ var.value.env_project }}.{{ params['accessible_by_sns_dataset'] }}.v-{{ params['events_dataset'] }}-{{ params['holidays_table'] }}` AS 
select * from `{{ var.value.env_project }}.{{ params['events_dataset'] }}.{{ params['holidays_table'] }}`;

CREATE OR REPLACE VIEW `{{ var.value.env_project }}.{{ params['accessible_by_sns_dataset'] }}.v-{{ params['ground_truth_volume_model_dataset'] }}-{{ params['attribute_adapted_pois_table'] }}` AS 
select * from `{{ var.value.env_project }}.{{ params['ground_truth_volume_model_dataset'] }}.{{ params['attribute_adapted_pois_table'] }}`;

CREATE OR REPLACE VIEW `{{ var.value.env_project }}.{{ params['accessible_by_sns_dataset'] }}.v-{{ params['postgres_dataset'] }}-{{ params['CensusTract_table'] }}` AS 
select * from `{{ var.value.env_project }}.{{ params['postgres_dataset'] }}.{{ params['CensusTract_table'] }}`;

CREATE OR REPLACE VIEW `{{ var.value.env_project }}.{{ params['accessible_by_sns_dataset'] }}.v-{{ params['postgres_dataset'] }}-{{ params['sgcenterraw_table'] }}` AS 
select * from `{{ var.value.env_project }}.{{ params['postgres_dataset'] }}.{{ params['sgcenterraw_table'] }}`;

CREATE OR REPLACE VIEW `{{ var.value.env_project }}.{{ params['accessible_by_sns_dataset'] }}.v-{{ params['postgres_dataset'] }}-{{ params['sgplacedailyvisitsraw_table'] }}` AS 
select * from `{{ var.value.env_project }}.{{ params['postgres_dataset'] }}.{{ params['sgplacedailyvisitsraw_table'] }}`;

CREATE OR REPLACE VIEW `{{ var.value.env_project }}.{{ params['accessible_by_sns_dataset'] }}.v-{{ params['postgres_dataset'] }}-{{ params['sgplacemonthlyvisitsraw_table'] }}` AS 
select * from `{{ var.value.env_project }}.{{ params['postgres_dataset'] }}.{{ params['sgplacemonthlyvisitsraw_table'] }}`;

CREATE OR REPLACE VIEW `{{ var.value.env_project }}.{{ params['accessible_by_sns_dataset'] }}.v-{{ params['postgres_dataset'] }}-{{ params['sgplaceraw_table'] }}` AS 
select * from `{{ var.value.env_project }}.{{ params['postgres_dataset'] }}.{{ params['sgplaceraw_table'] }}`;

CREATE OR REPLACE VIEW `{{ var.value.env_project }}.{{ params['accessible_by_sns_dataset'] }}.v-{{ params['postgres_dataset'] }}-{{ params['zipcoderaw_table'] }}` AS 
select * from `{{ var.value.env_project }}.{{ params['postgres_dataset'] }}.{{ params['zipcoderaw_table'] }}`;

CREATE OR REPLACE VIEW `{{ var.value.env_project }}.{{ params['accessible_by_sns_dataset'] }}.v-{{ params['postgres_batch_dataset'] }}-{{ params['sgcenterraw_table'] }}` AS 
select * from `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['sgcenterraw_table'] }}`;

CREATE OR REPLACE VIEW `{{ var.value.env_project }}.{{ params['accessible_by_sns_dataset'] }}.v-{{ params['postgres_batch_dataset'] }}-{{ params['sgplacedailyvisitsraw_table'] }}` AS 
select * from `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['sgplacedailyvisitsraw_table'] }}`;

CREATE OR REPLACE VIEW `{{ var.value.env_project }}.{{ params['accessible_by_sns_dataset'] }}.v-{{ params['postgres_batch_dataset'] }}-{{ params['sgplacemonthlyvisitsraw_table'] }}` AS 
select * from `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['sgplacemonthlyvisitsraw_table'] }}`;

CREATE OR REPLACE VIEW `{{ var.value.env_project }}.{{ params['accessible_by_sns_dataset'] }}.v-{{ params['postgres_batch_dataset'] }}-{{ params['sgplaceraw_table'] }}` AS 
select * from `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['sgplaceraw_table'] }}`;

CREATE OR REPLACE VIEW `{{ var.value.env_project }}.{{ params['accessible_by_sns_dataset'] }}.v-{{ params['regressors_dataset'] }}-{{ params['predicthq_events_collection_table'] }}` AS 
select * from `{{ var.value.env_project }}.{{ params['regressors_dataset'] }}.{{ params['predicthq_events_collection_table'] }}`;

CREATE OR REPLACE VIEW `{{ var.value.env_project }}.{{ params['accessible_by_sns_dataset'] }}.v-{{ params['regressors_dataset'] }}-{{ params['predicthq_events_dictionary_table'] }}` AS 
select * from `{{ var.value.env_project }}.{{ params['regressors_dataset'] }}.{{ params['predicthq_events_dictionary_table'] }}`;

CREATE OR REPLACE VIEW `{{ var.value.env_project }}.{{ params['accessible_by_sns_dataset'] }}.v-{{ params['regressors_dataset'] }}-{{ params['web_search_trend_table'] }}` AS 
select * from `{{ var.value.env_project }}.{{ params['regressors_dataset'] }}.{{ params['web_search_trend_table'] }}`;

CREATE OR REPLACE VIEW `{{ var.value.env_project }}.{{ params['accessible_by_sns_dataset'] }}.v-{{ params['regressors_dataset'] }}-{{ params['web_search_trend_dictionary_table'] }}` AS 
select * from `{{ var.value.env_project }}.{{ params['regressors_dataset'] }}.{{ params['web_search_trend_dictionary_table'] }}`;

CREATE OR REPLACE VIEW `{{ var.value.env_project }}.{{ params['accessible_by_sns_dataset'] }}.v-{{ params['sg_base_tables_dataset'] }}-{{ params['naics_code_subcategories_table'] }}` AS 
select * from `{{ var.value.env_project }}.{{ params['sg_base_tables_dataset'] }}.{{ params['naics_code_subcategories_table'] }}`;

CREATE OR REPLACE VIEW `{{ var.value.env_project }}.{{ params['accessible_by_sns_dataset'] }}.v-{{ params['sg_places_dataset'] }}-{{ params['openings_adjustment_table'] }}` AS 
select * from `{{ var.value.env_project }}.{{ params['sg_places_dataset'] }}.{{ params['openings_adjustment_table'] }}`;

CREATE OR REPLACE VIEW `{{ var.value.env_project }}.{{ params['accessible_by_sns_dataset'] }}.v-{{ params['sg_places_dataset'] }}-{{ params['openings_adjustment_week_array_table'] }}` AS 
select * from `{{ var.value.env_project }}.{{ params['sg_places_dataset'] }}.{{ params['openings_adjustment_week_array_table'] }}`;

CREATE OR REPLACE VIEW `{{ var.value.env_project }}.{{ params['accessible_by_sns_dataset'] }}.v-{{ params['sg_places_dataset'] }}-{{ params['places_dynamic_table'] }}` AS 
select * from `{{ var.value.env_project }}.{{ params['sg_places_dataset'] }}.{{ params['places_dynamic_table'] }}`;

CREATE OR REPLACE VIEW `{{ var.value.env_project }}.{{ params['accessible_by_sns_dataset'] }}.v-{{ params['sg_places_dataset'] }}-{{ params['spend_patterns_static_table'] }}` AS 
select * from `{{ var.value.env_project }}.{{ params['sg_places_dataset'] }}.{{ params['spend_patterns_static_table'] }}`;

CREATE OR REPLACE VIEW `{{ var.value.env_project }}.{{ params['accessible_by_sns_dataset'] }}.v-{{ params['smc_daily_estimation_dataset'] }}-{{ params['grouped_daily_olvin_table'] }}` AS 
select * from `{{ var.value.env_project }}.{{ params['smc_daily_estimation_dataset'] }}.{{ params['grouped_daily_olvin_table'] }}`;

CREATE OR REPLACE VIEW `{{ var.value.env_project }}.{{ params['accessible_by_sns_dataset'] }}.v-{{ params['smc_daily_estimation_dataset'] }}-{{ params['poi_list_table'] }}` AS 
select * from `{{ var.value.env_project }}.{{ params['smc_daily_estimation_dataset'] }}.{{ params['poi_list_table'] }}`;

CREATE OR REPLACE VIEW `{{ var.value.env_project }}.{{ params['accessible_by_sns_dataset'] }}.v-{{ params['smc_ground_truth_volume_model_dataset'] }}-{{ params['model_input_table'] }}` AS 
select * from `{{ var.value.env_project }}.{{ params['smc_ground_truth_volume_model_dataset'] }}.{{ params['model_input_table'] }}`;

CREATE OR REPLACE VIEW `{{ var.value.env_project }}.{{ params['accessible_by_sns_dataset'] }}.v-{{ params['smc_ground_truth_volume_model_dataset'] }}-{{ params['model_input_visits_table'] }}` AS 
select * from `{{ var.value.env_project }}.{{ params['smc_ground_truth_volume_model_dataset'] }}.{{ params['model_input_visits_table'] }}`;

CREATE OR REPLACE VIEW `{{ var.value.env_project }}.{{ params['accessible_by_sns_dataset'] }}.v-{{ params['smc_ground_truth_volume_model_dataset'] }}-{{ params['prior_brand_visits_table'] }}` AS 
select * from `{{ var.value.env_project }}.{{ params['smc_ground_truth_volume_model_dataset'] }}.{{ params['prior_brand_visits_table'] }}`;

CREATE OR REPLACE VIEW `{{ var.value.env_project }}.{{ params['accessible_by_sns_dataset'] }}.v-{{ params['smc_ground_truth_volume_model_dataset'] }}-{{ params['stats_to_unscale_block_A_table'] }}` AS 
select * from `{{ var.value.env_project }}.{{ params['smc_ground_truth_volume_model_dataset'] }}.{{ params['stats_to_unscale_block_A_table'] }}`;

CREATE OR REPLACE VIEW `{{ var.value.env_project }}.{{ params['accessible_by_sns_dataset'] }}.v-{{ params['smc_postgres_dataset'] }}-{{ params['sgplaceraw_table'] }}` AS 
select * from `{{ var.value.env_project }}.{{ params['smc_postgres_dataset'] }}.{{ params['sgplaceraw_table'] }}`;

CREATE OR REPLACE VIEW `{{ var.value.env_project }}.{{ params['accessible_by_sns_dataset'] }}.v-{{ params['smc_sg_places_dataset'] }}-{{ params['brands_dynamic_table'] }}` AS 
select * from `{{ var.value.env_project }}.{{ params['smc_sg_places_dataset'] }}.{{ params['brands_dynamic_table'] }}`;

CREATE OR REPLACE VIEW `{{ var.value.env_project }}.{{ params['accessible_by_sns_dataset'] }}.v-{{ params['smc_sg_places_dataset'] }}-{{ params['places_dynamic_table'] }}` AS 
select * from `{{ var.value.env_project }}.{{ params['smc_sg_places_dataset'] }}.{{ params['places_dynamic_table'] }}`;

CREATE OR REPLACE VIEW `{{ var.value.env_project }}.{{ params['accessible_by_sns_dataset'] }}.v-{{ params['smc_sg_places_dataset'] }}-{{ params['places_dynamic_placekey_table'] }}` AS 
select * from `{{ var.value.env_project }}.{{ params['smc_sg_places_dataset'] }}.{{ params['places_dynamic_placekey_table'] }}`;

CREATE OR REPLACE VIEW `{{ var.value.env_project }}.{{ params['accessible_by_sns_dataset'] }}.v-{{ params['smc_sg_places_dataset'] }}-{{ params['spend_patterns_static_table'] }}` AS 
select * from `{{ var.value.env_project }}.{{ params['smc_sg_places_dataset'] }}.{{ params['spend_patterns_static_table'] }}`;

CREATE OR REPLACE VIEW `{{ var.value.env_project }}.{{ params['accessible_by_sns_dataset'] }}.v-{{ params['visits_to_malls_dataset'] }}-{{ params['training_data_table'] }}` AS 
select * from `{{ var.value.env_project }}.{{ params['visits_to_malls_dataset'] }}.{{ params['training_data_table'] }}`;

CREATE OR REPLACE VIEW `{{ var.value.env_project }}.{{ params['accessible_by_sns_dataset'] }}.v-{{ params['public_data_feeds_type_1_dataset'] }}-{{ params['store_visits_table'] }}` AS 
select * from `{{ var.value.env_project }}.{{ params['public_data_feeds_type_1_dataset'] }}.{{ params['store_visits_table'] }}`;