DROP SNAPSHOT TABLE IF EXISTS `{{ params['analysis_project'] }}.{{ params['marketing_dataset'] }}.{{ tbl }}`;

CREATE SNAPSHOT TABLE IF NOT EXISTS `{{ params['analysis_project'] }}.{{ params['marketing_dataset'] }}.{{ tbl }}` CLONE `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ tbl }}`;
