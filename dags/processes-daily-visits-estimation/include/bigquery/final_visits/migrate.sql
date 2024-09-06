MERGE INTO `{{ params['bigquery-project'] }}.{{ params['postgres-rt-dataset'] }}.{{ params['sgplacedailyvisitsraw-table'] }}` AS target
USING `{{ var.value.sns_project }}.{{ params['accessible-dataset'] }}.{{ params['postgres-rt-dataset'] }}-{{ params['sgplacedailyvisitsraw-table'] }}` AS source
ON
  target.fk_sgplaces = source.fk_sgplaces
  AND target.local_date = source.local_date
WHEN MATCHED THEN
  UPDATE SET target.visits = source.visits
;

MERGE INTO `{{ params['bigquery-project'] }}.{{ params['postgres-rt-dataset'] }}.{{ params['sgplacemonthlyvisitsraw-table'] }}` AS target
USING `{{ var.value.sns_project }}.{{ params['accessible-dataset'] }}.{{ params['postgres-rt-dataset'] }}-{{ params['sgplacemonthlyvisitsraw-table'] }}` AS source
ON
  target.fk_sgplaces = source.fk_sgplaces
  AND target.local_date = source.local_date
WHEN MATCHED THEN
  UPDATE SET target.visits = source.visits
;