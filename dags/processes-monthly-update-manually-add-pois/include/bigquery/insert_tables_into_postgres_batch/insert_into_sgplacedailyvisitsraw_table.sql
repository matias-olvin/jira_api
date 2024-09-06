MERGE INTO
    `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['sgplacedailyvisitsraw_table'] }}` AS target USING `{{ var.value.env_project }}.{{ params['manually_add_pois_dataset'] }}.{{ params['sgplacedailyvisitsraw_table'] }}` AS source ON target.fk_sgplaces = source.fk_sgplaces
    AND target.local_date = source.local_date
WHEN MATCHED THEN
UPDATE SET
    fk_sgplaces = source.fk_sgplaces,
    local_date = source.local_date,
    visits = source.visits
WHEN NOT MATCHED THEN
INSERT
    (fk_sgplaces, local_date, visits)
VALUES
    (
        source.fk_sgplaces,
        source.local_date,
        source.visits
    );