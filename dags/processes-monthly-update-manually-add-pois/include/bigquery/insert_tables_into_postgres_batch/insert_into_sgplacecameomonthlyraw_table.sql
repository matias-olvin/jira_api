MERGE INTO
    `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['sgplacecameomonthlyraw_table'] }}` AS target USING `{{ var.value.env_project }}.{{ params['manually_add_pois_dataset'] }}.{{ params['sgplacecameomonthlyraw_table'] }}` AS source ON target.fk_sgplaces = source.fk_sgplaces
    AND target.local_date = source.local_date
WHEN MATCHED THEN
UPDATE SET
    fk_sgplaces = source.fk_sgplaces,
    local_date = source.local_date,
    cameo_scores = source.cameo_scores
WHEN NOT MATCHED THEN
INSERT
    (fk_sgplaces, local_date, cameo_scores)
VALUES
    (
        source.fk_sgplaces,
        source.local_date,
        source.cameo_scores
    );