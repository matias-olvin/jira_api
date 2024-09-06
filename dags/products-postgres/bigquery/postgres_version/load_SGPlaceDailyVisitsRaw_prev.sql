LOAD DATA OVERWRITE `{{ var.value.env_project }}.{{ params['postgres_staging_dataset'] }}.{{ params['SGPlaceDailyVisitsRaw_table'] }}_prev`
PARTITION BY
  local_date
FROM
  FILES (
    FORMAT = 'CSV',
    compression = 'GZIP',
    field_delimiter = "\t",
    uris = ['{{ uris }}']
  );