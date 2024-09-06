LOAD DATA OVERWRITE `storage-prod-olvin-com.postgres_staging.SGPlaceDailyVisitsRaw_prev`
PARTITION BY local_date
FROM FILES(
  format='CSV',
  compression='GZIP',
  field_delimiter="\t",
  uris = ['{{ uris }}']
);
