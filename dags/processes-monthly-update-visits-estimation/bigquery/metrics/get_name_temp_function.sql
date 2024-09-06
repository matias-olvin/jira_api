CREATE TEMP FUNCTION GetName(id STRING)
RETURNS STRING
AS (
  (
    select name
    from `storage-prod-olvin-com.postgres.SGBrandRaw`
    where pid = id
  )
);
