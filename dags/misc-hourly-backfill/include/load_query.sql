LOAD DATA OVERWRITE `storage-prod-olvin-com.accessible-by-sns.SGPlaceHourlyVisitsRaw_bf`
FROM
    FILES (
        FORMAT = 'CSV',
        compression = 'GZIP',
        field_delimiter = "\t",
        uris = [
            'gs://postgres-database-tables/SGPlaceHourlyVisitsRaw/{{ (macros.datetime.strptime(ds, "%Y-%m-%d").strftime("%Y") }}/{{ (macros.datetime.strptime(ds, "%Y-%m-%d").strftime("%m") }}/*.csv.gz'
        ]
    );