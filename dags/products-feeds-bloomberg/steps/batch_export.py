from datetime import date, datetime, timedelta

from google.cloud import bigquery

bucket_name = "bloomberg-feed-olvin-com"


def main():
    project = "storage-prod-olvin-com"
    client = bigquery.Client(project=project)
    dataset_id = "bloomberg_feed"
    table_id = "2021"
    dataset_ref = bigquery.DatasetReference(project, dataset_id)

    job_config = bigquery.job.ExtractJobConfig()
    job_config.destination_format = bigquery.DestinationFormat.PARQUET
    job_config.compression = bigquery.Compression.SNAPPY

    jobs = []
    # days_in_year = {"2018": 366, "2019": 366, "2020": 367, "2021": 366}

    start_date = date(2021, 1, 1)
    end_date = date(2021, 8, 1)

    days = dates_between_two_dates(start_date, end_date)

    for import_day in days:
        day_date = import_day.strftime("%Y%m%d")
        month_date = import_day.strftime("%m")
        day = import_day.strftime("%d")

        print(day_date, month_date)
        destination_uri = f"gs://{bucket_name}/{table_id}/{month_date}/{day}/*.parquet"

        table_ref = dataset_ref.table(f"{table_id}${day_date}")

        extract_job = client.extract_table(
            table_ref,
            destination_uri,
            # Location must match that of the source table.
            job_config=job_config,
            location="EU",
        )  # API request
        jobs.append(extract_job)

    for job in jobs:
        job.result()


def dates_between_two_dates(d1: datetime, d2: datetime):
    days = []
    delta = d2 - d1
    for i in range(delta.days + 1):
        day = d1 + timedelta(days=i)
        days.append(day)

    return days


if __name__ == "__main__":
    main()
    # print('hello')
    # dates_between_two_dates(date(2020, 12, 1), date(2020, 12, 31))
