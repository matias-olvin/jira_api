from args import args
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark import sql


def load_from_bq_to_spark(
    project: str,
    tbl: str,
    partition_column: str,
    date_start: str,
    date_end: str,
    spark: sql.SparkSession,
) -> sql.DataFrame:
    """
    Load data from BigQuery into Spark DataFrame.

    Args:
        project (str): The BigQuery project ID.
        tbl (str): The BigQuery table name. Must be be qualified with the dataset name. eg. dataset.table_name
        partition_column (str): The column used for partitioning the data.
        date_start (str): The start date for the data range.
        date_end (str): The end date for the data range.
        spark (sql.SparkSession): The SparkSession object.

    Returns:
        sql.DataFrame: The loaded data as a Spark DataFrame.
    """

    df_read = (
        spark.read.format("bigquery")
        .option("table", f"{project}:{tbl}")
        .load()
        .where(
            f"{partition_column} >= '{date_start}' AND {partition_column} < '{date_end}'"
        )
    )

    return df_read


def write_parquet_from_spark_to_gcs(
    df: sql.DataFrame, num_partitions: int, partition_column: str, gcs_folder: str, append_mode: str
):
    """
    Writes a DataFrame to Google Cloud Storage (GCS) in Parquet format using Spark.

    Args:
        df (sql.DataFrame): The DataFrame to be written to GCS.
        num_partitions (int): The number of partitions to use when writing the data to gcs.
        partition_column (str): The column used for partitioning the data.
        gcs_folder (str): The GCS folder path where the Parquet files will be stored.
        append_mode (str): The write mode for the Parquet files. Can be 'append' or 'overwrite'.

    Returns:
        None
    """
    df.repartition(num_partitions, f"{partition_column}", "brand").write.mode(
        f"{append_mode}"
    ).partitionBy(f"{partition_column}").option("overwriteSchema", "true").option(
        "compression", "zstd"
    ).parquet(
        gcs_folder
    )

conf = SparkConf().setAppName("export-data-feed-gcs")
conf.set("spark.executor.cores", "5")

spark = SparkSession.builder.enableHiveSupport().config(
    conf=conf
).getOrCreate()

df = load_from_bq_to_spark(
    project=args.project,
    tbl=args.input_table,
    partition_column=args.partition_column,
    date_start=args.date_start,
    date_end=args.date_end,
    spark=spark
)

write_parquet_from_spark_to_gcs(
    df=df,
    num_partitions=int(args.num_files_per_partition),
    partition_column=args.partition_column,
    gcs_folder=args.output_folder,
    append_mode=args.append_mode
)