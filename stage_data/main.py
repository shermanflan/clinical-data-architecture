"""
Usage:
- From docker interactive against bitnami docker-compose cluster:
docker run --rm -it --name test_pyspark --network container:spark_ingest_spark_1 spark-ingest:latest /bin/bash
- From Spark 3.1.1 base container with Python bindings:
docker run --rm -it --name test_pyspark spark-ingest:latest /bin/bash
./bin/spark-submit spark-ingest/main.py --filepath ./examples/src/main/python/pi.py
"""
from datetime import datetime, date, timedelta
import logging
import os

# import boto3
import click
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, concat, expr, lit, sum as spark_sum, to_timestamp,
    year as spark_year
)
from pyspark.sql.types import (
    StringType, StructType, StructField, IntegerType,
    ArrayType, BooleanType, FloatType, DateType
)

logging.basicConfig(format='%(asctime)s %(levelname)s [%(name)s]: %(message)s',
                    datefmt='%Y-%m-%d %I:%M:%S %p', level=logging.INFO)
logger = logging.getLogger(__name__)
LOG_LEVEL = os.environ.get('LOG_LEVEL', 'WARN')

# Programmatic way to define a schema
# ID,DATE,PATIENT,CODE,DESCRIPTION,REASONCODE,REASONDESCRIPTION
ENCOUNTER = StructType([
    StructField('ID', StringType(), True),
    StructField('DATE', DateType(), True),
    StructField('PATIENT', StringType(), True),
    StructField('CODE', StringType(), True),
    StructField('DESCRIPTION', StringType(), True),
    StructField('REASONCODE', StringType(), True),
    StructField('REASONDESCRIPTION', StringType(), True),
])


@click.command()
@click.option('--filepath', required=False, help='The input file path')
@click.option('--output_path', required=False, help='The output file path')
def main(filepath: str, output_path: str) -> None:
    """
    To configure AWS bucket-specific authorization, use the
    `fs.s3a.bucket.[bucket name].access.key` configuration setting.

    As specified here:
    - https://hadoop.apache.org/docs/current2/hadoop-aws/tools/hadoop-aws/index.html#Configuring_different_S3_buckets

    TODO: Consider optimizing the S3A for I/O.
    - https://spark.apache.org/docs/3.1.1/cloud-integration.html#recommended-settings-for-writing-to-object-stores
    """
    spark_session = (SparkSession
                     .builder
                     .appName("spark_ingest_poc")
                     .config(f"fs.s3a.bucket.{os.environ['P3_BUCKET']}.access.key",
                             os.environ['P3_AWS_ACCESS_KEY'])
                     .config(f"fs.s3a.bucket.{os.environ['P3_BUCKET']}.secret.key",
                             os.environ['P3_AWS_SECRET_KEY'])
                     .config("spark.hadoop.fs.s3a.bucket.bangkok.access.key",
                             os.environ['BK_AWS_ACCESS_KEY'])
                     .config("spark.hadoop.fs.s3a.bucket.bangkok.secret.key",
                             os.environ['BK_AWS_SECRET_KEY'])
                     .config("spark.hadoop.fs.s3a.bucket.condesa.access.key",
                             os.environ['CO_AWS_ACCESS_KEY'])
                     .config("spark.hadoop.fs.s3a.bucket.condesa.secret.key",
                             os.environ['CO_AWS_SECRET_KEY'])
                     # TODO: S3A Optimizations
                     # .config("spark.hadoop.fs.s3a.committer.name", "directory")
                     # .config("spark.sql.sources.commitProtocolClass",
                     #         "org.apache.spark.internal.io.cloud.PathOutputCommitProtocol")
                     # .config("spark.sql.parquet.output.committer.class",
                     #         "org.apache.spark.internal.io.cloud.BindingParquetOutputCommitter")
                     # TODO: Parquet Optimizations
                     # .config("spark.hadoop.parquet.enable.summary-metadata", "false")
                     # .config("spark.sql.parquet.mergeSchema", "false")
                     # .config("spark.sql.parquet.filterPushdown", "true")
                     # .config("spark.sql.hive.metastorePartitionPruning", "true")
                     # Hive
                     # .config("spark.sql.warehouse.dir", "/opt/spark/hive_warehouse")
                     # .config("spark.sql.catalogImplementation", "hive")
                     .getOrCreate()
                     )
    spark_session.sparkContext.setLogLevel(LOG_LEVEL)

    start = datetime.now()
    logger.info(f"Load process started")

    # Use the DataFrameReader interface to read a CSV file
    encounters = (
        spark_session
        .read
        .csv('sample_data/encounters.csv', header=True, schema=ENCOUNTER)
    )

    encounters.show(n=21, truncate=False)

    # counts = (
    #     encounters
    #     .withColumn("CallYear", to_timestamp(col("CallDate"), "MM/dd/yyyy"))
    #     .withColumnRenamed("NumAlarms", "Alarms")
    #     .where((col("CallType") != "Medical Incident") & col("CallDate").isNotNull())
    #     .select('City', spark_year('CallYear').alias('Year'), 'CallType', 'Alarms')
    #     .groupBy('City', 'Year', 'CallType')
    #     .agg(spark_sum('Alarms').alias("TotalAlarms"))
    #     .orderBy("TotalAlarms", ascending=False)
    # )

    # counts.show(n=21, truncate=False)

    # # Save as parquet (filtered for speed)
    # (counts
    #  .limit(21)
    #  .write
    #  .format("parquet")
    #  .mode("overwrite")
    #  .save(output_path)
    #  )

    logger.info(f"Load process finished in {datetime.now() - start}")

    spark_session.stop()


if __name__ == "__main__":

    main()
