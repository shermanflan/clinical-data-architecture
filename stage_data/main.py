"""
Usage:
- From docker interactive against bitnami docker-compose cluster:
docker run --rm -it --name test_pyspark --network container:spark_ingest_spark_1 spark-ingest:latest /bin/bash
- From Spark 3.1.1 base container with Python bindings:
docker run --rm -it --name test_pyspark spark-ingest:latest /bin/bash
./bin/spark-submit spark-ingest/main.py --filepath ./examples/src/main/python/pi.py
"""
from datetime import datetime, date, timedelta
import os

import boto3
import click
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    asc, col, concat, count, expr, lit,
    sum as spark_sum, to_timestamp,
    year as spark_year
)

from lib import logger, SPARK_LOG_LEVEL
from lib.etl import (
    create_vitals_delta, cache_mpmi, stage_data, load_vitals,
    upsert_vitals, time_travel
)


"""
To configure AWS bucket-specific authorization, use the
`fs.s3a.bucket.[bucket name].access.key` configuration setting.

As specified here:
- https://hadoop.apache.org/docs/current2/hadoop-aws/tools/hadoop-aws/index.html#Configuring_different_S3_buckets

TODO: Consider optimizing the S3A for I/O.
- https://spark.apache.org/docs/3.1.1/cloud-integration.html#recommended-settings-for-writing-to-object-stores
"""
spark_session = (
    SparkSession
    .builder
    .appName("stage_data")
    # AWS bucket-specific authorization
    # .config(f"fs.s3a.bucket.{os.environ['P3_BUCKET']}.access.key", os.environ['P3_AWS_ACCESS_KEY'])
    # .config(f"fs.s3a.bucket.{os.environ['P3_BUCKET']}.secret.key", os.environ['P3_AWS_SECRET_KEY'])
    # .config("spark.hadoop.fs.s3a.bucket.bangkok.access.key", os.environ['BK_AWS_ACCESS_KEY'])
    # .config("spark.hadoop.fs.s3a.bucket.bangkok.secret.key", os.environ['BK_AWS_SECRET_KEY'])
    # .config("spark.hadoop.fs.s3a.bucket.condesa.access.key", os.environ['CO_AWS_ACCESS_KEY'])
    # .config("spark.hadoop.fs.s3a.bucket.condesa.secret.key", os.environ['CO_AWS_SECRET_KEY'])
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
    # Specify different location for Hive metastore
    # .config("spark.sql.warehouse.dir", "/opt/spark/hive_warehouse")
    # .config("spark.sql.catalogImplementation", "hive")
    # TODO: Delta lake
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)
spark_session.sparkContext.setLogLevel(SPARK_LOG_LEVEL)


@click.group()
def cli():
    pass


@cli.command()
@click.option('--filepath', required=False, help='The input file path')
@click.option('--filepath2', required=False, help='The input file path')
@click.option('--output_path', required=False, help='The output file path')
def acquire_vitals(filepath: str, filepath2: str, output_path: str) -> None:
    """
    """
    # TODO: Implement "Current" tables as delta lake tables (merge/upsert)
    # TODO: Stream (update mode) to PostgreSQL table
    # TODO: Stream from csv/parquet directory to PostgreSQL (append, update)
    # TODO: Stream from csv/parquet vitals to groupBy(patient).max(observation)?
    # TODO: Implement "Current" tables as delta lake tables (merge)
    # TODO: Stream (update mode) to PostgreSQL table

    start = datetime.now()

    logger.info(f"Creating vitals delta: {output_path}")
    delta_path = create_vitals_delta(spark_session, output_path)
    logger.info(f"Create finished in {datetime.now() - start}")

    logger.info(f"Caching mpmi: {output_path}")
    cache_mpmi(spark_session)
    logger.info(f"Cache finished in {datetime.now() - start}")

    logger.info(f"Processing vitals: {filepath}")
    load_vitals(spark_session, filepath, output_path)
    logger.info(f"Load process finished in {datetime.now() - start}")

    logger.info(f"Processing vitals: {filepath2}")
    upsert_vitals(spark_session, filepath2, output_path)
    logger.info(f"Upsert process finished in {datetime.now() - start}")

    logger.info(f"Time-travel vitals: {delta_path}")
    time_travel(
        spark_session,
        delta_path
    )
    logger.info(f"Time-travel finished in {datetime.now() - start}")

    input("Press enter to exit...")  # keep alive for Spark UI


if __name__ == "__main__":

    cli()
