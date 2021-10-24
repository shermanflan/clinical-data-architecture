"""
Usage:
- From Spark 3.1.1 base container with Python bindings:
docker run --rm -it --name test_pyspark spark-ingest:latest /bin/bash
./bin/spark-submit spark-ingest/main.py --filepath ./examples/src/main/python/pi.py
- From binaries:
./pyspark --packages io.delta:delta-core_2.12:1.0.0 \
    --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
    --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
./spark-sql --packages io.delta:delta-core_2.12:1.0.0 \
    --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
    --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
"""
from datetime import datetime, date, timedelta
import os

import boto3
import click
from pyspark.sql import SparkSession

from lib import logger, SPARK_LOG_LEVEL
from lib.etl import (
    create_vitals_delta, cache_mpmi, load_vitals,
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
    # .config(f"fs.s3a.bucket.{os.environ['P3_BUCKET']}.session.token", os.environ['P3_AWS_SESSION_TOKEN'])
    # Or
    # .config(f"spark.hadoop.fs.s3a.bucket.{os.environ['P3_BUCKET']}.access.key", os.environ['P3_AWS_ACCESS_KEY'])
    # .config(f"spark.hadoop.fs.s3a.bucket.{os.environ['P3_BUCKET']}.secret.key", os.environ['P3_AWS_SECRET_KEY'])
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
    # Delta lake integration with Spark DataSourceV2 and Catalog
    # .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    # .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)
spark_session.sparkContext.setLogLevel(SPARK_LOG_LEVEL)


@click.group()
def cli():
    pass


@cli.command()
@click.option('--filepath', required=False, help='The input file path')
@click.option('--filepath2', required=False, help='The input file path')
@click.option('--output-path', required=False, help='The output file path')
def acquire_vitals(filepath: str, filepath2: str, output_path: str) -> None:
    """
    """
    # TODO: Build Spark 3.2 container with Python bindings
    # TODO: RE: patient matches, load demographics as a Delta and keep sync'd
    # TODO: Partition demographics Delta by prac
    # TODO: Implement "Current" tables as delta lake tables (merge/upsert)
    start = datetime.now()

    # logger.info(f"Creating vitals delta: {output_path}")
    # delta_path = create_vitals_delta(spark_session, output_path)
    delta_path = "{root}/public/vitals/delta".format(root=output_path)
    # logger.info(f"Create finished in {datetime.now() - start}")

    logger.info(f"Caching mpmi: {output_path}")
    mpmi = cache_mpmi(spark_session)
    logger.info(f"Cache finished in {datetime.now() - start}")

    logger.info(f"Processing vitals: {filepath}")
    load_vitals(spark_session, mpmi, filepath, output_path)
    logger.info(f"Load process finished in {datetime.now() - start}")

    logger.info(f"Processing vitals: {filepath2}")
    upsert_vitals(spark_session, mpmi, filepath2, output_path)
    logger.info(f"Upsert process finished in {datetime.now() - start}")

    logger.info(f"Time-travel vitals: {delta_path}")
    time_travel(
        spark_session,
        delta_path
    )
    logger.info(f"Time-travel finished in {datetime.now() - start}")

    input("Press enter to exit...")  # keep alive for Spark UI


@cli.command()
@click.option('--source-path', required=False, help='The Delta path')
@click.option('--output-path', required=False, help='The output file path')
def stream_vitals(source_path: str, output_path: str) -> None:
    """
    JDBC streaming is not supported so I'm not sure how to use this.
    It may be that Kafka is necessary.
    """
    logger.info(f"Stream (append mode) to delta on: {source_path}")
    (
        spark_session
        .readStream
        .format("delta")
        # .option("ignoreDeletes", "true")
        # .option("ignoreChanges", "true")
        .load(source_path)
        .writeStream
        # .format("console")  # debug
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{output_path}/_checkpoints/stream-from-delta")
        .queryName('vitals_stream')
        .start(output_path)
        .awaitTermination(timeout=60*5)  # 5 min
    )


if __name__ == "__main__":

    cli()
