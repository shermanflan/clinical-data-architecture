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
from pyspark.sql.types import (
    ArrayType, BooleanType, DateType, DecimalType,
    IntegerType, FloatType, StringType, StructType,
    StructField,
)

from lib import logger, SPARK_LOG_LEVEL
from lib.etl import (
    create_vitals_delta, cache_mpmi, stage_data, load_vitals,
    upsert_vitals, time_travel
)
from lib.schema import ENCOUNTERS, OBSERVATIONS, PATIENTS


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


@cli.command()
@click.option('--filepath', required=False, help='The input file path')
@click.option('--output_path', required=False, help='The output file path')
def examples(filepath: str, output_path: str) -> None:
    """
    """
    start = datetime.now()
    logger.info(f"Load process started")

    # TODO: Implement "Current" tables as delta lake tables (merge/upsert)
    # TODO: Stream (update mode) to PostgreSQL table
    # TODO: Build ETL for raw -> parquet
    # Use the DataFrameReader interface to read a CSV file
    encounters_1 = (
        spark_session
        .read
        # .option("mode", "FAILFAST")  # Exit if any errors
        # .option("nullValue", "")  # Replace any null data with quotes
        .csv('sample_data/output_1/encounters.csv', header=True, schema=ENCOUNTERS)
    )

    # TODO: Read across practices
    # prac_id/section/yyyy/mm/dd/* or
    # section/yyyy/mm/dd/*.parquet (partitioned on prac id)
    vitals_1 = (
        spark_session
        .read
        .csv('sample_data/output_1/observations.csv', header=True, schema=OBSERVATIONS)
        .withColumn("PRAC_ID", lit("output_1"))
    )
    vitals_2 = (
        spark_session
        .read
        .csv('sample_data/output_2/observations.csv', header=True, schema=OBSERVATIONS)
        .withColumn("PRAC_ID", lit("output_2"))
    )

    enc_counts = (
        encounters_1
        # .withColumn("CallYear", to_timestamp(col("CallDate"), "MM/dd/yyyy"))
        .withColumnRenamed("DATE", "encounter_date")
        .where("REASONCODE != '4448140091' AND REASONCODE IS NOT NULL")
        # .where((col("REASONCODE") != "444814009") & col("REASONCODE").isNotNull())
        .select('CODE', spark_year('encounter_date').alias('enc_year'), 'ID')
        .groupBy('CODE', 'enc_year')  # wide transformation
        .agg(count('ID').alias("total_enc"))  # action
        .orderBy(asc("total_enc"))  # wide transformation
        # .orderBy("total_enc", ascending=False)  # wide transformation
    )

    logger.info("Create managed table...")
    # Tables persist after the Spark application terminates, but views disappear.
    # Uses /user/hive/warehouse metastore by default
    # TODO: Read data using DataGrip
    spark_session.sql("CREATE DATABASE clinical_data")
    spark_session.sql("USE clinical_data")
    (
        enc_counts
        .write
        # .option("path", "/tmp/data/us_flights_delay")  # converts to unmanaged
        .saveAsTable("encounter_codes")  # action
    )

    logger.info("Create unmanaged table...")
    spark_session.sql("""
        CREATE TABLE encounters
        (
            ID                  String,
            DATE                Date,
            PATIENT             String,
            CODE                String,
            DESCRIPTION         String,
            REASONCODE          String,
            REASONDESCRIPTION   String
        ) 
        USING csv
        OPTIONS
        (
            PATH 'sample_data/output_1/encounters.csv'
        )
    """)

    logger.info("Calculate BP metrics using views...")
    (
        vitals_1
        .write
        .saveAsTable("vitals_1")  # action
    )
    spark_session.sql("""
        -- Global scope (across sessions)
        CREATE OR REPLACE GLOBAL TEMP VIEW vw_vitals_1
        AS
        SELECT  *
        FROM    vitals_1
    """)
    vitals_2.createOrReplaceTempView("vitals_2")

    # TODO: Join systolic and diastolic
    # TODO: Unnest row to columns: stack
    # https://medium.com/@koushikweblog/pivot-unpivot-data-with-sparksql-pyspark-databricks-89f575f3b3ed
    bp_counts = spark_session.sql("""
        SELECT  PRAC_ID,
                CODE,
                DESCRIPTION,
                COUNT(*) AS total
        FROM    global_temp.vw_vitals_1
        WHERE   CODE IN ('8480-6', '8462-4')
        GROUP BY PRAC_ID, CODE, DESCRIPTION
        UNION ALL
        SELECT  PRAC_ID,
                CODE,
                DESCRIPTION,
                COUNT(*) AS total
        FROM    vitals_2
        WHERE   CODE IN ('8480-6', '8462-4')
        GROUP BY PRAC_ID, CODE, DESCRIPTION
        --ORDER BY DESCRIPTION
    """)

    encounters_1.show(n=21, truncate=False)  # action
    enc_counts.show(n=21, truncate=False)  # action
    # logger.info(vitals_1.printSchema())
    bp_counts.show(n=2100, truncate=False)  # action

    to_path = "output_data/vitals/parquet/{yr}/{mon:02d}/{day:02d}".format(
        yr=date.today().year,
        mon=date.today().month,
        day=date.today().day,
    )
    logger.info(f"Save as parquet to {to_path}...")
    (
        vitals_1.unionAll(vitals_2)
        # .limit(21)  # filtered for speed
        .write
        .parquet(to_path,
                 mode='overwrite',
                 partitionBy=['PRAC_ID'])  # action
    )

    logger.info(f"Save to PostgreSQL...")
    (
        bp_counts
        .write
        .jdbc(url=os.environ['POSTGRES_JDBC_URL'],
              table='bp_counts',
              mode='overwrite')
     )

    # TODO: Stream from csv/parquet directory to PostgreSQL (append, update)
    # TODO: Stream from csv/parquet vitals to groupBy(patient).max(observation)?
    logger.info(f"Load process finished in {datetime.now() - start}")
    input("Press enter to exit...")  # keep alive for Spark UI

    spark_session.stop()


@cli.command()
@click.option('--filepath', required=False, help='The input file path')
@click.option('--output_path', required=False, help='The output file path')
def delta_lake(filepath: str, output_path: str) -> None:
    """
    Delta lake examples
    """
    start = datetime.now()
    logger.info(f"Load process started")

    # TODO: Implement "Current" tables as delta lake tables (merge)
    # Use the DataFrameReader interface to read a CSV file
    encounters_1 = (
        spark_session
        .read
        # .option("mode", "FAILFAST")  # Exit if any errors
        # .option("nullValue", "")  # Replace any null data with quotes
        .csv('sample_data/output_1/encounters.csv', header=True, schema=ENCOUNTERS)
    )

    enc_counts = (
        encounters_1
        # .withColumn("CallYear", to_timestamp(col("CallDate"), "MM/dd/yyyy"))
        .withColumnRenamed("DATE", "encounter_date")
        .where("REASONCODE != '4448140091' AND REASONCODE IS NOT NULL")
        # .where((col("REASONCODE") != "444814009") & col("REASONCODE").isNotNull())
        .select('CODE', spark_year('encounter_date').alias('enc_year'), 'ID')
        .groupBy('CODE', 'enc_year')  # wide transformation
        .agg(count('ID').alias("total_enc"))  # action
        .orderBy(asc("total_enc"))  # wide transformation
        # .orderBy("total_enc", ascending=False)  # wide transformation
    )

    to_path = "output_data/encounters/parquet/{yr}/{mon:02d}/{day:02d}".format(
        yr=date.today().year,
        mon=date.today().month,
        day=date.today().day,
    )
    logger.info(f"Save as parquet to {to_path}...")
    (
        enc_counts
        # .limit(21)  # filtered for speed
        .write
        .parquet(to_path,
                 mode='overwrite',
                 partitionBy=['enc_year'])  # action
    )

    delta_path = "output_data/encounters/delta/{yr}/{mon:02d}/{day:02d}".format(
        yr=date.today().year,
        mon=date.today().month,
        day=date.today().day,
    )
    logger.info(f"Save as delta to {delta_path}...")
    (
        enc_counts
        .write
        .format("delta")
        .mode("append")
        # .option("mergeSchema", "true")
        .save(delta_path)  # action
    )

    encounters_delta = (
        spark_session
        .read
        .format("delta")
        .load(delta_path)
    )
    # encounters_delta.show(n=21, truncate=False)
    encounters_delta.createOrReplaceTempView("encounters_delta")

    spark_session.sql("""
        SELECT  enc_year,
                COUNT(*) AS TOTAL
        FROM    encounters_delta
        GROUP BY enc_year
        ORDER BY enc_year
    """).show(n=21, truncate=False)

    logger.info("Show transaction history...")
    delta_table = DeltaTable.forPath(spark_session, delta_path)
    (
        delta_table
        .history(3)
        .select("version", "timestamp", "operation", "operationParameters")
        .show(truncate=False)
    )

    logger.info("Time travel to prior timestamp")
    (
        spark_session
        .read
        .format("delta")
        .option("timestampAsOf", "2021-10-19 01:22:16.924")  # timestamp after table creation
        .load(delta_path)
        .show()
    )

    logger.info("Time travel to prior version")
    (
        spark_session
        .read
        .format("delta")
        .option("versionAsOf", "1")
        .load(delta_path)
        .show()
    )

    # TODO: Stream (update mode) to PostgreSQL table

    logger.info(f"Load process finished in {datetime.now() - start}")
    input("Press enter to exit...")  # keep alive for Spark UI

    spark_session.stop()


if __name__ == "__main__":

    cli()
