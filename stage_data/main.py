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
    asc, col, concat, count, expr, lit, sum as spark_sum, to_timestamp,
    year as spark_year
)
from pyspark.sql.types import (
    ArrayType, BooleanType, DateType, DecimalType,
    IntegerType, FloatType, StringType, StructType,
    StructField,
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

# DATE,PATIENT,ENCOUNTER,CODE,DESCRIPTION,VALUE,UNITS
VITALS = """
    `DATE`        Date,
    `PATIENT`     String,
    `ENCOUNTER`   String,
    `CODE`        String,
    `DESCRIPTION` String,
    `VALUE`       Decimal(12, 2),
    `UNITS`       String
"""


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
    spark_session = (
        SparkSession
        .builder
        .appName("stage_data")
        .config(f"fs.s3a.bucket.{os.environ['P3_BUCKET']}.access.key", os.environ['P3_AWS_ACCESS_KEY'])
        .config(f"fs.s3a.bucket.{os.environ['P3_BUCKET']}.secret.key", os.environ['P3_AWS_SECRET_KEY'])
        .config("spark.hadoop.fs.s3a.bucket.bangkok.access.key", os.environ['BK_AWS_ACCESS_KEY'])
        .config("spark.hadoop.fs.s3a.bucket.bangkok.secret.key", os.environ['BK_AWS_SECRET_KEY'])
        .config("spark.hadoop.fs.s3a.bucket.condesa.access.key", os.environ['CO_AWS_ACCESS_KEY'])
        .config("spark.hadoop.fs.s3a.bucket.condesa.secret.key", os.environ['CO_AWS_SECRET_KEY'])
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
    encounters_1 = (
        spark_session
        .read
        .csv('sample_data/output_1/encounters.csv', header=True, schema=ENCOUNTER)
    )

    # TODO: Read across practices
    # prac_id/section/yyyy/mm/dd/* or
    # section/yyyy/mm/dd/*.parquet (partitioned on prac id)
    vitals_1 = (
        spark_session
        .read
        .csv('sample_data/output_1/observations.csv', header=True, schema=VITALS)
        .withColumn("PRAC_ID", lit("output_1"))
    )
    vitals_2 = (
        spark_session
        .read
        .csv('sample_data/output_2/observations.csv', header=True, schema=VITALS)
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

    # Calculate BP metrics
    vitals_1.createOrReplaceTempView("vitals_1")
    vitals_2.createOrReplaceTempView("vitals_2")
    bp_counts = spark_session.sql("""
        SELECT  PRAC_ID,
                CODE,
                DESCRIPTION,
                COUNT(*) AS total
        FROM    vitals_1
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
        vitals_1
        .unionAll(vitals_2)
        # .limit(21)  # filtered for speed
        .write
        .parquet(to_path,
                 mode='overwrite',
                 partitionBy=['PRAC_ID'])  # action
        # .saveAsTable('bp_counts')  # action
    )

    logger.info(f"Load process finished in {datetime.now() - start}")
    input("Press enter to exit...")  # keep alive for Spark UI

    spark_session.stop()


if __name__ == "__main__":

    main()
