from datetime import datetime, date, timedelta

from pyspark.sql.functions import (
    asc, col, concat, count, expr, lit,
    sum as spark_sum, to_timestamp,
    year as spark_year
)

from lib import logger
from lib.path import get_lake_path
from lib.schema import ENCOUNTERS, OBSERVATIONS, PATIENTS


def stage_data(session) -> None:
    """

    :return:
    """
    encounters_1 = (
        session
        .read
        # .option("mode", "FAILFAST")  # Exit if any errors
        # .option("nullValue", "")  # Replace any null data with quotes
        .csv('sample_data/output_1/encounters.csv',
             header=True,
             schema=ENCOUNTERS)
        .withColumn("PRAC_ID", lit("output_1"))
    )

    vitals_1 = (
        session
        .read
        .csv('sample_data/output_1/observations.csv',
             header=True,
             schema=OBSERVATIONS)
        .withColumn("PRAC_ID", lit("output_1"))
    )

    to_path = get_lake_path().format(
        stage='staging',
        section='encounters',
        yr=date.today().year,
        mon=date.today().month,
        day=date.today().day,
    )

    logger.info(f"Save as parquet to {to_path}...")
    (
        encounters_1
        .write
        .parquet(to_path,
                 mode='overwrite',
                 partitionBy=['PRAC_ID'])  # action
    )

    to_path = get_lake_path().format(
        stage='staging',
        section='vitals',
        yr=date.today().year,
        mon=date.today().month,
        day=date.today().day,
    )

    logger.info(f"Save as parquet to {to_path}...")
    (
        vitals_1
        .write
        .parquet(to_path,
                 mode='overwrite',
                 partitionBy=['PRAC_ID'])  # action
    )
