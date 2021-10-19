from datetime import datetime, date, timedelta

from pyspark.sql.functions import (
    asc, col, concat, count, expr, lit,
    sum, to_timestamp, to_date, year
)

from lib import logger
from lib.path import get_lake_path
from lib.schema import ENCOUNTERS, OBSERVATIONS, VITALS


def load_vitals(session, input_path) -> None:
    """

    :return:
    """
    logger.info(f"Read vitals")
    raw = (
        session
        .read
        .csv(input_path,
             sep='|', header=False, schema=VITALS)
    )

    # raw.show(n=21, truncate=False)

    # TODO: Exception handling, need a try_cast UDF?
    # logger.info(f"Try cast date/time")
    # conformed = (
    #     raw
    #     .withColumn('observation_date', to_date(vitals.service_date, 'yyyyMMdd'))
    #     .withColumn('observation_datetime',
    #                 to_timestamp(
    #                     concat(vitals.service_date, lit(' '), vitals.service_time),
    #                     'yyyyMMdd HH:mm:ss'))
    # )

    # (
    #     conformed
    #     .select(conformed.service_date,
    #             conformed.service_time,
    #             'observation_date',
    #             col('observation_datetime'))
    #     .drop('height_cm', 'height_comment', 'weight_kg', 'weight_comment',
    #           'temperature_c', 'temperature_f', 'temperature_comment',
    #           'pulse', 'pulse_comment', 'respiration', 'respiration_comment',
    #           'bp_comment', 'oxygen_saturation', 'oxygen_comment', 'bmi_comment',
    #           'clinician', 'comment', 'client_vitals_id')
    #     .show(n=21, truncate=False)
    # )

    logger.info(f"Conform vitals")
    raw.createOrReplaceTempView("raw_vitals")

    # TODO: Load MPMI as a DataFrame and use a broadcast join
    # TODO: Cache/Persist MPMI to memory, disk
    # TODO: De-duplicate by comparing row hash
    conformed = session.sql("""
        SELECT  client_id,
                patient_id,
                encounter_id,
                --height_in,
                --weight_lbs,
                --bp_systolic,
                --bp_diastolic,
                --bmi,
                STACK(5,
                      'Height', '8302-2', height_in, '[in_i]',
                      'Body Weight', '29463-7', weight_lbs, '[lb_av]', 
                      'BP Systolic', '8480-6', bp_systolic, 'mm[Hg]',
                      'BP Diastolic', '8462-4', bp_diastolic, 'mm[Hg]',
                      'BMI (Body Mass Index)', '39156-5', bmi, 'kg/m2'
                ) AS (name, code, value, unit),
                'LOINC' as code_system_name,
                '2.16.840.1.113883.6.1' as code_system_oid,
                --hj_create_timestamp,
                --hj_modify_timestamp,
                --service_date,
                --service_time,
                IF (service_time IS NOT NULL,
                    to_timestamp(concat(service_date, ' ', service_time),
                                'yyyyMMdd HH:mm:ss'),
                    to_timestamp(service_date, 'yyyyMMdd')
                ) AS observation_date,
                row_hash
        FROM    raw_vitals
    """)

    (
        conformed
        .filter(conformed.value.isNotNull())
        # .filter('value IS NOT NULL')
        .show(n=21, truncate=False)
    )


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
