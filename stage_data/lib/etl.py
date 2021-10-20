import os
from datetime import datetime, date, timedelta

from delta.tables import DeltaTable
from pyspark.sql.functions import (
    asc, col, concat, count, expr, lit,
    sum, to_timestamp, to_date, year
)

from lib import logger
from lib.path import get_lake_path
from lib.schema import ENCOUNTERS, OBSERVATIONS, VITALS, CQ_VITALS


def load_vitals(session, input_path: str, output_path: str) -> str:
    """

    :return: the path to the delta table
    """
    logger.info(f"Read vitals: {input_path}")
    raw = (
        session
        .read
        .csv(input_path,
             sep='|', header=False, schema=VITALS)
    )

    # raw.show(n=21, truncate=False)

    stage_path = (
        "{root}/stage/vitals/parquet/{Y}/{M:02d}/{D:02d}"
        .format(root=output_path,
                Y=date.today().year,
                M=date.today().month,
                D=date.today().day)
    )

    logger.info(f"Stage vitals, prac partitioned: {stage_path}")
    (
        raw
        .write
        .parquet(stage_path,
                 mode='overwrite',
                 partitionBy=['client_id'])  # action
    )

    logger.info(f"Read stage, prac partitioned: {stage_path}")
    stage = (
        session
        .read
        .parquet(stage_path)
    )

    # stage.show(n=21, truncate=False)

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

    logger.info("Load mc_practice_master_info and cache to memory")
    mpmi = (
        session
        .read
        .jdbc(url=os.environ['POSTGRES_JDBC_URL'],
              table='public.mc_practice_master_info')
        .filter('enabled = true AND ale_prac_id IS NOT NULL')
        # .filter(col('ale_prac_id').isNotNull())
        .select('document_oid', 'ale_prac_id')
        .distinct()
        .cache()
    )
    mpmi.createOrReplaceTempView("mpmi")
    # mpmi.show(n=21)

    logger.info(f"Conform vitals")
    stage.createOrReplaceTempView("stage_vitals")

    # TODO: De-duplicate by comparing row hash against delta cq
    conformed = session.sql("""
        SELECT  v.client_id,
                m.ale_prac_id AS source_ale_prac_id,
                v.encounter_id,
                v.patient_id,
                --height_in,
                --weight_lbs,
                --bp_systolic,
                --bp_diastolic,
                --bmi,
                STACK(5,
                      'Height', '8302-2', v.height_in, '[in_i]',
                      'Body Weight', '29463-7', v.weight_lbs, '[lb_av]', 
                      'BP Systolic', '8480-6', v.bp_systolic, 'mm[Hg]',
                      'BP Diastolic', '8462-4', v.bp_diastolic, 'mm[Hg]',
                      'BMI (Body Mass Index)', '39156-5', v.bmi, 'kg/m2'
                ) AS (name, code, value, unit),
                'LOINC' as code_system_name,
                '2.16.840.1.113883.6.1' as code_system_oid,
                --hj_create_timestamp,
                --hj_modify_timestamp,
                --service_date,
                --service_time,
                IF (v.service_time IS NOT NULL,
                    to_timestamp(concat(v.service_date, ' ', v.service_time),
                                'yyyyMMdd HH:mm:ss'),
                    to_timestamp(v.service_date, 'yyyyMMdd')
                ) AS observation_date,
                v.row_hash
        FROM    stage_vitals AS v
            INNER JOIN mpmi AS m
                ON v.client_id = m.document_oid
    """)

    # conformed.show(n=21, truncate=False)

    delta_path = "{root}/public/vitals/delta".format(root=output_path)

    # TODO: Upsert to delta
    # TODO: Create from schema
    logger.info(f"Publish vitals delta: {delta_path}")
    # TODO: Patient match, load demographics cached?
    # TODO: Store demographic matches as delta, partitioned by
    # client_id, patient_id%8?
    (
        conformed
        .filter(conformed.value.isNotNull())
        .select('client_id',
                'source_ale_prac_id',
                'encounter_id',
                'patient_id',
                'name',
                'code',
                'code_system_name',
                'code_system_oid',
                'value',
                'unit',
                'observation_date',
                col('row_hash').alias('source_guid'))
        .write
        .partitionBy('source_ale_prac_id')
        .format('delta')
        # TODO: What other modes?
        .mode("append")
        # .option("mergeSchema", "true")
        .save(delta_path)
    )

    # logger.info(f"Read vitals delta: {delta_path}")
    # delta = (
    #     session
    #     .read
    #     .format("delta")
    #     .load(delta_path)
    # )
    # delta.createOrReplaceTempView("delta_vitals")

    # (
    #     session.sql("""
    #         SELECT  source_ale_prac_id,
    #                 COUNT(*)
    #         FROM    delta_vitals
    #         GROUP BY source_ale_prac_id
    #         ORDER BY source_ale_prac_id
    #     """)
    # ).show(n=21, truncate=False)

    return delta_path


def upsert_vitals(session, input_path: str, output_path: str) -> str:
    """

    :return: the path to the delta table
    """
    logger.info(f"Read vitals: {input_path}")
    raw = (
        session
        .read
        .csv(input_path,
             sep='|', header=False, schema=VITALS)
    )
    # raw.show(n=21, truncate=False)
    # return

    stage_path = (
        "{root}/stage/vitals/parquet/{Y}/{M:02d}/{D:02d}"
        .format(root=output_path,
                Y=date.today().year,
                M=date.today().month,
                D=date.today().day)
    )

    logger.info(f"Stage vitals, prac partitioned: {stage_path}")
    (
        raw
        .write
        .parquet(stage_path,
                 mode='overwrite',
                 partitionBy=['client_id'])  # action
    )

    logger.info(f"Read stage, prac partitioned: {stage_path}")
    stage = (
        session
        .read
        .parquet(stage_path)
    )

    logger.info("Load mc_practice_master_info and cache to memory")
    mpmi = (
        session
        .read
        .jdbc(url=os.environ['POSTGRES_JDBC_URL'],
              table='public.mc_practice_master_info')
        .filter('enabled = true AND ale_prac_id IS NOT NULL')
        .select('document_oid', 'ale_prac_id')
        .distinct()
        .cache()
    )
    mpmi.createOrReplaceTempView("mpmi")

    logger.info(f"Conform vitals")
    stage.createOrReplaceTempView("stage_vitals")

    conformed = session.sql("""
        SELECT  v.client_id,
                m.ale_prac_id AS source_ale_prac_id,
                v.encounter_id,
                v.patient_id,
                STACK(5,
                      'Height', '8302-2', v.height_in, '[in_i]',
                      'Body Weight', '29463-7', v.weight_lbs, '[lb_av]', 
                      'BP Systolic', '8480-6', v.bp_systolic, 'mm[Hg]',
                      'BP Diastolic', '8462-4', v.bp_diastolic, 'mm[Hg]',
                      'BMI (Body Mass Index)', '39156-5', v.bmi, 'kg/m2'
                ) AS (name, code, value, unit),
                'LOINC' as code_system_name,
                '2.16.840.1.113883.6.1' as code_system_oid,
                v.hj_modify_timestamp,
                v.observation_date,
                v.source_guid
                -- v.rn
        FROM    (
            SELECT  client_id,
                    encounter_id,
                    patient_id,
                    height_in,
                    weight_lbs,
                    bp_systolic,
                    bp_diastolic,
                    bmi,
                    to_timestamp(hj_create_timestamp,
                                 'yyyyMMdd HH:mm:ss:SSSSSS'
                    ) AS hj_modify_timestamp,
                    IF (service_time IS NOT NULL,
                        to_timestamp(concat(service_date, ' ', service_time),
                                    'yyyyMMdd HH:mm:ss'),
                        to_timestamp(service_date, 'yyyyMMdd')
                    ) AS observation_date,
                    row_hash AS source_guid,
                    ROW_NUMBER() OVER (
                        PARTITION BY client_id,
                                     encounter_id, 
                                     patient_id
                        ORDER BY 
                            IF (service_time IS NOT NULL,
                                to_timestamp(
                                    concat(service_date, ' ', service_time),
                                    'yyyyMMdd HH:mm:ss'),
                                to_timestamp(service_date, 'yyyyMMdd')) DESC
                    ) AS rn                    
            FROM    stage_vitals
        ) AS v
            INNER JOIN mpmi AS m
                ON v.client_id = m.document_oid
        WHERE   v.rn = 1
    """)
    conformed.createOrReplaceTempView("conformed_vitals")

    delta_path = "{root}/public/vitals/delta".format(root=output_path)

    logger.info(f"Publish vitals delta: {delta_path}")
    (
        DeltaTable
        .forPath(session, delta_path).alias('tgt')
        # .merge(conformed.filter('rn = 1').alias('src'),
        .merge(conformed.alias('src'), """
               tgt.client_id = src.client_id
               AND tgt.encounter_id = src.encounter_id
               AND tgt.patient_id = src.patient_id
               AND tgt.code = src.code
               AND tgt.source_guid != src.source_guid
               --AND tgt.hj_modify_timestamp < src.hj_modify_timestamp
        """)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    # TODO: Create from schema
    # TODO: Patient match, load demographics cached?
    # TODO: Store demographic matches as delta, partitioned by
    # client_id, patient_id%8?
    # (
    #     conformed
    #     .filter(conformed.value.isNotNull())
    #     .select('client_id',
    #             'source_ale_prac_id',
    #             'encounter_id',
    #             'patient_id',
    #             'name',
    #             'code',
    #             'code_system_name',
    #             'code_system_oid',
    #             'value',
    #             'unit',
    #             'observation_date',
    #             col('row_hash').alias('source_guid'))
    #     .write
    #     .partitionBy('source_ale_prac_id')
    #     .format('delta')
    #     .mode("append")
    #     .option("mergeSchema", "true")
    #     .save(delta_path)
    # )

    # logger.info(f"Read vitals delta: {delta_path}")
    # (
    #     session
    #     .read
    #     .format("delta")
    #     .load(delta_path)  # To DataFrame
    #     .createOrReplaceTempView("delta_vitals")
    # )

    # (
    #     session.sql("""
    #         SELECT  source_ale_prac_id,
    #                 COUNT(*)
    #         FROM    delta_vitals
    #         GROUP BY source_ale_prac_id
    #         ORDER BY source_ale_prac_id
    #     """)
    # ).show(n=21, truncate=False)

    return delta_path


def time_travel(session, delta_path) -> None:

    logger.info(f"Transaction history: {delta_path}")
    (
        DeltaTable
        .forPath(session, delta_path)
        .history(21)
        .select(
            "version",
            "timestamp",
            "operation",
            "operationParameters",
            "operationMetrics")
        .show(truncate=False)
    )

    logger.info(f"Current delta: {delta_path}")
    (
        session
        .read
        .format("delta")
        .load(delta_path)  # As DataFrame
        .createOrReplaceTempView("delta_vitals_current")
    )
    (
        session.sql("""
            SELECT  --*
                    source_ale_prac_id,
                    COUNT(*)
            FROM    delta_vitals_current
            GROUP BY source_ale_prac_id
            ORDER BY source_ale_prac_id
        """)
    ).show(n=21, truncate=False)

    logger.info(f"Time travel to version 0: {delta_path}")
    (
        session
        .read
        .format("delta")
        .option("versionAsOf", "0")
        .load(delta_path)  # As DataFrame
        .createOrReplaceTempView("delta_vitals_0")
    )

    (
        session.sql(f"""
            SELECT  source_ale_prac_id,
                    COUNT(*)
            FROM    delta_vitals_0 
            GROUP BY source_ale_prac_id
            ORDER BY source_ale_prac_id
        """)
    ).show(n=21, truncate=False)


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
