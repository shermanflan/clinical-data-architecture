from datetime import datetime, date, timedelta
import os
from os.path import basename

from delta.tables import DeltaTable
from pyspark.sql.functions import (
    asc, col, concat, count, countDistinct, expr,
    lit, sum, to_timestamp, to_date, year
)
from pyspark.sql.types import (
    ArrayType, BooleanType, DateType, DecimalType,
    IntegerType, FloatType, LongType, StringType,
    StructType, StructField, TimestampType
)

from lib import logger
from lib.path import get_lake_path
from lib.schema import VITALS, CQ_VITALS


def create_vitals_delta(session, delta_root: str) -> str:
    """

    :return: the path to the delta table
    """
    delta_path = "{root}/public/vitals/delta".format(root=delta_root)

    # TODO: Add constraints (update trigger, sequence) via generated columns
    (
        DeltaTable
        .createIfNotExists(session)
        .addColumn("client_id", StringType())
        .addColumn("source_ale_prac_id", "INT", comment="Source prac")
        .addColumn("encounter_id", StringType())
        .addColumn("patient_id", "STRING", comment="Source pt id")
        .addColumn("name", StringType())
        .addColumn("code", StringType())
        .addColumn("code_system_name", StringType())
        .addColumn("code_system_oid", StringType())
        .addColumn("value", StringType())
        .addColumn("unit", StringType())
        .addColumn("observation_date", TimestampType())
        .addColumn("source_guid", StringType(), comment="Traceability")
        # .addColumn("created_at",
        #            TimestampType(),
        #            generatedAlwaysAs="current_timestamp()")
        # .addColumn("updated_at",
        #            TimestampType(),
        #            generatedAlwaysAs="current_timestamp()")
        .property("description", "published vitals")
        .location(delta_path)
        .partitionedBy("source_ale_prac_id")
        .execute()
    )

    (
        session
        .read
        .format("delta")
        .load(delta_path)  # As DataFrame
        .select('source_ale_prac_id', 'patient_id')
        .groupBy('source_ale_prac_id')
        .agg(countDistinct('patient_id').alias("total_pt"))  # action
        .orderBy("total_pt", ascending=False)  # wide transformation
        .show()
    )
    return delta_path


def cache_mpmi(session) -> None:
    """
    """
    logger.info("Load mc_practice_master_info and cache to memory")
    (
        session
        .read
        .jdbc(url=os.environ['POSTGRES_JDBC_URL'],
              table='public.mc_practice_master_info')
        .filter('enabled = true AND ale_prac_id IS NOT NULL')
        .select('document_oid', 'ale_prac_id')
        .distinct()
        .cache()
        .createOrReplaceTempView("mpmi")
    )


def stage_data(session, input_path: str, output_path: str) -> None:
    """
    """
    logger.info(f"Read vitals: {input_path}")
    raw = (
        session
        .read
        # .option("mode", "FAILFAST")  # Exit if any errors
        # .option("nullValue", "")  # Replace any null data with quotes
        .csv(input_path,
             sep='|', header=False, schema=VITALS)
        # .withColumn("source", lit(basename(input_path)))
    )

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
    (
        session
        .read
        .parquet(stage_path)
        .createOrReplaceTempView("stage_vitals")
    )


def load_vitals(session, input_path: str, output_path: str) -> None:
    """
    """
    stage_data(session, input_path, output_path)

    # TODO: De-duplicate by comparing row hash against delta cq
    logger.info(f"Conform vitals")
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
                IF (v.service_time IS NOT NULL,
                    to_timestamp(concat(v.service_date, ' ', v.service_time),
                                'yyyyMMdd HH:mm:ss'),
                    to_timestamp(v.service_date, 'yyyyMMdd')
                ) AS observation_date,
                v.row_hash AS source_guid
        FROM    stage_vitals AS v
            INNER JOIN mpmi AS m
                ON v.client_id = m.document_oid
    """)

    delta_path = "{root}/public/vitals/delta".format(root=output_path)

    logger.info(f"Publish vitals delta: {delta_path}")
    # TODO: Patient match, load demographics cached?
    # TODO: Store demographic matches as delta, partitioned by client_id?
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
                'source_guid')
        .write
        # .partitionBy('source_ale_prac_id')
        .format('delta')
        .mode("append")
        # .option("mergeSchema", "true")
        .save(delta_path)
    )

    # logger.info(f"Read vitals delta: {delta_path}")
    # (
    #     session
    #     .read
    #     .format("delta")
    #     .load(delta_path)  # As DataFrame
    #     .groupBy('source_ale_prac_id')
    #     .count()
    #     .show(n=21, truncate=False)
    # )


def upsert_vitals(session, input_path: str, output_path: str) -> None:
    """
    """
    stage_data(session, input_path, output_path)

    logger.info(f"Conform vitals")
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
                        PARTITION BY client_id, encounter_id, patient_id
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

    # TODO: Patient match, load demographics cached?
    # TODO: Store demographic matches as delta, partitioned by
    delta_path = "{root}/public/vitals/delta".format(root=output_path)

    logger.info(f"Publish vitals delta: {delta_path}")
    (
        DeltaTable
        .forPath(session, delta_path).alias('tgt')
        .merge(conformed.alias('src'), """
               tgt.source_ale_prac_id = src.source_ale_prac_id
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

    # logger.info(f"Read vitals delta: {delta_path}")
    # (
    #     session
    #     .read
    #     .format("delta")
    #     .load(delta_path)  # To DataFrame
    #     .groupBy('source_ale_prac_id')
    #     .count()
    #     .show(n=21)
    # )


def time_travel(session, delta_path: str) -> None:
    """
    """
    logger.info(f"Transaction history: {delta_path}")
    (
        DeltaTable
        .forPath(session, delta_path)
        .history(21)
        .select(
            "version",
            "timestamp",
            "operation",
            # "operationParameters",
            # "operationMetrics"
        )
        .show(truncate=False)
    )

    logger.info(f"Current delta: {delta_path}")
    (
        session
        .read
        .format("delta")
        .load(delta_path)  # As DataFrame
        .groupBy('source_ale_prac_id')
        .count()
        .orderBy('source_ale_prac_id')
        .show(n=21)
    )

    logger.info(f"Time travel to version 1: {delta_path}")
    (
        session
        .read
        .format("delta")
        .option("versionAsOf", "1")
        .load(delta_path)  # As DataFrame
        .groupBy('source_ale_prac_id')
        .count()
        .orderBy('source_ale_prac_id')
        .show(n=21)
    )
