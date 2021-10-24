from datetime import datetime, date, timedelta
import os
from os.path import basename

from delta.tables import DeltaTable
from pyspark.sql.functions import (
    asc, col, concat, count, countDistinct, expr,
    DataFrame, lit, sum, to_timestamp, to_date, year
)
from pyspark.sql.types import (
    ArrayType, BooleanType, DateType, DecimalType,
    IntegerType, FloatType, LongType, StringType,
    StructType, StructField, TimestampType
)

from lib import logger, ETL_DATE
from lib.path import get_lake_path
from lib.schema import VITALS, CQ_VITALS


def create_vitals_delta(session, delta_root: str) -> str:
    """
    The `partitionedBy` routine is not creating the partition metadata.
    For now, the Delta table will be created inline during initial
    staging.
    TODO: Try converting to Spark SQL DDL.

    :return: the path to the delta table
    """
    delta_path = "{root}/public/vitals/delta".format(root=delta_root)

    (
        DeltaTable
        .createIfNotExists(session)
        .tableName("delta_vitals")
        .addColumn("client_id", StringType())
        .addColumn("source_ale_prac_id", "INT",
                   comment="Source practice id")
        .addColumn("encounter_id", StringType())
        .addColumn("patient_id", "STRING", comment="Source patient id")
        .addColumn("name", StringType())
        .addColumn("code", StringType())
        .addColumn("code_system_name", StringType())
        .addColumn("code_system_oid", StringType())
        .addColumn("value", StringType())
        .addColumn("unit", StringType())
        .addColumn("observation_date", TimestampType())
        .addColumn("source_guid", StringType(), comment="Traceability")
        .addColumn("source", StringType(), comment="Traceability")
        .addColumn("created_at", TimestampType(), nullable=False)
        .addColumn("updated_at", TimestampType(), nullable=False)
        .property("description", "published vitals")
        .partitionedBy("source_ale_prac_id")
        .location(delta_path)
        .execute()
    )

    # (
    #     session
    #     .read
    #     .format("delta")
    #     .load(delta_path)  # As DataFrame
    #     .select('source_ale_prac_id', 'patient_id')
    #     .groupBy('source_ale_prac_id')
    #     .agg(countDistinct('patient_id').alias("total_pt"))  # action
    #     .orderBy("total_pt", ascending=False)  # wide transformation
    #     .show()
    # )

    return delta_path


def cache_mpmi(session) -> DataFrame:
    """
    """
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
    return mpmi


def stage_data(session, mpmi: DataFrame, input_path: str, output_path: str) -> None:
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
        .drop('height_cm', 'height_comment', 'weight_kg', 'weight_comment',
              'temperature_c', 'temperature_f', 'temperature_comment',
              'pulse', 'pulse_comment', 'respiration', 'respiration_comment',
              'bp_comment', 'oxygen_saturation', 'oxygen_comment',
              'bmi_comment', 'clinician', 'comment', 'client_vitals_id')
        .withColumn("source", lit(basename(input_path)))
        .withColumn("created_at", to_timestamp(lit(ETL_DATE), "yyyy-MM-dd HH:mm:ss"))
        .withColumn("updated_at", to_timestamp(lit(ETL_DATE), "yyyy-MM-dd HH:mm:ss"))
        .alias('s')
        .join(mpmi.alias('m'), col("s.client_id") == col("m.document_oid"))
        .drop('document_oid')
        .withColumnRenamed('ale_prac_id', 'source_ale_prac_id')
    )

    stage_path = (
        "{root}/stage/vitals/parquet/{Y}/{M:02d}/{D:02d}"
        .format(root=output_path,
                Y=date.today().year,
                M=date.today().month,
                D=date.today().day)
    )

    logger.info(f"Stage vitals, partitioned by practice: {stage_path}")
    (
        raw
        .write
        .parquet(stage_path,
                 mode='overwrite',
                 partitionBy=['source_ale_prac_id'])  # action
    )

    logger.info(f"Create stage temp view: {stage_path}")
    (
        session
        .read
        .parquet(stage_path)
        .createOrReplaceTempView("stage_vitals")
    )


def load_vitals(session, mpmi: DataFrame, input_path: str, output_path: str) -> None:
    """
    Initial load which creates the partitioned Delta table.
    """
    stage_data(session, mpmi, input_path, output_path)

    logger.info(f"Conform vitals (load)")
    conformed = session.sql("""
        SELECT  v.client_id,
                v.source_ale_prac_id,
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
                v.row_hash AS source_guid,
                v.source,
                v.created_at,
                v.updated_at
        FROM    stage_vitals AS v
    """)

    delta_path = "{root}/public/vitals/delta".format(root=output_path)

    logger.info(f"Create vitals delta: {delta_path}")
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
                'source_guid',
                'source',
                'created_at',
                'updated_at')
        .write
        .partitionBy('source_ale_prac_id')
        .format('delta')
        .mode("append")
        # .mode("overwrite")
        .save(delta_path)
        # Cleaner option, but throws errors when re-running
        # .partitionBy('source_ale_prac_id')
        # .option('path', delta_path)
        # .saveAsTable('delta_vitals', format='delta', mode='append')
    )

    # logger.info(f"Read vitals delta: {delta_path}")
    # (
    #     session
    #     .read
    #     .format("delta")
    #     .load(delta_path)  # As DataFrame
    #     .groupBy('source_ale_prac_id')
    #     .agg(count("*"))
    #     .orderBy('source_ale_prac_id')
    #     .show(n=21, truncate=False)
    # )


def upsert_vitals(session, mpmi: DataFrame, input_path: str, output_path: str) -> None:
    """
    """
    stage_data(session, mpmi, input_path, output_path)

    delta_path = "{root}/public/vitals/delta".format(root=output_path)

    logger.info(f"Conform vitals (upsert)")
    conformed = session.sql(f"""
        WITH clean_vitals
        AS
        (
            SELECT  sv.client_id,
                    sv.source_ale_prac_id,
                    sv.encounter_id,
                    sv.patient_id,
                    sv.height_in,
                    sv.weight_lbs,
                    sv.bp_systolic,
                    sv.bp_diastolic,
                    sv.bmi,
                    sv.row_hash AS source_guid,
                    sv.source,
                    sv.created_at,
                    sv.updated_at,
                    to_timestamp(sv.hj_modify_timestamp,
                                 'yyyyMMdd HH:mm:ss:SSSSSS') AS hj_modify_timestamp,
                    IF (sv.service_time IS NOT NULL,
                        to_timestamp(concat(sv.service_date, ' ', sv.service_time),
                                    'yyyyMMdd HH:mm:ss'),
                        to_timestamp(sv.service_date, 'yyyyMMdd')
                    ) AS observation_date,
                    ROW_NUMBER() OVER (
                        PARTITION BY sv.client_id, sv.encounter_id, sv.patient_id
                        ORDER BY
                            IF (sv.service_time IS NOT NULL,
                                to_timestamp(
                                    concat(sv.service_date, ' ', sv.service_time),
                                    'yyyyMMdd HH:mm:ss'),
                                to_timestamp(sv.service_date, 'yyyyMMdd')) DESC
                    ) AS rn
            FROM    stage_vitals AS sv
                LEFT JOIN delta.`{delta_path}` AS delta
                    ON sv.row_hash = delta.source_guid
            WHERE   delta.source_guid IS NULL
        )
        SELECT  v.client_id,
                v.source_ale_prac_id,
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
                v.source_guid,
                v.source,
                v.created_at,
                v.updated_at
        FROM    clean_vitals AS v
        WHERE   v.rn = 1
    """)

    logger.info(f"Merge vitals delta: {delta_path}")
    (
        DeltaTable
        .forPath(session, delta_path).alias('tgt')
        .merge(conformed.alias('src'), """
               tgt.source_ale_prac_id = src.source_ale_prac_id
               AND tgt.encounter_id = src.encounter_id
               AND tgt.patient_id = src.patient_id
               AND tgt.code = src.code
        """)
        .whenMatchedUpdate(
            condition="tgt.observation_date < src.observation_date",
            set={
                "patient_id": col('src.patient_id'),
                "name": col('src.name'),
                "code": col('src.code'),
                "code_system_name": col('src.code_system_name'),
                "code_system_oid": col('src.code_system_oid'),
                "value": col('src.value'),
                "unit": col('src.unit'),
                "observation_date": col('src.observation_date'),
                "source_guid": col('src.source_guid'),
                "source": col('src.source'),
                "updated_at": col('src.updated_at'),
            })
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
    #     .orderBy('source_ale_prac_id')
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
        .withColumn("row_count", expr("operationMetrics.numOutputRows"))
        .select(
            "version",
            "timestamp",
            "userName",
            "operation",
            "job",
            # "operationParameters",
            # "operationMetrics",
            "row_count"
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
