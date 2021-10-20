from pyspark.sql.types import (
    ArrayType, BooleanType, DateType, DecimalType,
    IntegerType, FloatType, LongType, StringType,
    StructType, StructField, TimestampType
)

VITALS = StructType([
    StructField('client_id', StringType(), nullable=True),
    StructField('patient_id', StringType(), nullable=True),
    StructField('service_date', StringType(), nullable=True),
    StructField('service_time', StringType(), nullable=True),
    StructField('height_cm', StringType(), nullable=True),
    StructField('height_in', StringType(), nullable=True),
    StructField('height_comment', StringType(), nullable=True),
    StructField('weight_kg', StringType(), nullable=True),
    StructField('weight_lbs', StringType(), nullable=True),
    StructField('weight_comment', StringType(), nullable=True),
    StructField('temperature_c', StringType(), nullable=True),
    StructField('temperature_f', StringType(), nullable=True),
    StructField('temperature_comment', StringType(), nullable=True),
    StructField('pulse', StringType(), nullable=True),
    StructField('pulse_comment', StringType(), nullable=True),
    StructField('respiration', StringType(), nullable=True),
    StructField('respiration_comment', StringType(), nullable=True),
    StructField('bp_systolic', StringType(), nullable=True),
    StructField('bp_diastolic', StringType(), nullable=True),
    StructField('bp_comment', StringType(), nullable=True),
    StructField('oxygen_saturation', StringType(), nullable=True),
    StructField('oxygen_comment', StringType(), nullable=True),
    StructField('bmi', StringType(), nullable=True),
    StructField('bmi_comment', StringType(), nullable=True),
    StructField('clinician', StringType(), nullable=True),
    StructField('encounter_id', StringType(), nullable=True),
    StructField('comment', StringType(), nullable=True),
    StructField('hj_create_timestamp', StringType(), nullable=True),
    StructField('hj_modify_timestamp', StringType(), nullable=True),
    StructField('client_vitals_id', StringType(), nullable=True),
    StructField('row_hash', StringType(), nullable=True),
])

# TODO: Create delta table from schema
# TODO: How to setup constraints (update trigger, sequence)? See generated columns
CQ_VITALS = StructType([
    # StructField('id', LongType(), nullable=True),
    StructField('client_id', StringType(), nullable=True),
    StructField('source_ale_prac_id', LongType(), nullable=True),
    StructField('encounter_id', StringType(), nullable=True),
    StructField('patient_id', StringType(), nullable=True),
    # StructField('pt_id', LongType(), nullable=True),
    StructField('name', StringType(), nullable=True),
    StructField('code', StringType(), nullable=True),
    StructField('code_system_name', StringType(), nullable=True),
    StructField('code_system_oid', StringType(), nullable=True),
    # StructField('orig_code', StringType(), nullable=True),
    # StructField('orig_code_system_name', StringType(), nullable=True),
    StructField('value', StringType(), nullable=True),
    StructField('unit', StringType(), nullable=True),
    # StructField('status', StringType(), nullable=True),
    StructField('observation_date', TimestampType(), nullable=True),
    # StructField('cq_document_id', LongType(), nullable=True),
    # StructField('content_lineage_id', LongType(), nullable=True),
    # StructField('source_name', StringType(), nullable=True),
    StructField('source_guid', StringType(), nullable=True),
    # StructField('created_at', TimestampType(), nullable=True),
    # StructField('updated_at', TimestampType(), nullable=True),
])

ENCOUNTERS = StructType([
    StructField('ID', StringType(), True),
    StructField('DATE', DateType(), True),
    StructField('PATIENT', StringType(), True),
    StructField('CODE', StringType(), True),
    StructField('DESCRIPTION', StringType(), True),
    StructField('REASONCODE', StringType(), True),
    StructField('REASONDESCRIPTION', StringType(), True),
])

OBSERVATIONS = """
    `DATE`        Date,
    `PATIENT`     String,
    `ENCOUNTER`   String,
    `CODE`        String,
    `DESCRIPTION` String,
    `VALUE`       Decimal(12, 2),
    `UNITS`       String
"""

PATIENTS = """
    ID          String,
    BIRTHDATE   Date,
    DEATHDATE   Date,
    SSN         String,
    DRIVERS     String,
    PASSPORT    Boolean,
    PREFIX      String,
    FIRST       String,
    LAST        String,
    SUFFIX      String,
    MAIDEN      String,
    MARITAL     String,
    RACE        String,
    ETHNICITY   String,
    GENDER      String,
    BIRTHPLACE  String,
    ADDRESS     String
"""
