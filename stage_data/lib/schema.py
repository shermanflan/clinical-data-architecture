from pyspark.sql.types import (
    ArrayType, BooleanType, DateType, DecimalType,
    IntegerType, FloatType, StringType, StructType,
    StructField,
)


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
