# PySpark and Delta Lake
This repo contains a reference implementation for deploying Spark 3.1.1
and Delta Lake. In addition, it includes a reference implementation
showcasing the feature this architecture offers for acquiring data at scale.

## References
- [Learning Spark](https://www.amazon.com/Learning-Spark-Jules-Damji/dp/1492050040/ref=asc_df_1492050040/?tag=hyprod-20&linkCode=df0&hvadid=459538011055&hvpos=&hvnetw=g&hvrand=9637554060338455232&hvpone=&hvptwo=&hvqmt=&hvdev=c&hvdvcmdl=&hvlocint=&hvlocphy=9026797&hvtargid=pla-918087322526&psc=1)
- [Delta Lake](https://docs.delta.io/latest/index.html)
- [spark-submit](http://spark.apache.org/docs/latest/submitting-applications.html)
- [Spark on Kubernetes](http://spark.apache.org/docs/latest/running-on-kubernetes.html)

## Index
- [Appendix](#appendix)
- [PySpark Examples - Fluent API](#fluent-api)
- [PySpark Examples - SparkSQL](#spark-sql)
- [PySpark Examples - Delta Lake](#delta-lake)

## How it Works
![Spark on K8s](https://spark.apache.org/docs/latest/img/k8s-cluster-mode.png)

```shell
./bin/docker-image-tool.sh -t 3.1.1 build
```

## Appendix
This section contains examples on general PySpark usage. Both a fluent
API (Pandas-like) and an `ANSI:2003`-compliant SQL interface is
supported. In a sense PySpark's DataFrame API unifies programmatic
access to Spark functionality, rather than having to use distinct
tools (`Python` and `dbt` for example).

### <a name='fluent-api'></a>PySpark 101 - Fluent API
The fluent API will be familiar to those with Pandas experience.
1. Define a schema. Using a pre-defined schema avoids schema inference thereby improving load performance.
```python
ENCOUNTERS = StructType([
    StructField('ID', StringType(), True),
    StructField('DATE', DateType(), True),
    StructField('PATIENT', StringType(), True),
    StructField('CODE', StringType(), True),
    StructField('DESCRIPTION', StringType(), True),
    StructField('REASONCODE', StringType(), True),
    StructField('REASONDESCRIPTION', StringType(), True),
])
```

2. Read a CSV into a DataFrame using a pre-defined schema. 
```python
encounters_1 = (
    spark_session
    .read
    .option("mode", "FAILFAST")  # Exit if any errors
    .option("nullValue", "")  # Replace any null data with quotes
    .csv('sample_data/output_1/encounters.csv',
         header=True,
         schema=ENCOUNTERS)
)
```
2. Transform and aggregate a DataFrame
```python
enc_counts = (
    encounters_1
    .withColumn("encounter_date", to_timestamp(col("DATE"), "MM/dd/yyyy"))
    .where(col("encounter_date").isNotNull() & col("REASONCODE").isNotNull())
    .where("REASONCODE != '4448140091' AND REASONCODE IS NOT NULL")
    .select('CODE', year('encounter_date').alias('enc_year'), 'ID')
    .groupBy('CODE', 'enc_year')  # wide transformation
    .agg(countDistinct('ID').alias("total_enc"))  # action
    .orderBy("total_enc", ascending=False)  # wide transformation
)
```
3. Save a `DataFrame` to a partitioned parquet file.
```python
(
    vitals_1.unionAll(vitals_2)
    .limit(21)  # filtered for demo
    .write
    .parquet(to_path,
             mode='overwrite',
             partitionBy=['PRAC_ID'])  # action
)
```
4. Save a `DataFrame` to a PostgreSQL.
```python
(
    bp_counts
    .write
    .jdbc(url=os.environ['POSTGRES_JDBC_URL'],
          table='bp_counts',
          mode='overwrite')
 )
```

### <a name='spark-sql'></a>PySpark 101 - Spark SQL
Spark SQL complies with `ANSI:2003` and can be preferable when implementing complex transformations.
1. Create a database in the `Hive` metastore (i.e. data dictionary).
```python
spark_session.sql("CREATE DATABASE clinical_data")
spark_session.sql("USE clinical_data")
```
2. Create a managed table (i.e. both schema and data managed in `Hive`).
```python
(
    enc_counts
    .write
    # .option("path", "/tmp/data/us_flights_delay")  # converts to unmanaged
    .saveAsTable("encounter_codes")  # action
)
```
3. Create an unmanaged table (only schema managed in `Hive`)
```python
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
```
4. Create a table and add to the `Hive` metastore (i.e. data dictionary).
```python
(
    vitals_1  # DataFrame
    .write
    .saveAsTable("vitals_1")  # action
)
```
5. Create a global (visible across Spark sessions) and local temporary views. Tables persist after session termination but views disappear.
```python
spark_session.sql("""
    CREATE OR REPLACE GLOBAL TEMP VIEW vw_vitals_1
    AS
    SELECT  *
    FROM    vitals_1
""")
vitals_2.createOrReplaceTempView("vitals_2")
```
6. Implement a Spark SQL transformation.
```python
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
    ORDER BY DESCRIPTION
""")
```

### <a name='delta-lake'></a>PySpark 101 - Delta Lake API
The Delta Lake combines the ACID/CRUD properties of a database with the performance/flexibility of a data lake. These are also termed "Data Lake Houses".
1. Save a `DataFrame` as a `Delta` table. The `mergeSchema` option supports "schema drift".
```python
(
    enc_counts
    .write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .save(delta_path)  # action
)
```
2. Load a `Delta` table as a `DataFrame`.
```python
encounters_delta = (
    spark_session
    .read
    .format("delta")
    .load(delta_path)
)
```
3. Load a `Delta` table from disk.
```python
delta_table = DeltaTable.forPath(spark_session, delta_path)
```
4. Use the "time-travel" feature to show the `Delta` table's last 3 versions.
```python
(
    delta_table
    .history(3)
    .select("version", "timestamp", "operation", "operationParameters")
    .show(truncate=False)
)
```
4. Use the "time-travel" feature to load the `Delta` table's previous version (by timestamp).
```python
(
    spark_session
    .read
    .format("delta")
    .option("timestampAsOf", "2021-10-19 01:22:16.924")  # timestamp after table creation
    .load(delta_path)
    .show()
)
```
5. Use the "time-travel" feature to load the `Delta` table's previous version (by version number).
```python
(
    spark_session
    .read
    .format("delta")
    .option("versionAsOf", "1")
    .load(delta_path)
    .show()
)
```
