from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, date_format, to_date, lit
from pyspark.sql.types import TimestampType

spark = (
    SparkSession.builder
    .appName("CreateDeltaTables")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .enableHiveSupport()
    .getOrCreate()
)

#database config to connect
DATABASE_CONFIG = {
    "url": "jdbc:oracle:thin:@10.91.101.161:1521/tradingnkt",
    "user": "mispoc",
    "password": "d8daa822c8b0c6f9d85",
    "driver": "oracle.jdbc.driver.OracleDriver"
}

###---------------------------------READ DATA FROM ORACLE DB---------------------------------
#
incremental_df = (
    spark.read
        .format("jdbc")
        .option("url", DATABASE_CONFIG["url"])
        .option("dbtable", "(SELECT * FROM BACK.T_TLO_DEBIT_BALANCE_HISTORY WHERE C_TRANSACTION_DATE >= TRUNC(SYSDATE) AND C_TRANSACTION_DATE < TRUNC(SYSDATE) + 1) tbl")
        .option("user", DATABASE_CONFIG["user"])
        .option("password", DATABASE_CONFIG["password"])
        .option("driver", DATABASE_CONFIG["driver"])
        .load()
)
#
###

###---------------------------------INCREMENTAL LOAD DATA TO SILVER LAYER---------------------------------
#
silver_df  = (
    incremental_df.withColumn("partition_date", to_date("C_TRANSACTION_DATE", "yyyy-MM-dd"))
                                .withColumn("valid_from", current_timestamp())
                                .withColumn("valid_to", lit(None).cast(TimestampType()))
                                .withColumn("is_current", lit(True))
)
(
    silver_df.write.format("delta")
                .mode("append").partitionBy("partition_date")
                .option("path", "s3a://warehouse/silver/test_inc_T_TLO_DEBIT_BALANCE_HISTORY")
                .save()
)
#
###

###---------------------------------TEST INCREMENTAL LOAD DATA TO SILVER LAYER---------------------------------
#
spark.sql("DROP TABLE IF EXISTS silver.test_inc_fact_T_TLO_DEBIT_BALANCE_HISTORY")

spark.sql("""
    CREATE TABLE IF NOT EXISTS silver.test_inc_fact_T_TLO_DEBIT_BALANCE_HISTORY
    USING delta
    LOCATION 's3a://warehouse/silver/test_inc_T_TLO_DEBIT_BALANCE_HISTORY'
""")
#

spark.stop()
