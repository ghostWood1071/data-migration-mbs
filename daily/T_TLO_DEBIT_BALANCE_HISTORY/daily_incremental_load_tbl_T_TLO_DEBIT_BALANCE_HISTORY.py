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

###---------------------------------INCREMENTAL LOAD DATA TO BRONZE LAYER---------------------------------
#
incremental_df.write.format("parquet").mode("append").save("s3a://warehouse/bronze/T_TLO_DEBIT_BALANCE_HISTORY")
#
###

###---------------------------------INCREMENTAL LOAD DATA TO SILVER LAYER---------------------------------
#
silver_df  = (
    incremental_df.withColumn("partition_date", to_date("C_TRANSACTION_DATE", "yyyy-MM-dd"))
                                .withColumn("valid_from", current_timestamp())
                                .withColumn("valid_to", lit(None).cast(TimestampType()))
                                .withColumn("is_current", lit(True))
                                .withColumn("create_at", current_timestamp())
)
silver_df.write.format("parquet").mode("append").save("s3a://warehouse/silver/T_TLO_DEBIT_BALANCE_HISTORY")
#
###

spark.stop()
