from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, date_format, to_date, lit
from pyspark.sql.types import TimestampType

spark = (
    SparkSession.builder
    .appName("CreateDeltaTables")
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
T_MARGIN_EXTRA_BALANCE_HIS_df = (
    spark.read
        .format("jdbc")
        .option("url", DATABASE_CONFIG["url"])
        .option("dbtable", "BACK.T_MARGIN_EXTRA_BALANCE_HIS")
        .option("user", DATABASE_CONFIG["user"])
        .option("password", DATABASE_CONFIG["password"])
        .option("driver", DATABASE_CONFIG["driver"])
        .load()
)
#
###


###---------------------------------ADD TECHNIQUE COLUMN---------------------------------
#
# source_df.withColumn("partiton_date", date_format("C_WITHDRAW_DATE", "yyyy-MM-dd"))
silver_df = (
    T_MARGIN_EXTRA_BALANCE_HIS_df.withColumn("partition_date", to_date("C_TRADING_DATE", "yyyy-MM-dd"))
                                .withColumn("valid_from", current_timestamp())
                                .withColumn("valid_to", lit(None).cast(TimestampType()))
                                .withColumn("is_current", lit(True))
                                .withColumn("create_at", current_timestamp())
)
#


###---------------------------------CREATE DATABASES---------------------------------
#
spark.sql("CREATE DATABASE IF NOT EXISTS silver")
#


###---------------------------------WRITE TABLE TO SILVER BUCKET IN MINIO---------------------------------
#
(
    silver_df.write.format("delta")
                    .mode("overwrite")
                    .partitionBy("partition_date")
                    .option("path", "s3a://warehouse/silver/T_MARGIN_EXTRA_BALANCE_HIS")
                    .save()
)

spark.sql("DROP TABLE IF EXISTS silver.fact_T_MARGIN_EXTRA_BALANCE_HIS")


spark.sql("""
    CREATE TABLE IF NOT EXISTS silver.fact_T_MARGIN_EXTRA_BALANCE_HIS
    USING delta
    LOCATION 's3a://warehouse/silver/T_MARGIN_EXTRA_BALANCE_HIS'
""")
#
###

spark.stop()
