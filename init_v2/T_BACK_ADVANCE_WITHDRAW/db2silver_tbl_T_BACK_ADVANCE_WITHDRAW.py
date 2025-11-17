from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, date_format, to_date, lit
from pyspark.sql.types import TimestampType

spark = (
    SparkSession.builder
    .appName("create_delta_tbl_T_BACK_ADVANCE_WITHDRAW")
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
T_BACK_ADVANCE_WITHDRAW_df = (
    spark.read
        .format("jdbc")
        .option("url", DATABASE_CONFIG["url"])
        .option("dbtable", "BACK.T_BACK_ADVANCE_WITHDRAW")
        .option("user", DATABASE_CONFIG["user"])
        .option("password", DATABASE_CONFIG["password"])
        .option("driver", DATABASE_CONFIG["driver"])
        .load()
)
#
###


###---------------------------------ADD TECHNIQUE COLUMN---------------------------------
#
silver_df  = (
    T_BACK_ADVANCE_WITHDRAW_df.withColumn("partition_date", to_date("C_APPROVE_TIME", "yyyy-MM-dd"))
                                .withColumn("valid_from", current_timestamp())
                                .withColumn("valid_to", lit(None).cast(TimestampType()))
                                .withColumn("is_current", lit(True))
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
                    .option("path", "s3a://warehouse/silver/T_BACK_ADVANCE_WITHDRAW")
                    .save()
)

spark.sql("DROP TABLE IF EXISTS silver.fact_T_BACK_ADVANCE_WITHDRAW")

spark.sql("""
    CREATE TABLE IF NOT EXISTS silver.fact_T_BACK_ADVANCE_WITHDRAW
    USING delta
    LOCATION 's3a://warehouse/silver/T_BACK_ADVANCE_WITHDRAW'
""")


#
###


spark.stop()
