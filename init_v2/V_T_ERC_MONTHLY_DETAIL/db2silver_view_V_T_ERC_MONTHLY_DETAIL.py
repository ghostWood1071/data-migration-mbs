from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, date_format, to_date, lit, col
from pyspark.sql.types import TimestampType, StringType

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
V_T_ERC_MONTHLY_DETAIL_df = (
    spark.read
        .format("jdbc")
        .option("url", DATABASE_CONFIG["url"])
        .option("dbtable", "MISUSER.V_T_ERC_MONTHLY_DETAIL")
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
    V_T_ERC_MONTHLY_DETAIL_df.withColumn("valid_from", current_timestamp())
            .withColumn("partition_year", col("C_YEAR").cast(StringType()))
            .withColumn("partition_month", col("C_MONTH").cast(StringType()))
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
                    .partitionBy("partition_year", "partition_month")
                    .option("path", "s3a://warehouse/silver/V_T_ERC_MONTHLY_DETAIL")
                    .save()
)

spark.sql("DROP TABLE IF EXISTS silver.fact_V_T_ERC_MONTHLY_DETAIL")


spark.sql("""
    CREATE TABLE IF NOT EXISTS silver.fact_V_T_ERC_MONTHLY_DETAIL
    USING delta
    LOCATION 's3a://warehouse/silver/V_T_ERC_MONTHLY_DETAIL'
""")
#
###

spark.stop()
