from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, date_format, to_date, lit, col
from pyspark.sql.types import TimestampType, DecimalType

spark = (
    SparkSession.builder
    .appName("create_delta_tbl_T_BACK_DEAL_HISTORY")
    .enableHiveSupport()
    .getOrCreate()
)


###---------------------------------READ DATA FROM BRONZE---------------------------------
#
source_df = (
    spark.read
        .format("parquet")
        .load("s3a://warehouse/bronze/T_BACK_DEAL_HISTORY")
)
#
###


###---------------------------------ADD TECHNIQUE COLUMN---------------------------------
#
silver_df = (
    source_df.withColumn("partition_date", to_date("C_TRANSACTION_DATE", "yyyy-MM-dd"))
                                .withColumn("valid_from", current_timestamp())
                                .withColumn("valid_to", lit(None).cast(TimestampType()))
                                .withColumn("is_current", lit(True))
                                .withColumn("create_at", current_timestamp())
                                .withColumn("C_ORDER_NO", col("C_ORDER_NO").cast(DecimalType(38, 10)))
                                .withColumn("C_SUB_ORDER_NO", col("C_SUB_ORDER_NO").cast(DecimalType(38, 10)))
                                .withColumn("C_MATCHED_VOLUME", col("C_MATCHED_VOLUME").cast(DecimalType(38, 10)))
                                .withColumn("C_MATCHED_PRICE", col("C_MATCHED_PRICE").cast(DecimalType(38, 10)))
                                .withColumn("C_COMM_RATE", col("C_COMM_RATE").cast(DecimalType(38, 10)))
                                .withColumn("C_TAX_FLAG", col("C_TAX_FLAG").cast(DecimalType(38, 10)))
                                .withColumn("C_TAX_RATE", col("C_TAX_RATE").cast(DecimalType(38, 10)))
                                .withColumn("C_DEAL_STATUS", col("C_DEAL_STATUS").cast(DecimalType(38, 10)))
                                .withColumn("C_CARE_RATE", col("C_CARE_RATE").cast(DecimalType(38, 10)))          
)
#


###---------------------------------CREATE DATABASES---------------------------------
#
spark.sql("CREATE DATABASE IF NOT EXISTS silver")
spark.sql("CREATE DATABASE IF NOT EXISTS gold")
#


###---------------------------------WRITE TABLE TO SILVER BUCKET IN MINIO---------------------------------
#
(
    silver_df.write.format("delta")
                    .mode("overwrite")
                    .partitionBy("partition_date")
                    .option("path", "s3a://warehouse/silver/T_BACK_DEAL_HISTORY")
                    .save()
)

spark.sql("DROP TABLE IF EXISTS silver.fact_T_BACK_DEAL_HISTORY")

spark.sql("""
    CREATE TABLE IF NOT EXISTS silver.fact_T_BACK_DEAL_HISTORY
    USING delta
    LOCATION 's3a://warehouse/silver/T_BACK_DEAL_HISTORY'
""")
#
###

spark.stop()
