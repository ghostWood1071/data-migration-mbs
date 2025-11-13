from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, date_format, to_date, lit

spark = (
    SparkSession.builder
    .appName("CreateDeltaTables")
    .enableHiveSupport()
    .getOrCreate()
)


###---------------------------------READ DATA FROM BRONZE---------------------------------
#
source_df = (
    spark.read
        .format("parquet")
        .load("s3a://warehouse/bronze/T_TLO_DEBIT_BALANCE_HISTORY")
)
#
###


###---------------------------------ADD TECHNIQUE COLUMN---------------------------------
#
# source_df.withColumn("partiton_date", date_format("C_WITHDRAW_DATE", "yyyy-MM-dd"))
silver_df = (
    source_df.withColumn("partition_date", to_date("C_TRANSACTION_DATE", "yyyy-MM-dd"))
                                .withColumn("valid_from", current_timestamp())
                                .withColumn("valid_to", lit(None))
                                .withColumn("is_current", lit(True))
                                .withColumn("create_at", current_timestamp())
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
                    .option("path", "s3a://warehouse/silver/T_TLO_DEBIT_BALANCE_HISTORY")
                    .save()
)

spark.sql("""
    CREATE TABLE IF NOT EXISTS silver.fact_T_TLO_DEBIT_BALANCE_HISTORY
    USING delta
    LOCATION 's3a://warehouse/silver/T_TLO_DEBIT_BALANCE_HISTORY'
""")
#
###

spark.stop()
