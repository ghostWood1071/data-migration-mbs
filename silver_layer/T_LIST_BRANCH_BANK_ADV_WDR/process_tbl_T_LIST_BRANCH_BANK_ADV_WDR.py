from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, date_format, to_date

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
        .load("s3a://warehouse/bronze/T_LIST_BRANCH_BANK_ADV_WDR")
)
#
###


###---------------------------------ADD TECHNIQUE COLUMN---------------------------------
#
# source_df.withColumn("partiton_date", date_format("C_WITHDRAW_DATE", "yyyy-MM-dd"))
(
    source_df.withColumn("valid_from", current_timestamp())
                                .withColumn("valid_to", None)
                                .withColumn("is_current", True)
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
    source_df.write.format("delta")
                    .mode("overwrite")
                    .option("path", "s3a://warehouse/silver/T_LIST_BRANCH_BANK_ADV_WDR")
                    .saveAsTable("silver.dim_T_LIST_BRANCH_BANK_ADV_WDR")
)
#
###

spark.stop()
