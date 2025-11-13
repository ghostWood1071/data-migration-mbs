from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, date_format, to_date, lit

spark = (
    SparkSession.builder
    .appName("create_delta_tbl_T_BACK_ADVANCE_WITHDRAW")
    .enableHiveSupport()
    .getOrCreate()
)


###---------------------------------READ DATA FROM BRONZE---------------------------------
#
source_df = (
    spark.read
        .format("parquet")
        .load("s3a://warehouse/bronze/T_BACK_ADVANCE_WITHDRAW")
)
#
###

print("Đọc thành công")


###---------------------------------ADD TECHNIQUE COLUMN---------------------------------
#
silver_df  = (
    source_df.withColumn("partition_date", to_date("C_WITHDRAW_DATE", "yyyy-MM-dd"))
                                .withColumn("valid_from", current_timestamp())
                                .withColumn("valid_to", lit(None))
                                .withColumn("is_current", lit(True))
                                .withColumn("create_at", current_timestamp())
)
#

print("Thêm cột thành công")



###---------------------------------CREATE DATABASES---------------------------------
#
spark.sql("CREATE DATABASE IF NOT EXISTS silver")
spark.sql("CREATE DATABASE IF NOT EXISTS gold")
#

print("Tạo database thành công")


###---------------------------------WRITE TABLE TO SILVER BUCKET IN MINIO---------------------------------
#
spark.sql("DROP TABLE silver.t_back_advance_withdraw")

(
    silver_df.write.format("delta")
                    .mode("overwrite")
                    .partitionBy("partition_date")
                    .option("path", "s3a://warehouse/silver/T_BACK_ADVANCE_WITHDRAW")
                    .save()
)

spark.sql("""
    CREATE TABLE IF NOT EXISTS silver.fact_T_BACK_ADVANCE_WITHDRAW
    USING delta
    LOCATION 's3a://warehouse/silver/T_BACK_ADVANCE_WITHDRAW'
""")


#
###

print("Tạo bảng thành công")


spark.stop()
