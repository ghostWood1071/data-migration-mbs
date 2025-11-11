from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

spark = (
    SparkSession.builder
    .appName("CreateDeltaTables")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .enableHiveSupport()
    .getOrCreate()
)

#database config 
DATABASE_CONFIG_DICT = {
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
        .option("url", DATABASE_CONFIG_DICT["url"])
        .option("dbtable", "BACK.T_BACK_ADVANCE_WITHDRAW")
        .option("user", DATABASE_CONFIG_DICT["user"])
        .option("password", DATABASE_CONFIG_DICT["password"])
        .option("driver", DATABASE_CONFIG_DICT["driver"])
        .load()
)
#
###

###---------------------------------ADD INGEST TIME COLUMN---------------------------------
#
T_BACK_ADVANCE_WITHDRAW_df.withColumn("ingest_time", current_timestamp())
#
###

###---------------------------------WRITE DATA TO BRONZE BUCKET IN MINIO---------------------------------
#
T_BACK_ADVANCE_WITHDRAW_df.write.format("delta").mode("overwrite").save("s3a://warehouse/bronze/T_BACK_ADVANCE_WITHDRAW")
#
###

spark.stop()