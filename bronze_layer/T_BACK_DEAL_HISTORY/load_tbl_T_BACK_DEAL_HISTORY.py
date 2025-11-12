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

#database config to connect
DATABASE_CONFIG = {
    "url": "jdbc:oracle:thin:@10.91.101.161:1521/tradingnkt",
    "user": "mispoc",
    "password": "d8daa822c8b0c6f9d85",
    "driver": "oracle.jdbc.driver.OracleDriver"
}

###---------------------------------READ DATA FROM ORACLE DB---------------------------------
#
T_BACK_DEAL_HISTORY_df = (
    spark.read
        .format("jdbc")
        .option("url", DATABASE_CONFIG["url"])
        .option("dbtable", "BACK.T_BACK_DEAL_HISTORY")
        .option("user", DATABASE_CONFIG["user"])
        .option("password", DATABASE_CONFIG["password"])
        .option("driver", DATABASE_CONFIG["driver"])
        .load()
)
#
###

###---------------------------------WRITE DATA TO BRONZE BUCKET IN MINIO---------------------------------
#
T_BACK_DEAL_HISTORY_df.write.format("parquet").mode("overwrite").save("s3a://warehouse/bronze/T_BACK_DEAL_HISTORY")
#
###

spark.stop()