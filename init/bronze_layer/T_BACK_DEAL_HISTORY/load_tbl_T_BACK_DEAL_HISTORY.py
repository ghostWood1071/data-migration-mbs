from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date

spark = (
    SparkSession.builder
    .appName("CreateDeltaTables")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .enableHiveSupport()
    .getOrCreate()
)

DATABASE_CONFIG = {
    "url": "jdbc:oracle:thin:@10.91.101.161:1521/tradingnkt",
    "user": "mispoc",
    "password": "d8daa822c8b0c6f9d85",
    "driver": "oracle.jdbc.driver.OracleDriver"
}

def ETL_data_each_year(year):
    T_BACK_DEAL_HISTORY_df = (
        spark.read
            .format("jdbc")
            .option("url", DATABASE_CONFIG["url"])
            .option("dbtable", f"(SELECT * FROM BACK.T_BACK_DEAL_HISTORY WHERE EXTRACT(YEAR FROM C_TRANSACTION_DATE) = {year}) tbl")
            .option("user", DATABASE_CONFIG["user"])
            .option("password", DATABASE_CONFIG["password"])
            .option("driver", DATABASE_CONFIG["driver"])
            .load()
    )
    transformed_df = T_BACK_DEAL_HISTORY_df.withColumn("partition_date", to_date("C_TRANSACTION_DATE", "yyyy-MM-dd"))
    transformed_df.write.format("parquet").mode("overwrite").partitionBy("partition_date").save("s3a://warehouse/bronze/T_BACK_DEAL_HISTORY")

for year in range(2008, 2026):
    ETL_data_each_year(year)

spark.stop()