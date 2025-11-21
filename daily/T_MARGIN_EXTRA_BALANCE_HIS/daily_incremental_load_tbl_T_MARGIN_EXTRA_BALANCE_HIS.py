from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, date_format, to_date, lit
from pyspark.sql.types import TimestampType

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
incremental_df = (
    spark.read
        .format("jdbc")
        .option("url", DATABASE_CONFIG["url"])
        .option("dbtable", "(SELECT * FROM BACK.T_MARGIN_EXTRA_BALANCE_HIS WHERE C_TRADING_DATE >= TRUNC(SYSDATE) -1 AND C_TRADING_DATE < TRUNC(SYSDATE)) tbl")
        .option("user", DATABASE_CONFIG["user"])
        .option("password", DATABASE_CONFIG["password"])
        .option("driver", DATABASE_CONFIG["driver"])
        .load()
)
#
###

###---------------------------------INCREMENTAL LOAD DATA TO SILVER LAYER---------------------------------
#
silver_df  = (
    incremental_df.withColumn("partition_date", to_date("C_TRADING_DATE", "yyyy-MM-dd"))
                                .withColumn("valid_from", current_timestamp())
                                .withColumn("valid_to", lit(None).cast(TimestampType()))
                                .withColumn("is_current", lit(True))
)

# (
#     silver_df.write.format("delta")
#                 .mode("append").partitionBy("partition_date")
#                 .option("path", "s3a://warehouse/silver/T_MARGIN_EXTRA_BALANCE_HIS")
#                 .save()
# )

table_name = "fact_t_margin_extra_balance_his"
(
    silver_df.write.format("starrocks")
        .option("starrocks.fe.http.url", "http://kube-starrocks-fe-service.warehouse.svc.cluster.local:8030")
        .option("starrocks.fe.jdbc.url", "jdbc:mysql://kube-starrocks-fe-service.warehouse.svc.cluster.local:9030")
        .option("starrocks.table.identifier", f"mbs_realtime_db.{table_name}")
        .option("starrocks.user", "mbs_demo")
        .option("starrocks.password", "mbs_demo")
        .mode("append")
        .save()
)
#
###

spark.stop()
