from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, date_format, to_date, lit, col
from pyspark.sql.types import TimestampType, StringType
from zoneinfo import ZoneInfo
from datetime import datetime

spark = (
    SparkSession.builder
                .appName("monthly_incremental_load_view_V_T_ERC_MONTHLY_DETAIL")
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
tz = ZoneInfo("Asia/Ho_Chi_Minh")
cur_year = datetime.now(tz).year
cur_month = datetime.now(tz).month
if cur_month == 1:
    cur_year -= 1
    cur_month = 12
else:
    cur_month -= 1


incremental_df = (
    spark.read
        .format("jdbc")
        .option("url", DATABASE_CONFIG["url"])
        .option("dbtable", f"(SELECT * FROM MISUSER.V_T_ERC_MONTHLY_DETAIL WHERE C_YEAR = {cur_year} AND C_MONTH = {cur_month}) tbl")
        .option("user", DATABASE_CONFIG["user"])
        .option("password", DATABASE_CONFIG["password"])
        .option("driver", DATABASE_CONFIG["driver"])
        .load()
)
#
###

silver_df = (
    incremental_df.withColumn("valid_from", current_timestamp())
            .withColumn("partition_year", col("C_YEAR").cast(StringType()))
            .withColumn("partition_month", col("C_MONTH").cast(StringType()))
                                .withColumn("valid_to", lit(None).cast(TimestampType()))
                                .withColumn("is_current", lit(True))
)

###---------------------------------INCREMENTAL LOAD DATA TO STARROCKS---------------------------------
table_name = "test_fact_v_t_erc_monthly_detail"
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

spark.stop()