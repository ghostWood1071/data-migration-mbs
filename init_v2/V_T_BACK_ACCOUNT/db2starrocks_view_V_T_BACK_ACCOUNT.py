from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, date_format, to_date, lit
from pyspark.sql.types import TimestampType

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
V_T_BACK_ACCOUNT_df = (
    spark.read
        .format("jdbc")
        .option("url", DATABASE_CONFIG["url"])
        .option("dbtable", "MISUSER.V_T_BACK_ACCOUNT")
        .option("user", DATABASE_CONFIG["user"])
        .option("password", DATABASE_CONFIG["password"])
        .option("driver", DATABASE_CONFIG["driver"])
        .load()
)
#
###

###---------------------------------LOAD DATA TO STARROCKS---------------------------------
#
transformed_df = V_T_BACK_ACCOUNT_df.select("C_ACCOUNT_CODE", "C_OPEN_DATE")

table_name = "dim_v_t_back_account"
(
    transformed_df.write.format("starrocks")
        .option("starrocks.fe.http.url", "http://kube-starrocks-fe-service.warehouse.svc.cluster.local:8030")
        .option("starrocks.fe.jdbc.url", "jdbc:mysql://kube-starrocks-fe-service.warehouse.svc.cluster.local:9030")
        .option("starrocks.table.identifier", f"mbs_realtime_db.{table_name}")
        .option("starrocks.user", "mbs_demo")
        .option("starrocks.password", "mbs_demo")
        .mode("append")
        .save()
)
###

spark.stop()
