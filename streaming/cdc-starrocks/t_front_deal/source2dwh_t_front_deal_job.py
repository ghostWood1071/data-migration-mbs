from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, coalesce
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, TimestampType, LongType
)
import pyspark.sql.functions as F

# =========================
# 1. SparkSession Config
# =========================

spark = (
    SparkSession.builder
    .appName("CDC_Account_SCD2_Delta")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")
checkpoint_path = "s3a://warehouse/checkpoints/cdc_front_deal_dwh"
spark.sql("CREATE DATABASE IF NOT EXISTS silver")
spark.sql(f"""
CREATE TABLE IF NOT EXISTS silver.t_front_deal_scd2
USING DELTA
LOCATION '{delta_path}'
""")

schema_after = StructType([
    StructField("PK_FRONT_DEAL", DoubleType(), False),
    StructField("C_MARKET", StringType(), True),
    StructField("C_TRADING_CENTER", StringType(), True),
    StructField("C_ORDER_NO", DoubleType(), True),
    StructField("C_SUB_ORDER_NO", DoubleType(), True),
    StructField("C_TRADER_BRANCH_CODE", StringType(), True),
    StructField("C_TRADER_SUB_BRANCH_CODE", StringType(), True),
    StructField("C_TRADER_CODE", StringType(), True),
    StructField("C_BROKER_BRANCH_CODE", StringType(), True),
    StructField("C_BROKER_SUB_BRANCH_CODE", StringType(), True),
    StructField("C_BROKER_CODE", StringType(), True),
    StructField("C_ACCOUNT_CODE", StringType(), True),
    StructField("C_ACCOUNT_TYPE", StringType(), True),
    StructField("C_SHARE_CODE", StringType(), True),
    StructField("C_SHARE_TYPE", StringType(), True),
    StructField("C_SHARE_STATUS", StringType(), True),
    StructField("C_SIDE", StringType(), True),
    StructField("C_SIDE_TYPE", StringType(), True),
    StructField("C_SHORT_SALE", DoubleType(), True),
    StructField("C_CHANEL", StringType(), True),
    StructField("C_DUEDAY", DoubleType(), True),
    StructField("C_MATCHED_VOLUME", DoubleType(), True),
    StructField("C_MATCHED_PRICE", DoubleType(), True),
    StructField("C_MATCHED_TIME", TimestampType(), True),
    StructField("C_CONFIRM_NO", StringType(), True),
    StructField("C_SET_ORDER_TYPE", StringType(), True),
    StructField("C_MAX_FEE_RATE", DoubleType(), True),
    StructField("C_DEAL_STATUS", DoubleType(), True),
    StructField("C_ISIN_CODE", StringType(), True),
    StructField("C_MARKET_ID", StringType(), True),
    StructField("C_MARKET_SUBID", StringType(), True),
    StructField("C_KRX_ORDER_ID", StringType(), True),
    StructField("C_KRX_EXEC_ID", StringType(), True),
    StructField("C_KRX_CLORDID", StringType(), True),
    StructField("C_KRX_ORIG_CLORDID", StringType(), True),
    StructField("C_TAX_RATE", DoubleType(), True),
])

schema_payload = StructType() \
    .add("before", schema_after) \
    .add("after", schema_after) \
    .add("op", StringType()) \
    .add("ts_ms", LongType())

schema_root = StructType() \
    .add("payload", schema_payload)

cols = [f.name for f in schema_after.fields]
cols_str = ", ".join(cols)

df_kafka = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "192.168.73.190:9092,192.168.73.191:9092,192.168.73.192:9092")
    .option("subscribe", "oracle-bo-poc.FRONT.T_FRONT_DEAL")
    .option("startingOffsets", "latest")
    .load()
)

df_json = df_kafka.selectExpr("CAST(value AS STRING) AS json_str")

df_parsed = df_json.select(
    from_json(col("json_str"), schema_root).alias("data")
)

from pyspark.sql.types import TimestampType

select_exprs = []

select_exprs.append(
    coalesce(
        col("data.payload.after.PK_FRONT_DEAL"),
        col("data.payload.before.PK_FRONT_DEAL")
    ).cast("string").alias("PK_FRONT_DEAL")
)

select_exprs.append(
    F.when(col("data.payload.op").isin('c', 'u'), F.lit(0)).otherwise(1).alias("__op")
)

# Các cột business: lấy từ after.* (delete thì null, vẫn ok cho SCD2)
for field in schema_after.fields:
    name = field.name
    if name not in ["PK_FRONT_DEAL"]:
    # PK_ACCOUNT & C_CREATE_TIME đã map rồi, nhưng giữ C_CREATE_TIME business vẫn tốt
        select_exprs.append(col(f"data.payload.after.{name}").alias(name))

df_scd2 = df_parsed.select(*select_exprs).filter(~F.col("PK_FRONT_DEAL").isNull())
cols_str = cols_str.split(", ")
cols_str.append("__op")
df_scd2 = df_scd2.select(*cols_str)

query =(
    df_scd2.writeStream.format("starrocks")
     .option("starrocks.fe.http.url", "http://kube-starrocks-fe-service.warehouse.svc.cluster.local:8030")
     .option("starrocks.fe.jdbc.url", "jdbc:mysql://kube-starrocks-fe-service.warehouse.svc.cluster.local:9030")
     .option("starrocks.table.identifier", "mbs_realtime_db.t_front_deal")
     .option("starrocks.user", "mbs_demo")
     .option("starrocks.password", "mbs_demo")
     .option("starrocks.columns", ", ".join(cols_str))
     .option("checkpointLocation", checkpoint_path)
     .outputMode("append")
     .start()
)

query.awaitTermination()
query.stop()
spark.stop()