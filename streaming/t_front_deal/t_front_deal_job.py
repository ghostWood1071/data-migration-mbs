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

# Location Delta trên MinIO
delta_path = "s3a://warehouse/silver/dim_t_front_deal_scd2"
checkpoint_path = "s3a://warehouse/checkpoints/cdc_front_deal_scd2"

# =========================
# 2. Schema Debezium payload
# =========================
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

# schema root của message: { "schema": {...}, "payload": {...} }
schema_root = StructType() \
    .add("payload", schema_payload)

# =========================
# 3. Read Debezium CDC from Kafka
# =========================

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

# =========================
# 4. Build SCD2 record: PK_ACCOUNT + created_at + updated_at + op + business columns
# =========================

from pyspark.sql.types import TimestampType

select_exprs = []

# PK_ACCOUNT: ưu tiên after, nếu delete thì lấy before
select_exprs.append(
    coalesce(
        col("data.payload.after.PK_FRONT_DEAL"),
        col("data.payload.before.PK_FRONT_DEAL")
    ).alias("PK_FRONT_DEAL")
)


# updated_at: thời điểm event Debezium (ts_ms → timestamp)
# select_exprs.append(
#     (col("data.payload.ts_ms") / 1000).cast(TimestampType()).alias("updated_at")
# )
select_exprs.append(
    F.from_utc_timestamp(F.from_unixtime(col("data.payload.ts_ms") / 1000), "Asia/Ho_Chi_Minh")
)
# op: Debezium operation 'c', 'u', 'd'
select_exprs.append(
    col("data.payload.op").alias("op")
)

# Các cột business: lấy từ after.* (delete thì null, vẫn ok cho SCD2)
for field in schema_after.fields:
    name = field.name
    if name not in ["PK_FRONT_DEAL"]:
    # PK_ACCOUNT & C_CREATE_TIME đã map rồi, nhưng giữ C_CREATE_TIME business vẫn tốt
        select_exprs.append(col(f"data.payload.after.{name}").alias(name))

df_scd2 = df_parsed.select(*select_exprs)
df_scd2 = df_scd2.withColumn(
    "created_at",
    F.when(F.col("op") == F.lit("c"), F.col("updated_at"))
).withColumn(
    "partition_date",
    F.date_format(F.col("updated_at"), "yyyy-MM-dd")
)
print("SCD2 columns:", len(df_scd2.columns))
print(df_scd2.columns)


# =========================
# 6. Write streaming ra Delta (append-only SCD2)
# =========================

query = (
    df_scd2.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_path)
    .start(delta_path)
    
)

query.awaitTermination(600)
query.stop()
spark.stop()