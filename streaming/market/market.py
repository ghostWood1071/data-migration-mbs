from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, coalesce
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, TimestampType, LongType
)
import pyspark.sql.functions as F
from pyspark.sql.protobuf.functions import from_protobuf
from minio import Minio
from minio.error import S3Error

# =========================
# 1. SparkSession Config
# =========================

client = Minio(
    "minio.storage.svc.cluster.local:9000",  # Địa chỉ MinIO server, ví dụ "localhost:9000"
    access_key="minioadmin",  # Thay bằng access key của bạn
    secret_key="minio@demo!",  # Thay bằng secret key của bạn
    secure=False  # True nếu MinIO của bạn sử dụng HTTPS
)

# Tên bucket và file cần tải
bucket_name = "asset"
file_name = "asset/decoder/schema_combined.desc"  # Tên file trên MinIO
download_path = "schema_combined.desc" 

spark = (
    SparkSession.builder
    .appName("CDC_Account_SCD2_Delta")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# Location Delta trên MinIO
delta_path = "s3a://warehouse/silver/market_raw"
checkpoint_path = "s3a://warehouse/checkpoints/market_raw_checkpoint"

# =========================
# 2. Schema Debezium payload
# =========================
spark.sql("CREATE DATABASE IF NOT EXISTS silver")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS silver.market_raw
USING DELTA
LOCATION '{delta_path}'
""")

# =========================
# 3. Read Debezium CDC from Kafka
# =========================
desc_file = "schema_combined.desc"
df_kafka = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "10.19.8.55:1221,10.19.8.56:1221,10.19.8.57:1221")
    .option("subscribe", "rlt.mkt.nd168.tick")
    .option("startingOffsets", "earliest")
    .load()
)

df = df_kafka.select(from_protobuf("value", "google.protobuf.Any", desc_file).alias("msg"))
df = df.select(
    F.split(F.col("msg.type_url"), "\/")[1].cast("string").alias("class"), 
    F.col("msg.value").alias("body")
)
df = df.select(from_protobuf("body", "google.protobuf.Any", desc_file).alias("msg"))

message_cls = [
    "ForeignRoom",
    "ICBIndustry",
    "IndexBasket",
    "IndexUpdate",
    "LastSaleFull",
    "LastSale",
    "TradingSessionStatus",
    "PutthroughDeal",
    "SecuritiesOrganization",
    "StatTicker",
    "TopPrice",
    "TotalSummary"
]
decoder = None
for msg_cls in message_cls:
    if decoder is None:
        decoder = F.when(F.col("class") == F.lit(f"bgv2.{msg_cls}"), F.to_json(from_protobuf("body", f"bgv2.{msg_cls}", desc_file)))
    else:
        decoder = decoder.when(F.col("class") == F.lit(f"bgv2.{msg_cls}"), F.to_json(from_protobuf("body", f"bgv2.{msg_cls}", desc_file)))

decoder = decoder.otherwise(None)
df = df.select(F.col("class").alias("message_class"), decoder.alias("message_body"))

query = (
    df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_path)
    .start(delta_path)
    
)

spark.stop()