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
file_name = "decoder/schema_combined.desc"  # Tên file trên MinIO
download_path = "schema_combined.desc" 

try:
    # Tải file từ MinIO xuống
    client.fget_object(bucket_name, file_name, download_path)
    print(f"File '{file_name}' đã được tải xuống thành công tại {download_path}")
except S3Error as e:
    print(f"Lỗi khi tải file: {e}")

spark = (
    SparkSession.builder
    .appName("CDC_Account_SCD2_Delta")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# Location Delta trên MinIO
delta_path = "s3a://warehouse/silver/market_data_index"
checkpoint_path = "s3a://warehouse/checkpoints/market_data_index_checkpoint"

# =========================
# 2. Schema Debezium payload
# =========================
spark.sql("CREATE DATABASE IF NOT EXISTS silver")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS silver.market_data_index
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
    #.option("subscribe", "rlt.mkt.nd168.tick")
    .option("assign", '''{"rlt.mkt.nd168.tick": [0,1,2]}''')
    .option("startingOffsets", "latest")
    .load()
)

df = df_kafka.select(from_protobuf("value", "google.protobuf.Any", desc_file).alias("msg"))
df = df.select(
    F.split(F.col("msg.type_url"), "\/")[1].cast("string").alias("class"), 
    F.col("msg.value").alias("body")
).filter(F.col("class") == F.lit("bgv2.IndexUpdate"))

df = df.select(F.to_json(from_protobuf("body", "bgv2.IndexUpdate", desc_file)).alias("data"))

schema = StructType([
    StructField("Header", StructType([
        StructField("MessageType", StringType(), True),
        StructField("TradingSession", StringType(), True),
        StructField("CreatedAt", TimestampType(), True),
        StructField("Last_updated", TimestampType(), True)
    ]), True),
    StructField("IndexCode", StringType(), True),
    StructField("MarketCode", StringType(), True),
    StructField("Session", StringType(), True),
    StructField("OriginalSession", StringType(), True),
    StructField("IndexValue", StructType([StructField("value", DoubleType(), True)]), True),
    StructField("ChangedIndexValue", StructType([StructField("value", DoubleType(), True)]), True),
    StructField("ChangedIndexPercentage", StructType([StructField("value", DoubleType(), True)]), True),
    StructField("PreviousIndexValue", StructType([StructField("value", DoubleType(), True)]), True),
    StructField("IndexColor", StringType(), True),
    StructField("TotalTrades", StructType([StructField("value", LongType(), True)]), True),
    StructField("TotalSharesTraded", StructType([StructField("value", LongType(), True)]), True),
    StructField("TotalValuesTraded", StructType([StructField("value", LongType(), True)]), True),
    StructField("TotalSharesTraded4MT", StructType([StructField("value", LongType(), True)]), True),
    StructField("TotalValuesTraded4MT", StructType([StructField("value", LongType(), True)]), True),
    StructField("TotalSharesTraded4PT", StructType([StructField("value", LongType(), True)]), True),
    StructField("TotalValuesTraded4PT", StructType([StructField("value", LongType(), True)]), True),
    StructField("UpVolume", StructType([StructField("value", LongType(), True)]), True),
    StructField("DownVolume", StructType([StructField("value", LongType(), True)]), True),
    StructField("NoChangeVolume", StructType([StructField("value", LongType(), True)]), True),
    StructField("Advances", StructType([StructField("value", LongType(), True)]), True),
    StructField("Declines", StructType([StructField("value", LongType(), True)]), True),
    StructField("NoChange", StructType([StructField("value", LongType(), True)]), True),
    StructField("Time", TimestampType(), True)
])

df_parsed = df.select(from_json(col("data"), schema).alias("data"))

df_flat = df_parsed.select(
    # Cột từ Header
    col("data.Header.MessageType").alias("MessageType"),
    col("data.Header.TradingSession").alias("TradingSession"),
    col("data.Header.CreatedAt").alias("CreatedAt"),
    col("data.Header.Last_updated").alias("Last_updated"),

    # Cột từ các trường khác
    col("data.IndexCode"),
    col("data.MarketCode"),
    col("data.Session"),
    col("data.OriginalSession"),

    # Các trường có dạng nested (có `value` trong một object)
    col("data.IndexValue.value").alias("IndexValue"),
    col("data.ChangedIndexValue.value").alias("ChangedIndexValue"),
    col("data.ChangedIndexPercentage.value").alias("ChangedIndexPercentage"),
    col("data.PreviousIndexValue.value").alias("PreviousIndexValue"),
    col("data.IndexColor"),
    col("data.TotalTrades.value").alias("TotalTrades"),
    col("data.TotalSharesTraded.value").alias("TotalSharesTraded"),
    col("data.TotalValuesTraded.value").alias("TotalValuesTraded"),
    col("data.TotalSharesTraded4MT.value").alias("TotalSharesTraded4MT"),
    col("data.TotalValuesTraded4MT.value").alias("TotalValuesTraded4MT"),
    col("data.TotalSharesTraded4PT.value").alias("TotalSharesTraded4PT"),
    col("data.TotalValuesTraded4PT.value").alias("TotalValuesTraded4PT"),
    col("data.UpVolume.value").alias("UpVolume"),
    col("data.DownVolume.value").alias("DownVolume"),
    col("data.NoChangeVolume.value").alias("NoChangeVolume"),
    col("data.Advances.value").alias("Advances"),
    col("data.Declines.value").alias("Declines"),
    col("data.NoChange.value").alias("NoChange"),
    col("data.Time")
)


query = (
    df_flat.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_path)
)
query = query.start(delta_path)
query.awaitTermination()

