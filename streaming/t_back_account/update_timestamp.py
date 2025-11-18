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
    .appName("data-update-timestamp")
    .enableHiveSupport()
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")
spark.sql('''
    UPDATE silver.t_back_account_scd2 
    SET updated_at = from_utc_timestamp(updated_at, 'Asia/Ho_Chi_Minh'),
        created_at = from_utc_timestamp(created_at, 'Asia/Ho_Chi_Minh')
''')
spark.sql('''
    UPDATE silver.t_front_deal_scd2 
    SET updated_at = from_utc_timestamp(updated_at, 'Asia/Ho_Chi_Minh')
        created_at = from_utc_timestamp(created_at, 'Asia/Ho_Chi_Minh')
''')
spark.stop()