from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, when, lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

spark = (
    SparkSession.builder
    .appName("CDC_Account_To_StarRocks")
    .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoints/cdc_account_starrocks")
    .getOrCreate()
)

schema_after = StructType([
    StructField("PK_ACCOUNT", StringType(), False),
    StructField("C_ACCOUNT_BRANCH_CODE", StringType(), True),
    StructField("C_ACCOUNT_SUB_BRANCH_CODE", StringType(), True),
    StructField("C_CREATOR_CODE", StringType(), True),
    StructField("C_CREATOR_BRANCH_CODE", StringType(), True),
    StructField("C_CREATOR_SUB_BRANCH_CODE", StringType(), True),
    StructField("C_CREATE_TIME", TimestampType(), True),
    StructField("C_APPROVER_CODE", StringType(), True),
    StructField("C_APPROVE_TIME", TimestampType(), True),
    StructField("C_CUSTOMER_CODE", StringType(), True),
    StructField("C_ACCOUNT_CODE", StringType(), True),
    StructField("C_ACCOUNT_TYPE", StringType(), True),
    StructField("C_ACCOUNT_FRONT_TYPE", StringType(), True),
    StructField("C_ACCOUNT_RELATION_TYPE", StringType(), True),
    StructField("C_STAFF_FLAG", StringType(), True),
    StructField("C_MARKETING_ID", StringType(), True),
    StructField("C_TAX_FLAG", StringType(), True),
    StructField("C_OPEN_DATE", StringType(), True),
    StructField("C_CLOSE_DATE", StringType(), True),
    StructField("C_BANK_CODE", StringType(), True),
    StructField("C_BANK_ACCOUNT", StringType(), True),
    StructField("C_COMM_PACKAGE", StringType(), True),
    StructField("C_ACCOUNT_STATUS", StringType(), True),
    StructField("C_CLOSER_CODE", StringType(), True),
    StructField("C_CLOSE_TIME", TimestampType(), True),
    StructField("C_NEW_CUST_EXPIRE_DATE", StringType(), True),
    StructField("C_COLLABORATOR", StringType(), True),
    StructField("C_MODIFY_USER_CODE", StringType(), True),
    StructField("C_RESTRICTION_ID", StringType(), True),
    StructField("C_CHANNEL", StringType(), True),
    StructField("C_GROUP_CODE", StringType(), True),
    StructField("C_ACCOUNT_CREDIT_TYPE", DoubleType(), True),
    StructField("C_ORIGIN_ACCOUNT_CODE", StringType(), True),
    StructField("C_ACCOUNT_LEVEL", StringType(), True),
    StructField("C_CAN_SHORT_SELL", StringType(), True),
    StructField("C_CAN_OVER_CREDIT", StringType(), True),
    StructField("C_FORCE_SELL", StringType(), True),
    StructField("C_LENDING_SELL", StringType(), True),
    StructField("C_TRADING_STATUS", DoubleType(), True),
    StructField("C_ONLINE_ACCOUNT_TYPE", StringType(), True),
    StructField("C_SUB_MARKETING_ID", StringType(), True),
    StructField("C_CHANGED_TAB", StringType(), True),
    StructField("C_TC_SIGNED_STATUS", DoubleType(), True),
    StructField("C_ODD_LOT_CONTRACT_SIGNED", DoubleType(), True),
    StructField("C_ODD_LOT_CONTRACT_NOTE", StringType(), True),
    StructField("C_DEPOSIT_APPROVER_CODE", StringType(), True),
    StructField("C_DEPOSIT_APPROVE_DATE", StringType(), True),
    StructField("C_COMM_BASE_RATE", DoubleType(), True),
    StructField("C_ATS_FLAG", DoubleType(), True),
    StructField("C_ATS_BANK_ACCOUNT", StringType(), True),
    StructField("C_ATS_BANK", StringType(), True),
    StructField("C_BANK_RESPONSE_MAPPING", StringType(), True),
    StructField("C_RESPONSE_MAPPING_TIME", TimestampType(), True),
    StructField("C_BANK_RESPONSE_UNMAPPING", StringType(), True),
    StructField("C_RESPONSE_UNMAPPING_TIME", TimestampType(), True),
    StructField("C_VSD_STATUS", StringType(), True),
    StructField("C_VSD_RESPONSE", StringType(), True),
    StructField("C_VSD_OPEN_FLAG", DoubleType(), True),
    StructField("C_VSD_CLOSE_FLAG", DoubleType(), True),
    StructField("C_COMM_START_DATE", StringType(), True),
    StructField("C_COMM_EXPIRE_DATE", StringType(), True),
    StructField("C_RECEIVE_FINAN_BILL", DoubleType(), True),
    StructField("C_VSD_OPEN_STATUS", StringType(), True),
    StructField("C_VSD_OPEN_RESPONSE", StringType(), True),
    StructField("C_VSD_OPEN_LOCATION", StringType(), True),
    StructField("C_VSD_CLOSE_STATUS", StringType(), True),
    StructField("C_VSD_CLOSE_RESPONSE", StringType(), True),
    StructField("C_VSD_CLOSE_LOCATION", StringType(), True),
    StructField("C_VSD_OPEN_DATE", StringType(), True),
    StructField("C_VSD_CLOSE_DATE", StringType(), True),
    StructField("C_RECEIVED_FEE_CLOSE_ACC", DoubleType(), True),
    StructField("C_CONFIRM_TYPE", StringType(), True),
    StructField("C_CONFIRM_USER", StringType(), True),
    StructField("FK_CONFIRM_CUSTOMER", StringType(), True),
    StructField("C_FILE_STATUS", StringType(), True),
    StructField("C_FILE_CONFIRM_DATE", StringType(), True),
    StructField("C_FILE_COMPLETING_LOCATION", DoubleType(), True),
    StructField("C_IS_NEW_CUSTOMER", DoubleType(), True),
    StructField("C_FILE_CONFIRM_USER", StringType(), True),
    StructField("C_FILE_CONFIRM_NOTE", StringType(), True),
    StructField("C_SOFT_FILE_STATUS", StringType(), True),
    StructField("C_SOFT_FILE_REJECT_REASON", StringType(), True),
    StructField("C_SOFT_FILE_REJECT_DATE", StringType(), True),
    StructField("C_FILE_STATUS_USER", StringType(), True),
    StructField("C_ADDRESS_SAVE_INFOR", StringType(), True),
    StructField("C_CUST_CARE_SERVICE_TYPE", StringType(), True),
    StructField("C_ATS_BANK_BRANCH_CD", StringType(), True),
    StructField("C_IS_ONBOARD", DoubleType(), True),
    StructField("C_COPI24_TYPE", StringType(), True),
    StructField("C_VSD_OPEN_SEND_TIME", TimestampType(), True),
    StructField("C_FOR_TRADING_BOND", DoubleType(), True),
    StructField("C_VSD_FIRST_OPEN_SEND_TIME", TimestampType(), True),
    StructField("C_CARE_PACKAGE", StringType(), True)
])

schema_cdc = StructType() \
    .add("before", schema_after) \
    .add("after", schema_after) \
    .add("op", StringType())

df_kafka = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "192.168.73.190:9092,192.168.73.191:9092,192.168.73.192:9092")
    .option("subscribe", "oracle-bo-poc.BACK.T_BACK_ACCOUNT")
    .option("startingOffsets", "earliest")  # <-- đọc toàn bộ dữ liệu
    .load()
)

df_json = df_kafka.selectExpr("CAST(value AS STRING) as json_str")

df_parsed = df_json.select(from_json(col("json_str"), schema_cdc).alias("data"))

df_flat = df_parsed.select(
    col("data.after.*"),
    col("data.op").alias("op")
)

def write_to_starrocks(batch_df, epoch_id):
    (
        batch_df.drop("op")
        .write
        .format("starrocks")
        .option("starrocks.fe.http.url", "http://kube-starrocks-fe-service.warehouse.svc.cluster.local:8030")
        .option("starrocks.fe.jdbc.url", "jdbc:mysql://kube-starrocks-fe-service.warehouse.svc.cluster.local:9030")
        .option("starrocks.table.identifier", "mbs_realtime_db.t_back_account")
        .option("starrocks.user", "mbs_demo")
        .option("starrocks.password", "mbs_demo")
        .option("sink.properties.enable_upsert_delete", "true")
        .option("sink.properties.format", "json")
        .option("sink.buffer.flush.max.rows", "5000")
        .mode("append")
        .save()
    )

(
    df_ready.writeStream
    .outputMode("append")
    .foreachBatch(write_to_starrocks)
    .option("checkpointLocation", "/tmp/checkpoints/cdc_account_starrocks")
    .start()
    .awaitTermination()
)
