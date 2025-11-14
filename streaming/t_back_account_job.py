from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, when, lit, coalesce
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, TimestampType, IntegerType
)

# =========================================================
# 1. SparkSession
# =========================================================
spark = (
    SparkSession.builder
    .appName("CDC_Account_To_StarRocks")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# =========================================================
# 2. Schema after / before (unwrap Debezium)
# =========================================================
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
    StructField("C_OPEN_DATE", TimestampType(), True),
    StructField("C_CLOSE_DATE", TimestampType(), True),
    StructField("C_BANK_CODE", StringType(), True),
    StructField("C_BANK_ACCOUNT", StringType(), True),
    StructField("C_COMM_PACKAGE", StringType(), True),
    StructField("C_ACCOUNT_STATUS", StringType(), True),
    StructField("C_CLOSER_CODE", StringType(), True),
    StructField("C_CLOSE_TIME", TimestampType(), True),
    StructField("C_NEW_CUST_EXPIRE_DATE", TimestampType(), True),
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
    StructField("C_DEPOSIT_APPROVE_DATE", TimestampType(), True),
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
    StructField("C_COMM_START_DATE", TimestampType(), True),
    StructField("C_COMM_EXPIRE_DATE", TimestampType(), True),
    StructField("C_RECEIVE_FINAN_BILL", DoubleType(), True),
    StructField("C_VSD_OPEN_STATUS", StringType(), True),
    StructField("C_VSD_OPEN_RESPONSE", StringType(), True),
    StructField("C_VSD_OPEN_LOCATION", StringType(), True),
    StructField("C_VSD_CLOSE_STATUS", StringType(), True),
    StructField("C_VSD_CLOSE_RESPONSE", StringType(), True),
    StructField("C_VSD_CLOSE_LOCATION", StringType(), True),
    StructField("C_VSD_OPEN_DATE", TimestampType(), True),
    StructField("C_VSD_CLOSE_DATE", TimestampType(), True),
    StructField("C_RECEIVED_FEE_CLOSE_ACC", DoubleType(), True),
    StructField("C_CONFIRM_TYPE", StringType(), True),
    StructField("C_CONFIRM_USER", StringType(), True),
    StructField("FK_CONFIRM_CUSTOMER", StringType(), True),
    StructField("C_FILE_STATUS", StringType(), True),
    StructField("C_FILE_CONFIRM_DATE", TimestampType(), True),
    StructField("C_FILE_COMPLETING_LOCATION", DoubleType(), True),
    StructField("C_IS_NEW_CUSTOMER", DoubleType(), True),
    StructField("C_FILE_CONFIRM_USER", StringType(), True),
    StructField("C_FILE_CONFIRM_NOTE", StringType(), True),
    StructField("C_SOFT_FILE_STATUS", StringType(), True),
    StructField("C_SOFT_FILE_REJECT_REASON", StringType(), True),
    StructField("C_SOFT_FILE_REJECT_DATE", TimestampType(), True),
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

# # Dùng để set "columns" cho StarRocks (tất cả cột + __op)
# columns_for_starrocks = ",".join([f.name for f in schema_after.fields] + ["__op"])

# =========================================================
# 3. Đọc Kafka (Debezium unwrap)
# =========================================================
df_kafka = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "192.168.73.190:9092,192.168.73.191:9092,192.168.73.192:9092")
    .option("subscribe", "oracle-bo-poc.BACK.T_BACK_ACCOUNT")
    .option("startingOffsets", "earliest")
    .load()
)

df_json = df_kafka.selectExpr("CAST(value AS STRING) AS json_str")

df_parsed = df_json.select(from_json(col("json_str"), schema_cdc).alias("data"))

# =========================================================
# 4. Flatten + CDC (__op)
#    - PK_ACCOUNT: lấy after, nếu null thì lấy before (delete)
#    - __op: 0 = upsert (c,r,u), 1 = delete (d)
# =========================================================
df_flat = df_parsed.select(
    # PK luôn có giá trị
    coalesce(col("data.after.PK_ACCOUNT"), col("data.before.PK_ACCOUNT")).alias("PK_ACCOUNT"),

    col("data.after.C_ACCOUNT_BRANCH_CODE").alias("C_ACCOUNT_BRANCH_CODE"),
    col("data.after.C_ACCOUNT_SUB_BRANCH_CODE").alias("C_ACCOUNT_SUB_BRANCH_CODE"),
    col("data.after.C_CREATOR_CODE").alias("C_CREATOR_CODE"),
    col("data.after.C_CREATOR_BRANCH_CODE").alias("C_CREATOR_BRANCH_CODE"),
    col("data.after.C_CREATOR_SUB_BRANCH_CODE").alias("C_CREATOR_SUB_BRANCH_CODE"),
    col("data.after.C_CREATE_TIME").alias("C_CREATE_TIME"),
    col("data.after.C_APPROVER_CODE").alias("C_APPROVER_CODE"),
    col("data.after.C_APPROVE_TIME").alias("C_APPROVE_TIME"),
    col("data.after.C_CUSTOMER_CODE").alias("C_CUSTOMER_CODE"),
    col("data.after.C_ACCOUNT_CODE").alias("C_ACCOUNT_CODE"),
    col("data.after.C_ACCOUNT_TYPE").alias("C_ACCOUNT_TYPE"),
    col("data.after.C_ACCOUNT_FRONT_TYPE").alias("C_ACCOUNT_FRONT_TYPE"),
    col("data.after.C_ACCOUNT_RELATION_TYPE").alias("C_ACCOUNT_RELATION_TYPE"),
    col("data.after.C_STAFF_FLAG").alias("C_STAFF_FLAG"),
    col("data.after.C_MARKETING_ID").alias("C_MARKETING_ID"),
    col("data.after.C_TAX_FLAG").alias("C_TAX_FLAG"),
    col("data.after.C_OPEN_DATE").alias("C_OPEN_DATE"),
    col("data.after.C_CLOSE_DATE").alias("C_CLOSE_DATE"),
    col("data.after.C_BANK_CODE").alias("C_BANK_CODE"),
    col("data.after.C_BANK_ACCOUNT").alias("C_BANK_ACCOUNT"),
    col("data.after.C_COMM_PACKAGE").alias("C_COMM_PACKAGE"),
    col("data.after.C_ACCOUNT_STATUS").alias("C_ACCOUNT_STATUS"),
    col("data.after.C_CLOSER_CODE").alias("C_CLOSER_CODE"),
    col("data.after.C_CLOSE_TIME").alias("C_CLOSE_TIME"),
    col("data.after.C_NEW_CUST_EXPIRE_DATE").alias("C_NEW_CUST_EXPIRE_DATE"),
    col("data.after.C_COLLABORATOR").alias("C_COLLABORATOR"),
    col("data.after.C_MODIFY_USER_CODE").alias("C_MODIFY_USER_CODE"),
    col("data.after.C_RESTRICTION_ID").alias("C_RESTRICTION_ID"),
    col("data.after.C_CHANNEL").alias("C_CHANNEL"),
    col("data.after.C_GROUP_CODE").alias("C_GROUP_CODE"),
    col("data.after.C_ACCOUNT_CREDIT_TYPE").alias("C_ACCOUNT_CREDIT_TYPE"),
    col("data.after.C_ORIGIN_ACCOUNT_CODE").alias("C_ORIGIN_ACCOUNT_CODE"),
    col("data.after.C_ACCOUNT_LEVEL").alias("C_ACCOUNT_LEVEL"),
    col("data.after.C_CAN_SHORT_SELL").alias("C_CAN_SHORT_SELL"),
    col("data.after.C_CAN_OVER_CREDIT").alias("C_CAN_OVER_CREDIT"),
    col("data.after.C_FORCE_SELL").alias("C_FORCE_SELL"),
    col("data.after.C_LENDING_SELL").alias("C_LENDING_SELL"),
    col("data.after.C_TRADING_STATUS").alias("C_TRADING_STATUS"),
    col("data.after.C_ONLINE_ACCOUNT_TYPE").alias("C_ONLINE_ACCOUNT_TYPE"),
    col("data.after.C_SUB_MARKETING_ID").alias("C_SUB_MARKETING_ID"),
    col("data.after.C_CHANGED_TAB").alias("C_CHANGED_TAB"),
    col("data.after.C_TC_SIGNED_STATUS").alias("C_TC_SIGNED_STATUS"),
    col("data.after.C_ODD_LOT_CONTRACT_SIGNED").alias("C_ODD_LOT_CONTRACT_SIGNED"),
    col("data.after.C_ODD_LOT_CONTRACT_NOTE").alias("C_ODD_LOT_CONTRACT_NOTE"),
    col("data.after.C_DEPOSIT_APPROVER_CODE").alias("C_DEPOSIT_APPROVER_CODE"),
    col("data.after.C_DEPOSIT_APPROVE_DATE").alias("C_DEPOSIT_APPROVE_DATE"),
    col("data.after.C_COMM_BASE_RATE").alias("C_COMM_BASE_RATE"),
    col("data.after.C_ATS_FLAG").alias("C_ATS_FLAG"),
    col("data.after.C_ATS_BANK_ACCOUNT").alias("C_ATS_BANK_ACCOUNT"),
    col("data.after.C_ATS_BANK").alias("C_ATS_BANK"),
    col("data.after.C_BANK_RESPONSE_MAPPING").alias("C_BANK_RESPONSE_MAPPING"),
    col("data.after.C_RESPONSE_MAPPING_TIME").alias("C_RESPONSE_MAPPING_TIME"),
    col("data.after.C_BANK_RESPONSE_UNMAPPING").alias("C_BANK_RESPONSE_UNMAPPING"),
    col("data.after.C_RESPONSE_UNMAPPING_TIME").alias("C_RESPONSE_UNMAPPING_TIME"),
    col("data.after.C_VSD_STATUS").alias("C_VSD_STATUS"),
    col("data.after.C_VSD_RESPONSE").alias("C_VSD_RESPONSE"),
    col("data.after.C_VSD_OPEN_FLAG").alias("C_VSD_OPEN_FLAG"),
    col("data.after.C_VSD_CLOSE_FLAG").alias("C_VSD_CLOSE_FLAG"),
    col("data.after.C_COMM_START_DATE").alias("C_COMM_START_DATE"),
    col("data.after.C_COMM_EXPIRE_DATE").alias("C_COMM_EXPIRE_DATE"),
    col("data.after.C_RECEIVE_FINAN_BILL").alias("C_RECEIVE_FINAN_BILL"),
    col("data.after.C_VSD_OPEN_STATUS").alias("C_VSD_OPEN_STATUS"),
    col("data.after.C_VSD_OPEN_RESPONSE").alias("C_VSD_OPEN_RESPONSE"),
    col("data.after.C_VSD_OPEN_LOCATION").alias("C_VSD_OPEN_LOCATION"),
    col("data.after.C_VSD_CLOSE_STATUS").alias("C_VSD_CLOSE_STATUS"),
    col("data.after.C_VSD_CLOSE_RESPONSE").alias("C_VSD_CLOSE_RESPONSE"),
    col("data.after.C_VSD_CLOSE_LOCATION").alias("C_VSD_CLOSE_LOCATION"),
    col("data.after.C_VSD_OPEN_DATE").alias("C_VSD_OPEN_DATE"),
    col("data.after.C_VSD_CLOSE_DATE").alias("C_VSD_CLOSE_DATE"),
    col("data.after.C_RECEIVED_FEE_CLOSE_ACC").alias("C_RECEIVED_FEE_CLOSE_ACC"),
    col("data.after.C_CONFIRM_TYPE").alias("C_CONFIRM_TYPE"),
    col("data.after.C_CONFIRM_USER").alias("C_CONFIRM_USER"),
    col("data.after.FK_CONFIRM_CUSTOMER").alias("FK_CONFIRM_CUSTOMER"),
    col("data.after.C_FILE_STATUS").alias("C_FILE_STATUS"),
    col("data.after.C_FILE_CONFIRM_DATE").alias("C_FILE_CONFIRM_DATE"),
    col("data.after.C_FILE_COMPLETING_LOCATION").alias("C_FILE_COMPLETING_LOCATION"),
    col("data.after.C_IS_NEW_CUSTOMER").alias("C_IS_NEW_CUSTOMER"),
    col("data.after.C_FILE_CONFIRM_USER").alias("C_FILE_CONFIRM_USER"),
    col("data.after.C_FILE_CONFIRM_NOTE").alias("C_FILE_CONFIRM_NOTE"),
    col("data.after.C_SOFT_FILE_STATUS").alias("C_SOFT_FILE_STATUS"),
    col("data.after.C_SOFT_FILE_REJECT_REASON").alias("C_SOFT_FILE_REJECT_REASON"),
    col("data.after.C_SOFT_FILE_REJECT_DATE").alias("C_SOFT_FILE_REJECT_DATE"),
    col("data.after.C_FILE_STATUS_USER").alias("C_FILE_STATUS_USER"),
    col("data.after.C_ADDRESS_SAVE_INFOR").alias("C_ADDRESS_SAVE_INFOR"),
    col("data.after.C_CUST_CARE_SERVICE_TYPE").alias("C_CUST_CARE_SERVICE_TYPE"),
    col("data.after.C_ATS_BANK_BRANCH_CD").alias("C_ATS_BANK_BRANCH_CD"),
    col("data.after.C_IS_ONBOARD").alias("C_IS_ONBOARD"),
    col("data.after.C_COPI24_TYPE").alias("C_COPI24_TYPE"),
    col("data.after.C_VSD_OPEN_SEND_TIME").alias("C_VSD_OPEN_SEND_TIME"),
    col("data.after.C_FOR_TRADING_BOND").alias("C_FOR_TRADING_BOND"),
    col("data.after.C_VSD_FIRST_OPEN_SEND_TIME").alias("C_VSD_FIRST_OPEN_SEND_TIME"),
    col("data.after.C_CARE_PACKAGE").alias("C_CARE_PACKAGE"),

    when(col("data.op") == lit("d"), lit(1))
        .otherwise(lit(0))
        .cast(IntegerType())
        .alias("__op")
)

# =========================================================
# 5. writeStream → StarRocks (CDC với __op)
# =========================================================
columns_for_starrocks = ",".join(df_flat.columns)
(
    df_flat.writeStream
    .format("starrocks")
    .outputMode("append")
    .option("checkpointLocation", "/tmp/checkpoints/cdc_account_starrocks")
    .option("starrocks.fe.http.url", "http://kube-starrocks-fe-service.warehouse.svc.cluster.local:8030")
    .option("starrocks.fe.jdbc.url", "jdbc:mysql://kube-starrocks-fe-service.warehouse.svc.cluster.local:9030")
    .option("starrocks.table.identifier", "mbs_realtime_db.t_back_account")
    .option("starrocks.user", "mbs_demo")
    .option("starrocks.password", "mbs_demo")
    # cấu hình cho stream load
    .option("starrocks.write.properties.format", "json")
    .option("starrocks.write.properties.columns", columns_for_starrocks)
    # tuỳ chọn giảm số request / tối ưu connection:
    .option("starrocks.write.buffer.rows", "5000")      # tăng lên nếu muốn ít batch hơn
    .option("starrocks.write.buffer.flush.interval.ms", "3000")
    .start()
    .awaitTermination()
)
