from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

spark = (
    SparkSession.builder
    .appName("CreateDeltaTables")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .enableHiveSupport()
    .getOrCreate()
)

#database config 
DATABASE_CONFIG_DICT = {
    "url": "jdbc:oracle:thin:@10.91.101.161:1521/tradingnkt",
    "user": "mispoc",
    "password": "d8daa822c8b0c6f9d85",
    "driver": "oracle.jdbc.driver.OracleDriver"
}

###---------------------------------READ DATA FROM ORACLE DB---------------------------------
#
T_BACK_ADVANCE_WITHDRAW_df = (
    spark.read
        .format("jdbc")
        .option("url", DATABASE_CONFIG_DICT["url"])
        .option("dbtable", "T_BACK_ADVANCE_WITHDRAW")
        .option("user", DATABASE_CONFIG_DICT["user"])
        .option("password", DATABASE_CONFIG_DICT["password"])
        .option("driver", DATABASE_CONFIG_DICT["driver"])
        .load()
)

T_BACK_DEAL_HISTORY_df = (
    spark.read
        .format("jdbc")
        .option("url", DATABASE_CONFIG_DICT["url"])
        .option("dbtable", "T_BACK_DEAL_HISTORY")
        .option("user", DATABASE_CONFIG_DICT["user"])
        .option("password", DATABASE_CONFIG_DICT["password"])
        .option("driver", DATABASE_CONFIG_DICT["driver"])
        .load()
)

T_FRONT_DEAL_df = (
    spark.read
        .format("jdbc")
        .option("url", DATABASE_CONFIG_DICT["url"])
        .option("dbtable", "T_FRONT_DEAL")
        .option("user", DATABASE_CONFIG_DICT["user"])
        .option("password", DATABASE_CONFIG_DICT["password"])
        .option("driver", DATABASE_CONFIG_DICT["driver"])
        .load()
)

T_LIST_BRANCH_BANK_ADV_WDR_df = (
    spark.read
        .format("jdbc")
        .option("url", DATABASE_CONFIG_DICT["url"])
        .option("dbtable", "T_LIST_BRANCH_BANK_ADV_WDR")
        .option("user", DATABASE_CONFIG_DICT["user"])
        .option("password", DATABASE_CONFIG_DICT["password"])
        .option("driver", DATABASE_CONFIG_DICT["driver"])
        .load()
)

T_MARGIN_EXTRA_BALANCE_HIS_df = (
    spark.read
        .format("jdbc")
        .option("url", DATABASE_CONFIG_DICT["url"])
        .option("dbtable", "T_MARGIN_EXTRA_BALANCE_HIS")
        .option("user", DATABASE_CONFIG_DICT["user"])
        .option("password", DATABASE_CONFIG_DICT["password"])
        .option("driver", DATABASE_CONFIG_DICT["driver"])
        .load()
)

T_TLO_DEBIT_BALANCE_HISTORY_df = (
    spark.read
        .format("jdbc")
        .option("url", DATABASE_CONFIG_DICT["url"])
        .option("dbtable", "T_TLO_DEBIT_BALANCE_HISTORY")
        .option("user", DATABASE_CONFIG_DICT["user"])
        .option("password", DATABASE_CONFIG_DICT["password"])
        .option("driver", DATABASE_CONFIG_DICT["driver"])
        .load()
)

V_T_BACK_ACCOUNT_df = (
    spark.read
        .format("jdbc")
        .option("url", DATABASE_CONFIG_DICT["url"])
        .option("dbtable", "V_T_BACK_ACCOUNT")
        .option("user", DATABASE_CONFIG_DICT["user"])
        .option("password", DATABASE_CONFIG_DICT["password"])
        .option("driver", DATABASE_CONFIG_DICT["driver"])
        .load()
)

V_T_ERC_MONTHLY_DETAIL_df = (
    spark.read
        .format("jdbc")
        .option("url", DATABASE_CONFIG_DICT["url"])
        .option("dbtable", "V_T_ERC_MONTHLY_DETAIL")
        .option("user", DATABASE_CONFIG_DICT["user"])
        .option("password", DATABASE_CONFIG_DICT["password"])
        .option("driver", DATABASE_CONFIG_DICT["driver"])
        .load()
)

V_T_LIST_FRONT_USER_df = (
    spark.read
        .format("jdbc")
        .option("url", DATABASE_CONFIG_DICT["url"])
        .option("dbtable", "V_T_LIST_FRONT_USER")
        .option("user", DATABASE_CONFIG_DICT["user"])
        .option("password", DATABASE_CONFIG_DICT["password"])
        .option("driver", DATABASE_CONFIG_DICT["driver"])
        .load()
)
#
###

###---------------------------------ADD INGEST TIME COLUMN---------------------------------
#
T_BACK_ADVANCE_WITHDRAW_df.withColumn("ingest_time", current_timestamp())
T_BACK_DEAL_HISTORY_df.withColumn("ingest_time", current_timestamp())
T_FRONT_DEAL_df.withColumn("ingest_time", current_timestamp())
T_LIST_BRANCH_BANK_ADV_WDR_df.withColumn("ingest_time", current_timestamp())
T_MARGIN_EXTRA_BALANCE_HIS_df.withColumn("ingest_time", current_timestamp())
T_TLO_DEBIT_BALANCE_HISTORY_df.withColumn("ingest_time", current_timestamp())
V_T_BACK_ACCOUNT_df.withColumn("ingest_time", current_timestamp())
V_T_ERC_MONTHLY_DETAIL_df.withColumn("ingest_time", current_timestamp())
V_T_LIST_FRONT_USER_df.withColumn("ingest_time", current_timestamp())
#
###


###---------------------------------WRITE DATA TO BRONZE BUCKET IN MINIO---------------------------------
#
T_BACK_ADVANCE_WITHDRAW_df.write.format("delta").mode("overwrite").save("s3a://warehouse/bronze/T_BACK_ADVANCE_WITHDRAW")
T_BACK_DEAL_HISTORY_df.write.format("delta").mode("overwrite").save("s3a://warehouse/bronze/T_BACK_DEAL_HISTORY")
T_FRONT_DEAL_df.write.format("delta").mode("overwrite").save("s3a://warehouse/bronze/T_FRONT_DEAL")
T_LIST_BRANCH_BANK_ADV_WDR_df.write.format("delta").mode("overwrite").save("s3a://warehouse/bronze/T_LIST_BRANCH_BANK_ADV_WDR")
T_MARGIN_EXTRA_BALANCE_HIS_df.write.format("delta").mode("overwrite").save("s3a://warehouse/bronze/T_MARGIN_EXTRA_BALANCE_HIS")
T_TLO_DEBIT_BALANCE_HISTORY_df.write.format("delta").mode("overwrite").save("s3a://warehouse/bronze/T_TLO_DEBIT_BALANCE_HISTORY")
V_T_BACK_ACCOUNT_df.write.format("delta").mode("overwrite").save("s3a://warehouse/bronze/V_T_BACK_ACCOUNT")
V_T_ERC_MONTHLY_DETAIL_df.write.format("delta").mode("overwrite").save("s3a://warehouse/bronze/V_T_ERC_MONTHLY_DETAIL")
V_T_LIST_FRONT_USER_df.write.format("delta").mode("overwrite").save("s3a://warehouse/bronze/V_T_LIST_FRONT_USER")
#
###

spark.stop()