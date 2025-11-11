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

###---------------------------------READ DATA FROM BRONZE LAYER---------------------------------
#
T_BACK_ADVANCE_WITHDRAW_df = (
    spark.read
        .format("delta")
        .option("inferSchema", "true")
        .load("s3a://warehouse/bronze/T_BACK_ADVANCE_WITHDRAW")
)

T_BACK_DEAL_HISTORY_df = (
    spark.read
        .format("delta")
        .option("inferSchema", "true")
        .load("s3a://warehouse/bronze/T_BACK_DEAL_HISTORY")
)

T_FRONT_DEAL_df = (
    spark.read
        .format("delta")
        .option("inferSchema", "true")
        .load("s3a://warehouse/bronze/T_FRONT_DEAL")
)

T_LIST_BRANCH_BANK_ADV_WDR_df = (
    spark.read
        .format("delta")
        .option("inferSchema", "true")
        .load("s3a://warehouse/bronze/T_LIST_BRANCH_BANK_ADV_WDR_")
)

T_MARGIN_EXTRA_BALANCE_HIS_df = (
    spark.read
        .format("delta")
        .option("inferSchema", "true")
        .load("s3a://warehouse/bronze/T_MARGIN_EXTRA_BALANCE_HIS")
)

T_TLO_DEBIT_BALANCE_HISTORY_df = (
    spark.read
        .format("delta")
        .option("inferSchema", "true")
        .load("s3a://warehouse/bronze/T_TLO_DEBIT_BALANCE_HISTORY")
)

V_T_BACK_ACCOUNT_df = (
    spark.read
        .format("delta")
        .option("inferSchema", "true")
        .load("s3a://warehouse/bronze/V_T_BACK_ACCOUNT")
)

V_T_ERC_MONTHLY_DETAIL_df = (
    spark.read
        .format("delta")
        .option("inferSchema", "true")
        .load("s3a://warehouse/bronze/V_T_ERC_MONTHLY_DETAIL_df")
)

V_T_LIST_FRONT_USER_df = (
    spark.read
        .format("delta")
        .option("inferSchema", "true")
        .load("s3a://warehouse/bronze/V_T_LIST_FRONT_USER")
)
#
###

###---------------------------------ADD TECHNIQUE COLUMNS---------------------------------
#
T_BACK_ADVANCE_WITHDRAW_df.withColumns({
        "valid_to": current_timestamp(),
        "valid_from": None,
        "is_current": True
    })

T_BACK_DEAL_HISTORY_df.withColumns({
        "valid_to": current_timestamp(),
        "valid_from": None,
        "is_current": True
    })

T_FRONT_DEAL_df.withColumns({
        "valid_to": current_timestamp(),
        "valid_from": None,
        "is_current": True
    })

T_LIST_BRANCH_BANK_ADV_WDR_df.withColumns({
        "valid_to": current_timestamp(),
        "valid_from": None,
        "is_current": True
    })

T_MARGIN_EXTRA_BALANCE_HIS_df.withColumns({
        "valid_to": current_timestamp(),
        "valid_from": None,
        "is_current": True
    })

T_TLO_DEBIT_BALANCE_HISTORY_df.withColumns({
        "valid_to": current_timestamp(),
        "valid_from": None,
        "is_current": True
    })

V_T_BACK_ACCOUNT_df.withColumns({
        "valid_to": current_timestamp(),
        "valid_from": None,
        "is_current": True
    })

V_T_ERC_MONTHLY_DETAIL_df.withColumns({
        "valid_to": current_timestamp(),
        "valid_from": None,
        "is_current": True
    })

V_T_LIST_FRONT_USER_df.withColumns({
        "valid_to": current_timestamp(),
        "valid_from": None,
        "is_current": True
    })        
#
###

###---------------------------------CREATE DATABASE---------------------------------
#
spark.sql("CREATE DATABASE IF NOT EXISTS silver")
spark.sql("CREATE DATABASE IF NOT EXISTS gold")
#
###

###---------------------------------WRITE TABLE TO SILVER LAYER---------------------------------
#
T_BACK_ADVANCE_WITHDRAW_df.write.format("delta") \
                            .mode("overwrite") \
                            .option("path", "s3a://warehouse/silver/T_BACK_ADVANCE_WITHDRAW") \
                            .saveAsTable("silver.T_BACK_ADVANCE_WITHDRAW")
                            
T_BACK_DEAL_HISTORY_df.write.format("delta") \
                            .mode("overwrite") \
                            .option("path", "s3a://warehouse/silver/T_BACK_DEAL_HISTORY") \
                            .saveAsTable("silver.T_BACK_DEAL_HISTORY")
                            
T_FRONT_DEAL_df.write.format("delta") \
                            .mode("overwrite") \
                            .option("path", "s3a://warehouse/silver/T_FRONT_DEAL") \
                            .saveAsTable("silver.T_FRONT_DEAL")

T_LIST_BRANCH_BANK_ADV_WDR_df.write.format("delta") \
                            .mode("overwrite") \
                            .option("path", "s3a://warehouse/silver/T_LIST_BRANCH_BANK_ADV_WDR") \
                            .saveAsTable("silver.T_LIST_BRANCH_BANK_ADV_WDR")

T_MARGIN_EXTRA_BALANCE_HIS_df.write.format("delta") \
                            .mode("overwrite") \
                            .option("path", "s3a://warehouse/silver/T_MARGIN_EXTRA_BALANCE_HIS") \
                            .saveAsTable("silver.T_MARGIN_EXTRA_BALANCE_HIS")
                            
T_TLO_DEBIT_BALANCE_HISTORY_df.write.format("delta") \
                            .mode("overwrite") \
                            .option("path", "s3a://warehouse/silver/T_TLO_DEBIT_BALANCE_HISTORY") \
                            .saveAsTable("silver.T_TLO_DEBIT_BALANCE_HISTORY")
                            
V_T_BACK_ACCOUNT_df.write.format("delta") \
                            .mode("overwrite") \
                            .option("path", "s3a://warehouse/silver/V_T_BACK_ACCOUNT") \
                            .saveAsTable("silver.V_T_BACK_ACCOUNT")
                            
V_T_ERC_MONTHLY_DETAIL_df.write.format("delta") \
                            .mode("overwrite") \
                            .option("path", "s3a://warehouse/silver/V_T_ERC_MONTHLY_DETAIL") \
                            .saveAsTable("silver.V_T_ERC_MONTHLY_DETAIL")
                            
V_T_LIST_FRONT_USER_df.write.format("delta") \
                            .mode("overwrite") \
                            .option("path", "s3a://warehouse/silver/V_T_LIST_FRONT_USER") \
                            .saveAsTable("silver.V_T_LIST_FRONT_USER")
#
###













