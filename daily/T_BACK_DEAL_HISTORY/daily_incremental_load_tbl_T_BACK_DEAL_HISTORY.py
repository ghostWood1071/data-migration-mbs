from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, date_format, to_date, lit, col
from pyspark.sql.types import TimestampType, DecimalType

spark = (
    SparkSession.builder
    .appName("CreateDeltaTables")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
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
incremental_df = (
    spark.read
        .format("jdbc")
        .option("url", DATABASE_CONFIG["url"])
        .option("dbtable", "(SELECT * FROM BACK.T_BACK_DEAL_HISTORY WHERE C_TRANSACTION_DATE >= TRUNC(SYSDATE) AND C_TRANSACTION_DATE < TRUNC(SYSDATE) + 1) tbl")
        .option("user", DATABASE_CONFIG["user"])
        .option("password", DATABASE_CONFIG["password"])
        .option("driver", DATABASE_CONFIG["driver"])
        .load()
)
#
###

###---------------------------------INCREMENTAL LOAD DATA TO SILVER LAYER---------------------------------
#
silver_df  = (
        incremental_df.withColumn("partition_date", to_date("C_TRANSACTION_DATE", "yyyy-MM-dd"))
                                .withColumn("valid_from", current_timestamp())
                                .withColumn("valid_to", lit(None).cast(TimestampType()))
                                .withColumn("is_current", lit(True))
                                .withColumn("C_ORDER_NO", col("C_ORDER_NO").cast(DecimalType(38, 10)))
                                .withColumn("C_SUB_ORDER_NO", col("C_SUB_ORDER_NO").cast(DecimalType(38, 10)))
                                .withColumn("C_MATCHED_VOLUME", col("C_MATCHED_VOLUME").cast(DecimalType(38, 10)))
                                .withColumn("C_MATCHED_PRICE", col("C_MATCHED_PRICE").cast(DecimalType(38, 10)))
                                .withColumn("C_COMM_RATE", col("C_COMM_RATE").cast(DecimalType(38, 10)))
                                .withColumn("C_TAX_FLAG", col("C_TAX_FLAG").cast(DecimalType(38, 10)))
                                .withColumn("C_TAX_RATE", col("C_TAX_RATE").cast(DecimalType(38, 10)))
                                .withColumn("C_DEAL_STATUS", col("C_DEAL_STATUS").cast(DecimalType(38, 10)))
                                .withColumn("C_CARE_RATE", col("C_CARE_RATE").cast(DecimalType(38, 10)))  
)


(
    silver_df.write.format("delta")
                .mode("append").partitionBy("partition_date")
                .option("path", "s3a://warehouse/silver/T_BACK_DEAL_HISTORY")
                .save()
)
#
###

spark.stop()
