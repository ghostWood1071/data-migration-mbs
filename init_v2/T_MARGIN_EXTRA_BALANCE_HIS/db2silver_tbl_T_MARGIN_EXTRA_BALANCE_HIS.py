from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, date_format, to_date, lit
from pyspark.sql.types import TimestampType

spark = (
    SparkSession.builder
    .appName("CreateDeltaTables")
    .enableHiveSupport()
    .getOrCreate()
)

first_load = True
year = 2025


DATABASE_CONFIG = {
    "url": "jdbc:oracle:thin:@10.91.101.161:1521/tradingnkt",
    "user": "mispoc",
    "password": "d8daa822c8b0c6f9d85",
    "driver": "oracle.jdbc.driver.OracleDriver"
}


def get_data_from_year(year):
    T_MARGIN_EXTRA_BALANCE_HIS_df = (
        spark.read
            .format("jdbc")
            .option("url", DATABASE_CONFIG["url"])
            .option("dbtable", f"(SELECT * FROM BACK.T_MARGIN_EXTRA_BALANCE_HIS WHERE EXTRACT(YEAR FROM C_TRADING_DATE) = {year}) tbl")
            .option("user", DATABASE_CONFIG["user"])
            .option("password", DATABASE_CONFIG["password"])
            .option("driver", DATABASE_CONFIG["driver"])
            .load()
    )
    return T_MARGIN_EXTRA_BALANCE_HIS_df


def transform_silver(df):
    silver_df = (
        df.withColumn("partition_date", to_date("C_TRADING_DATE", "yyyy-MM-dd"))
                                    .withColumn("valid_from", current_timestamp())
                                    .withColumn("valid_to", lit(None).cast(TimestampType()))
                                    .withColumn("is_current", lit(True))
    )
    #
    return silver_df


def create_table(silver_df,  first_load = False):
    spark.sql("CREATE DATABASE IF NOT EXISTS silver")
    (
        silver_df.write.format("delta")
                        .mode("overwrite")
                        .partitionBy("partition_date")
                        .option("path", "s3a://warehouse/silver/T_MARGIN_EXTRA_BALANCE_HIS")
                        .save()
    )
    if first_load:
        spark.sql("""
            CREATE TABLE IF NOT EXISTS silver.fact_T_MARGIN_EXTRA_BALANCE_HIS
            USING delta
            LOCATION 's3a://warehouse/silver/T_MARGIN_EXTRA_BALANCE_HIS'
        """)
       
        
for data_year in range(2016, 2025):
    print(f"start load data from:  {data_year}")
    T_MARGIN_EXTRA_BALANCE_HIS_df = get_data_from_year(data_year)
    silver_df = transform_silver(T_MARGIN_EXTRA_BALANCE_HIS_df)
    create_table(silver_df, False)
    print(f"done load data from:  {data_year}")

spark.stop()
