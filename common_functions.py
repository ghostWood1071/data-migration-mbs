from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import current_timestamp


def load_data_from_oracledb_to_bronze(db_schema: str, table: str, partition_column: str | list[str] = None):
    spark = (
        SparkSession.builder
        .appName("LoadDataFromOracleToBronze")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    
    #database config to connect
    DATABASE_CONFIG = {
        "url": "jdbc:oracle:thin:@10.91.101.161:1521/tradingnkt",
        "user": "mispoc",
        "password": "d8daa822c8b0c6f9d85",
        "driver": "oracle.jdbc.driver.OracleDriver"
    }
    
    #READ DATA FROM ORACLE DB
    source_df = (
        spark.read
            .format("jdbc")
            .option("url", DATABASE_CONFIG["url"])
            .option("dbtable", f"{db_schema.upper()}.{table.upper()}")
            .option("user", DATABASE_CONFIG["user"])
            .option("password", DATABASE_CONFIG["password"])
            .option("driver", DATABASE_CONFIG["driver"])
            .load()
    )
    #WRITE DATA TO BRONZE BUCKET IN MINIO
    if partition_column:
        (
            source_df.write
                    .format("parquet")
                    .mode("overwrite")
                    .partitionBy(partition_column)
                    .save(f"s3a://warehouse/bronze/{table.upper()}")
        )
    else:
        (
            source_df.write
                    .format("parquet")
                    .mode("overwrite")
                    .save(f"s3a://warehouse/bronze/{table.upper()}")
        )
    
    spark.stop()
    
    
def write_source_from_bronze_to_silver_table(schema_structure: str | StructType, table_category: str, table: str, ):
    spark = (
        SparkSession.builder
        .appName("LoadDataFromOracleToBronze")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .enableHiveSupport()
        .getOrCreate()
    )
    
    #create silver, gold databases
    spark.sql("CREATE DATABASE IF NOT EXISTS silver")
    spark.sql("CREATE DATABASE IF NOT EXISTS gold")
    
    #read data from bronze
    source_df = (
        spark.read.format("parquet").schema(schema_structure).load(f"s3a://warehouse/bronze/{table}")
    )
    
    #add technique column
    (
        source_df.withColumn("partition_date", "")
                .withColumn("valid_from", current_timestamp())
                .withColumn("valid_to", None)
                .withColumn("is_current", True)
    )
    
    #write table
    (
        source_df.write.format("delta")
                    .mode("overwrite")
                    .option("path", f"s3a://warehouse/silver/{table}")
                    .saveAsTable(f"silver.{table_category}_{table}")
    )
