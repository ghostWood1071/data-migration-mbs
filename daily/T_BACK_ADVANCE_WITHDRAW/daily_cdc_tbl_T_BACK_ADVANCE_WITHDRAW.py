from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

spark = (
    SparkSession.builder
    .appName("cdc_tbl_T_BACK_ADVANCE_WITHDRAW")
    .enableHiveSupport()
    .getOrCreate()
)


###---------------------------------READ DATA FROM BRONZE---------------------------------
#
source_df = (
    spark.read
        .format("parquet")
        .load("s3a://warehouse/bronze/T_BACK_ADVANCE_WITHDRAW")
)
#
###

###---------------------------------PROCESS UPSERT ACTION---------------------------------
#
all_columns_list = source_df.columns
all_columns_str = ", ".join(all_columns_list)
all_columns_comparison = " OR ".join([f"tgt.{c} <> src.{c}" for c in all_columns_list])
all_columns_insertion = ', '.join([f"src.{c}" for c in source_df.columns])
all_columns_assignment = ", ".join([f"tgt.{c} = src.{c}" for c in all_columns_list])
src_prefix_all_columns_name = ", ".join([f"src.{c}" for c in all_columns_list])


source_df.createOrReplaceTempView("source_tmp_view")

spark.sql(f"""
    MERGE INTO silver.fact_T_BACK_ADVANCE_WITHDRAW AS tgt
    USING source_tmp_view AS src
    ON tgt.PK_ADVANCE_WITHDRAW = src.PK_ADVANCE_WITHDRAW
    WHEN MATCHED AND tgt.is_current = TRUE AND ({all_columns_comparison})
    THEN UPDATE SET
        tgt.is_current = FALSE,
        tgt.valid_to = CURRENT_TIMESTAMP()
""")

spark.sql(f"""
    INSERT INTO silver.fact_T_BACK_ADVANCE_WITHDRAW ({all_columns_str}, partition_date, valid_from, valid_to, is_current, create_at)
    SELECT 
        {src_prefix_all_columns_name},
        TO_DATE(C_APPROVE_TIME) AS partition_date,
        CURRENT_TIMESTAMP() AS valid_from,
        CAST(NULL AS TIMESTAMP) AS valid_to,
        TRUE AS is_current,
        CURRENT_TIMESTAMP() AS create_at
    FROM source_tmp_view src
    LEFT JOIN silver.fact_T_BACK_ADVANCE_WITHDRAW tgt
    ON src.PK_ADVANCE_WITHDRAW = tgt.PK_ADVANCE_WITHDRAW AND tgt.is_current = TRUE
    WHERE tgt.PK_ADVANCE_WITHDRAW IS NULL
       OR ({all_columns_comparison})
""")
#
###