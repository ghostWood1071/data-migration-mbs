from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
                .appName("db2starrocks_transform_dim_erc_ranking")
                .getOrCreate()
)

def get_data_from_starrocks(table_name: str):
    df = (
        spark.read.format("starrocks")
            .option("starrocks.fe.http.url", "http://kube-starrocks-fe-service.warehouse.svc.cluster.local:8030")
            .option("starrocks.fe.jdbc.url", "jdbc:mysql://kube-starrocks-fe-service.warehouse.svc.cluster.local:9030")
            .option("starrocks.table.identifier", f"mbs_realtime_db.{table_name}")
            .option("starrocks.user", "mbs_demo")
            .option("starrocks.password", "mbs_demo")
            .load()
    )
    return df

def load_data_to_tbl_in_starrocks(transformed_df, table_name):
    (
        transformed_df.write.format("starrocks")
        .option("starrocks.fe.http.url", "http://kube-starrocks-fe-service.warehouse.svc.cluster.local:8030")
        .option("starrocks.fe.jdbc.url", "jdbc:mysql://kube-starrocks-fe-service.warehouse.svc.cluster.local:9030")
        .option("starrocks.table.identifier", f"mbs_golden.{table_name}")
        .option("starrocks.filter.query.merge", "false")
        .option("starrocks.user", "mbs_demo")
        .option("starrocks.password", "mbs_demo")
        .mode("append")
        .save()
    )


fact_v_t_erc_monthly_detail_df = get_data_from_starrocks("fact_v_t_erc_monthly_detail")

fact_v_t_erc_monthly_detail_df.createOrReplaceTempView("tv_fact_v_t_erc_monthly_detail")

transformed_df = spark.sql("""
with adjusted_erc as (
	SELECT DISTINCT
		C_ORIGIN_ACCOUNT_CODE,
		CASE
			WHEN C_MONTH = 12 THEN C_YEAR + 1
			ELSE C_YEAR
		END AS C_YEAR_ADJ,
		CASE
			WHEN C_MONTH = 12 THEN 1
			ELSE C_MONTH + 1
		END AS C_MONTH_ADJ,
		C_SYNTHETIC_ANNOUCED_RANKING
	FROM
		tv_fact_v_t_erc_monthly_detail
)
SELECT
	*,
	CONCAT(C_ORIGIN_ACCOUNT_CODE, '_', C_YEAR_ADJ, '_', C_MONTH_ADJ) AS C_RANKING_KEY
FROM adjusted_erc     
""")

load_data_to_tbl_in_starrocks(transformed_df, "dim_erc_ranking")

spark.stop()



