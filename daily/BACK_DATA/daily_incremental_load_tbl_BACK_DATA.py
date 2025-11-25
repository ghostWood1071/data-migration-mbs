from pyspark.sql import SparkSession
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

spark = (
    SparkSession.builder
                .appName("daily_load_back_data_tbl_BACK_DATA")
                .getOrCreate()
)


def get_data_from_starrocks(table_name: str,  partition_col: str = None, date: int = None):
    if date is None:
        tz = ZoneInfo("Asia/Ho_Chi_Minh")
        date = datetime.now(tz).date() - timedelta(days=1)
  
    df = (
        spark.read.format("starrocks")
            .option("starrocks.fe.http.url", "http://kube-starrocks-fe-service.warehouse.svc.cluster.local:8030")
            .option("starrocks.fe.jdbc.url", "jdbc:mysql://kube-starrocks-fe-service.warehouse.svc.cluster.local:9030")
            .option("starrocks.table.identifier", f"mbs_realtime_db.{table_name}")
            .option("starrocks.filter.query", f"{partition_col} = '{date}'")
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
        .option("starrocks.user", "mbs_demo")
        .option("starrocks.password", "mbs_demo")
        .mode("append")
        .save()
    )


fact_t_margin_extra_balance_his_df = get_data_from_starrocks("fact_t_margin_extra_balance_his", "partition_date")
fact_t_tlo_debit_balance_history_df = get_data_from_starrocks("fact_t_tlo_debit_balance_history", "partition_date")
fact_t_back_advance_withdraw_df = get_data_from_starrocks("fact_t_back_advance_withdraw", "C_WITHDRAW_DATE")
fact_t_back_deal_history_df = get_data_from_starrocks("fact_t_back_deal_history", "partition_date")

fact_t_margin_extra_balance_his_df.createOrReplaceTempView("tv_fact_t_margin_extra_balance_his")
fact_t_tlo_debit_balance_history_df.createOrReplaceTempView("tv_fact_t_tlo_debit_balance_history")
fact_t_back_advance_withdraw_df.createOrReplaceTempView("tv_fact_t_back_advance_withdraw")
fact_t_back_deal_history_df.createOrReplaceTempView("tv_fact_t_back_deal_history")


transformed_df = spark.sql("""
	WITH 
	margin_balance_hist AS (
	SELECT
			C_ACCOUNT_CODE,
			C_TRADING_DATE AS C_DATE,
			MONTH(C_TRADING_DATE) AS C_MONTH,
		YEAR(C_TRADING_DATE) AS C_YEAR,
		SUM(C_INTERM_LOAN - C_INTERM_LOAN_OUT + C_OUTTERM_LOAN - C_OUTTERM_LOAN_OUT) AS C_MARGIN_DEBT
	FROM
		tv_fact_t_margin_extra_balance_his 
	GROUP BY
		C_ACCOUNT_CODE,
		C_TRADING_DATE
	),
	tlo_balance_hist AS (
	SELECT 
			C_ACCOUNT_CODE,
			C_TRANSACTION_DATE AS C_DATE,
			MONTH(C_TRANSACTION_DATE) AS C_MONTH,
		YEAR(C_TRANSACTION_DATE) AS C_YEAR,
			SUM(C_INTERM_BALANCE - C_INTERM_CASH_OUT + C_OUTTERM_BALANCE - C_OUTTERM_CASH_OUT + C_MBS_EXPIRE_DEBIT - C_TLO_COL_1) AS C_MBLINK_DEBT
	FROM
		tv_fact_t_tlo_debit_balance_history
	GROUP BY
		C_ACCOUNT_CODE,
		C_TRANSACTION_DATE
	),
	back_adv_withdraw AS (
	SELECT 
			C_ACCOUNT_CODE,
			C_WITHDRAW_DATE AS C_DATE,
			MONTH(C_WITHDRAW_DATE) AS C_MONTH,
		YEAR(C_WITHDRAW_DATE) AS C_YEAR,
			SUM(C_CASH_VOLUME) AS C_UTTB
	FROM
		tv_fact_t_back_advance_withdraw
	GROUP BY
		C_ACCOUNT_CODE,
		C_WITHDRAW_DATE
	),
	back_deal_history AS (
	SELECT
			C_ACCOUNT_CODE,
			C_TRANSACTION_DATE AS C_DATE,
			MONTH(C_TRANSACTION_DATE) AS C_MONTH,
		YEAR(C_TRANSACTION_DATE) AS C_YEAR,
		SUM(C_MATCHED_PRICE * C_MATCHED_VOLUME) AS C_GTGD
	FROM
		tv_fact_t_back_deal_history
	GROUP BY
		C_ACCOUNT_CODE,
		C_TRANSACTION_DATE
	),
	OUTER_TABLE as (
	SELECT
		COALESCE(m.C_ACCOUNT_CODE, t.C_ACCOUNT_CODE, b.C_ACCOUNT_CODE, d.C_ACCOUNT_CODE) AS C_ACCOUNT_CODE,
		COALESCE(m.C_DATE, t.C_DATE, b.C_DATE, d.C_DATE) AS C_DATE,
		COALESCE(m.C_MONTH, t.C_MONTH, b.C_MONTH, d.C_MONTH) AS C_MONTH,
		COALESCE(m.C_YEAR, t.C_YEAR, b.C_YEAR, d.C_YEAR) AS C_YEAR,
		COALESCE(m.C_MARGIN_DEBT, 0) AS C_MARGIN_DEBT,
		COALESCE(t.C_MBLINK_DEBT, 0) AS C_MBLINK_DEBT,
		COALESCE(b.C_UTTB, 0) AS C_UTTB,
		COALESCE(d.C_GTGD, 0) AS C_GTGD,
		CONCAT(
			LEFT(COALESCE(m.C_ACCOUNT_CODE, t.C_ACCOUNT_CODE, b.C_ACCOUNT_CODE, d.C_ACCOUNT_CODE), 6),
			'_',
			COALESCE(m.C_YEAR, t.C_YEAR, b.C_YEAR, d.C_YEAR),
			'_',
			COALESCE(m.C_MONTH, t.C_MONTH, b.C_MONTH, d.C_MONTH)
		) AS C_RANKING_KEY
	FROM
		margin_balance_hist m
	FULL OUTER JOIN tlo_balance_hist t 
		ON 
			m.C_ACCOUNT_CODE = t.C_ACCOUNT_CODE 
			AND m.C_DATE = t.C_DATE
	FULL OUTER JOIN back_adv_withdraw b 
		ON 
			COALESCE(m.C_ACCOUNT_CODE, t.C_ACCOUNT_CODE) = b.C_ACCOUNT_CODE 
			AND COALESCE(m.C_DATE, t.C_DATE) = b.C_DATE
	FULL OUTER JOIN back_deal_history d 
		ON 
			COALESCE(m.C_ACCOUNT_CODE, t.C_ACCOUNT_CODE, b.C_ACCOUNT_CODE) = d.C_ACCOUNT_CODE
			AND COALESCE(m.C_DATE, t.C_DATE, b.C_DATE) = d.C_DATE
	)
	SELECT
 		LEFT(C_ACCOUNT_CODE, 6) AS C_ORIGIN_ACCOUNT_CODE,
		ot.*,
		CASE
			WHEN C_MONTH BETWEEN 1 AND 6 THEN CONCAT(C_YEAR, ' Ky 1')
			ELSE CONCAT(C_YEAR, ' Ky 2')
		END AS C_KY_KPI
	FROM
		OUTER_TABLE ot
""")
print("---------------LENGTH------------------")
print(transformed_df.count())

load_data_to_tbl_in_starrocks(transformed_df, "back_data")

spark.stop()