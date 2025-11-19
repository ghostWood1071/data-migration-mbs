from pyspark.sql import SparkSession

spark = SparkSession.builder \
                    .appName("remove_test_tbl") \
                    .enableHiveSupport() \
                    .getOrCreate()
                    
spark.sql("DROP TABLE IF EXISTS silver.test_inc_fact_t_back_advance_withdraw")
spark.sql("DROP TABLE IF EXISTS silver.test_inc_fact_t_back_deal_history")
spark.sql("DROP TABLE IF EXISTS silver.test_inc_fact_t_margin_extra_balance_his")
spark.sql("DROP TABLE IF EXISTS silver.test_inc_fact_t_tlo_debit_balance_history")
