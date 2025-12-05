from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, date_format, to_date, lit, col
from pyspark.sql.types import TimestampType, DecimalType

from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = (
    SparkSession.builder
        .appName("truncate_starrocks")
        .getOrCreate()
)

def truncate_starrocks_table(db: str, table: str):
    url = "jdbc:mysql://kube-starrocks-fe-service.warehouse.svc.cluster.local:9030"
    user = "mbs_demo"
    password = "mbs_demo"

    truncate_sql = f"TRUNCATE TABLE {db}.{table}"

    spark._sc._gateway.jvm.java.sql.DriverManager.getConnection(
        url, user, password
    ).createStatement().execute(truncate_sql)

truncate_starrocks_table("mbs_gold", "front_data")

spark.stop()
