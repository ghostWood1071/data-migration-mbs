from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime
import pendulum

default_args = {"owner": "airflow", "retries": 0}

with DAG(
    dag_id="spark_monthly_incremental_load_view_V_T_ERC_MONTHLY_DETAIL",
    start_date=pendulum.datetime(2025, 11, 17, tz="Asia/Ho_Chi_Minh"),
    schedule_interval="0 0 1 * *",
    catchup=False,
    tags=["spark", "delta", "hive", "monthly"]
) as dag:

    daily_incremental_load = SparkSubmitOperator(
        task_id="monthly_incremental_load_view_V_T_ERC_MONTHLY_DETAIL",
        application="s3a://asset/spark-jobs/monthly_incremental_load_view_V_T_ERC_MONTHLY_DETAIL.py",
        deploy_mode="cluster",
        name="spark-monthly_incremental_load_view_V_T_ERC_MONTHLY_DETAIL",
        conn_id="spark_k8s",
        conf={
            "spark.kubernetes.namespace": "compute",
            "spark.kubernetes.container.image": "ghostwood/mbs-spark:1.0.4-streaming",
            "spark.kubernetes.authenticate.driver.serviceAccountName": "spark",
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            "spark.sql.catalogImplementation": "hive",
            "spark.hadoop.hive.metastore.uris": "thrift://hive-metastore.metastore.svc.cluster.local:9083",
            "spark.sql.warehouse.dir": "s3a://warehouse/",
            "spark.hadoop.fs.s3a.endpoint": "http://minio.storage.svc.cluster.local:9000",
            "spark.hadoop.fs.s3a.access.key": "minioadmin",
            "spark.hadoop.fs.s3a.secret.key": "minio@demo!",
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.sql.sources.partitionOverwriteMode": "dynamic",
            "conf spark.eventLog.dir": "s3a://spark-logs/events",
            "spark.driver.extraJavaOptions": "-Divy.cache.dir=/tmp -Divy.home=/tmp"
        },
        verbose=True
    )

    daily_incremental_load