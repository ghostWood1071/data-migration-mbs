from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

default_args = {"owner": "airflow", "retries": 0}

with DAG(
    dag_id="spark_process_silver_layer_tbl_T_MARGIN_EXTRA_BALANCE_HIS",
    start_date=datetime(2025, 10, 19),
    schedule_interval=None,
    catchup=False,
    tags=["spark", "delta", "hive"]
) as dag:

    process_silver_layer = SparkSubmitOperator(
        task_id="process_silver_layer_tbl_T_MARGIN_EXTRA_BALANCE_HIS",
        application="s3a://asset/spark-jobs/process_tbl_T_MARGIN_EXTRA_BALANCE_HIS.py",
        deploy_mode="cluster",
        name="spark-process-silver_tbl_T_MARGIN_EXTRA_BALANCE_HIS",
        conn_id="spark_k8s",
        conf={
            "spark.kubernetes.namespace": "compute",
            "spark.kubernetes.container.image": "ghostwood/mbs-spark:1.0.1-oracle",
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

    process_silver_layer