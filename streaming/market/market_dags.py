from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

default_args = {"owner": "airflow", "retries": 0}

with DAG(
    dag_id="market_raw",
    start_date=datetime(2025, 10, 19),
    schedule_interval=None,
    catchup=False,
    is_paused_upon_creation=True,
    tags=["spark", "kafka", "debezium", "protobuf"]
) as dag:

    create_delta_job = SparkSubmitOperator(
        task_id="market_raw_task",
        application="s3a://asset/spark-jobs/market.py",
        deploy_mode="cluster",
        name="market_raw_task",
        conn_id="spark_k8s",
        conf={
            "spark.databricks.delta.schema.autoMerge.enabled": "true",
            "spark.kubernetes.namespace": "compute",
            "spark.kubernetes.container.image": "ghostwood/mbs-spark:1.0.7-protobuf",
            "spark.executor.instances": 3,
            "spark.executor.cores": 2,
            "spark.sql.shuffle.partitions": 6,
            "spark.streaming.kafka.maxRatePerPartition": 2000,
            "spark.kubernetes.authenticate.driver.serviceAccountName": "spark",
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            "spark.sql.catalogImplementation": "hive",
            "spark.hadoop.hive.metastore.uris": "thrift://hive-metastore.metastore.svc.cluster.local:9083",
            "spark.sql.warehouse.dir": "s3a://warehouse/delta/",
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

    create_delta_job
