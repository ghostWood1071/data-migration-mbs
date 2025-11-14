from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

default_args = {"owner": "airflow", "retries": 0}

with DAG(
    dag_id="cdc_source2silver_t_back_account",
    start_date=datetime(2025, 10, 19),
    schedule_interval=None,
    catchup=False,
    tags=["spark", "starrocks", "kafka", "debezium"]
) as dag:

    create_delta_job = SparkSubmitOperator(
        task_id="cdc_source2silver_t_back_account",
        application="s3a://asset/spark-jobs/t_back_account_job.py",
        deploy_mode="cluster",
        name="cdc_source2silver_t_back_account",
        conn_id="spark_k8s",
        conf={
            "spark.databricks.delta.schema.autoMerge.enabled": "true",
            "spark.kubernetes.namespace": "compute",
            "spark.kubernetes.container.image": "ghostwood/mbs-spark:1.0.4-streaming",
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
