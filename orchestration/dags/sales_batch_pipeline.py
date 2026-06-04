from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime, timedelta
import requests

default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
}

JAR_PATH = "/home/laila/Desktop/sales-data-plateform/spark-processing/target/spark-processing-1.0-SNAPSHOT.jar"
SPARK_MASTER = "spark://spark-master:7077"
KAFKA_CONNECT_URL = "http://kafka-connect:8083"

CONNECTORS = [
    "postgres-source-clients",
    "postgres-source-produits",
    "csv-retours",
    "csv-canaux-vente",
    "minio-sink",
]

def restart_connectors():
    for connector in CONNECTORS:
        r = requests.post(f"{KAFKA_CONNECT_URL}/connectors/{connector}/restart")
        print(f"[Kafka Connect] {connector} → {r.status_code}")

def check_connectors():
    for connector in CONNECTORS:
        r = requests.get(f"{KAFKA_CONNECT_URL}/connectors/{connector}/status")
        state = r.json()["connector"]["state"]
        print(f"[Kafka Connect] {connector} → {state}")
        if state != "RUNNING":
            raise Exception(f"Connector {connector} not RUNNING: {state}")

SPARK_CONF = (
    "--conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 "
    "--conf spark.hadoop.fs.s3a.access.key=saifi "
    "--conf spark.hadoop.fs.s3a.secret.key=PSSwOrd123 "
    "--conf spark.hadoop.fs.s3a.path.style.access=true "
    "--conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem "
    "--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension "
    "--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
)

with DAG(
    dag_id='sales_batch_pipeline',
    default_args=default_args,
    schedule='0 2 * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['sales', 'batch', 'spark'],
) as dag:

    t1 = PythonOperator(
        task_id='restart_kafka_connectors',
        python_callable=restart_connectors,
    )

    t2 = PythonOperator(
        task_id='check_kafka_connectors',
        python_callable=check_connectors,
    )

    t3 = BashOperator(
        task_id='spark_bronze_ingestion',
        bash_command=f"docker exec spark-master /opt/spark/bin/spark-submit --master {SPARK_MASTER} --class org.example.batch.bronze.BronzeIngestion {SPARK_CONF} {JAR_PATH}",
    )

    t4 = BashOperator(
        task_id='spark_silver_cleaning',
        bash_command=f"docker exec spark-master /opt/spark/bin/spark-submit --master {SPARK_MASTER} --class org.example.batch.silver.SilverCleaning {SPARK_CONF} {JAR_PATH}",
    )

    t5 = BashOperator(
        task_id='spark_gold_star_schema',
        bash_command=f"docker exec spark-master /opt/spark/bin/spark-submit --master {SPARK_MASTER} --class org.example.batch.gold.GoldStarSchema {SPARK_CONF} {JAR_PATH}",
    )

    t1 >> t2 >> t3 >> t4 >> t5
