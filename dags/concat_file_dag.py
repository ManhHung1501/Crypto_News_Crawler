import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.task_group import TaskGroup
from utils.common_utils import project_dir


# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    "start_date": datetime(2024, 10, 1),
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=10),
}


# Define DAG
with DAG(
    dag_id='Appsflyer_Transformation_DAG',
    default_args=default_args,
    tags=["Appsflyer", "Transformation", "Game"],
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
) as dag:
    #SPARK JARS

    # SPARK CONF
    spark_conf = {
        "spark.local.dir": "/tmp/spark-temp",
        "spark.cores.max": "4",
        "spark.executor.instances": "2",
        "spark.executor.cores": "2",
        "spark.executor.memory": "2g",
        "spark.driver.memory": "2g",
        # "spark.driver.maxResultSize": "2g",
        # "spark.sql.shuffle.partitions": "125",
    }

    concatenation_task = SparkSubmitOperator(task_id='concatenation_task',
                                                conn_id='spark',
                                                application=f"{project_dir}/crawler_processing/uinion_json.py",
                                                application_args=[
                                                    "web_crawler",
                                                    "result/crypto_news.json"
                                                ],
                                                conf=spark_conf,
                                                verbose=False,
                                                execution_timeout=timedelta(hours=2),
                                            )
