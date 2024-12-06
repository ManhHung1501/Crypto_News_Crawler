from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.task_group import TaskGroup
from crawler_utils.common_utils import project_dir
from crawler_constants.crawl_constants import Coindesk,Cointelegraph

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
    dag_id='Concatenation_Crypto_News_Json',
    default_args=default_args,
    tags=["Crypto", "Concatenation", "Spark", "Json"],
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
) as dag:
    #SPARK JARS

    # SPARK CONF
    spark_conf = {
        "spark.local.dir": "/tmp/spark-temp",
        # "spark.cores.max": "16",
        # "spark.executor.instances": "4",
        # "spark.executor.cores": "4",
        # "spark.executor.memory": "2g",
        # "spark.driver.memory": "2g",
        # "spark.driver.maxResultSize": "2g",
        # "spark.sql.shuffle.partitions": "125",
    }

    concatenation_cryptoslate_task = SparkSubmitOperator(task_id='concatenation_cryptoslate_task',
                                                conn_id='spark',
                                                application=f"{project_dir}/crawler_processing/uinion_json.py",
                                                application_args=[
                                                    "web_crawler/cryptoslate/*.json",
                                                    "result/crypto_news.json"
                                                ],
                                                conf=spark_conf,
                                                verbose=False,
                                                execution_timeout=timedelta(hours=2),
                                            )
    with TaskGroup(group_id=f"Concatenation_Coindesk") as task_group_coindesk:
        previous_task = None
        for topic in Coindesk.topics:
            crawl_coindesk_task = SparkSubmitOperator(task_id=f'concatenation_coindesk_{topic}_task',
                                                conn_id='spark',
                                                application=f"{project_dir}/crawler_processing/uinion_json.py",
                                                application_args=[
                                                    f"web_crawler/coindesk/{topic}/*.json",
                                                    "result/crypto_news.json"
                                                ],
                                                conf=spark_conf,
                                                verbose=False,
                                                execution_timeout=timedelta(hours=2),
                                            )
            if previous_task:
                previous_task >> crawl_coindesk_task
            previous_task = crawl_coindesk_task

    with TaskGroup(group_id=f"Group_Crawler_Cointelegraph") as task_group_cointelegraph:
        previous_task = None
        for tag in Cointelegraph.tags:
            crawl_cointelegraph_task = SparkSubmitOperator(task_id=f'concatenation_cointelegraph_{tag}_task',
                                                conn_id='spark',
                                                application=f"{project_dir}/crawler_processing/uinion_json.py",
                                                application_args=[
                                                    f"web_crawler/cointelegraph/{tag}/*.json",
                                                    "result/crypto_news.json"
                                                ],
                                                conf=spark_conf,
                                                verbose=False,
                                                execution_timeout=timedelta(hours=2),
                                            )
            if previous_task:
                previous_task >> crawl_cointelegraph_task
            previous_task = crawl_cointelegraph_task

    concatenation_cryptoslate_task >> task_group_coindesk >> task_group_cointelegraph