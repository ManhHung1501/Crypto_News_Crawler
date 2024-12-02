from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from crawler import coindesk,cointelegraph,cryptoslate



# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    "start_date": datetime(2024, 10, 1),
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


# Define DAG
with DAG(
    dag_id='Full_Crawl_Crypto_News_DAG',
    default_args=default_args,
    tags=["Crawl", "Crypto", "News", "Web"],
    schedule_interval=None,
    catchup=False,
) as dag:
    
    crawl_coindesk_task = PythonOperator(task_id='crawl_coindesk',
                                python_callable=coindesk.full_crawl_articles,
                                provide_context=True
                                )
    crawl_cointelegraph_task = PythonOperator(task_id='crawl_cointelegraph',
                                python_callable=cointelegraph.full_crawl_articles,
                                provide_context=True
                                )
    crawl_cryptoslate_task = PythonOperator(task_id='crawl_cryptoslate',
                                python_callable=cryptoslate.full_crawl_articles,
                                provide_context=True
                                )
   
