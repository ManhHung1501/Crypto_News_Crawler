from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from crawler import coindesk,cointelegraph,cryptoslate, bitcoinist, newsbitcoin, theblockcrypto
from crawler_constants.crawl_constants import Coindesk, Cointelegraph, NewsBitcoin


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
    crawl_cryptoslate_task = PythonOperator(task_id='crawl_cryptoslate',
                                python_callable=cryptoslate.full_crawl_articles,
                                provide_context=True
                                )
    crawl_bitcoinist_task = PythonOperator(task_id='crawl_bitcoinist',
                                python_callable=bitcoinist.full_crawl_articles,
                                provide_context=True
                                )
    crawl_theblockcrypto_task = PythonOperator(task_id='crawl_theblockcrypto',
                                python_callable=theblockcrypto.full_crawl_articles,
                                provide_context=True
                                )
    with TaskGroup(group_id=f"Group_Crawler_NewsBitcoin") as task_group_API_android:
        previous_task = None
        for category in NewsBitcoin.categories:
            crawl_newsbitcoin_task = PythonOperator(task_id=f'crawl_newsbitcoin_{category}',
                                        python_callable=newsbitcoin.full_crawl_articles,
                                        provide_context=True,
                                        op_kwargs={
                                                    'category': category
                                                }
                                        )
            if previous_task:
                previous_task >> crawl_newsbitcoin_task
            previous_task = crawl_newsbitcoin_task
    with TaskGroup(group_id=f"Group_Crawler_Coindesk") as task_group_API_android:
        previous_task = None
        for topic in Coindesk.topics:
            crawl_coindesk_task = PythonOperator(task_id=f'crawl_coindesk_{topic}',
                                        python_callable=coindesk.full_crawl_articles,
                                        provide_context=True,
                                        op_kwargs={
                                                    'topic': topic
                                                }
                                        )
            if previous_task:
                previous_task >> crawl_coindesk_task
            previous_task = crawl_coindesk_task

    with TaskGroup(group_id=f"Group_Crawler_Cointelegraph") as task_group_API_android:
        previous_task = None
        for tag in Cointelegraph.tags:
            crawl_cointelegraph_task = PythonOperator(task_id=f'crawl_cointelegraph_{tag}',
                                        python_callable=cointelegraph.full_crawl_articles,
                                        provide_context=True,
                                        op_kwargs={
                                                    'tag': tag
                                                }
                                        )
            if previous_task:
                previous_task >> crawl_cointelegraph_task
            previous_task = crawl_cointelegraph_task
   
