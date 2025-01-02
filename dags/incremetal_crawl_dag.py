from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from crawler import (coindesk,cointelegraph,cryptoslate, 
    bitcoinist, newsbitcoin, cryptonews, blockonomi, newsbtc, 
    decrypt, bankless, beincrypto, coingape, unchainedcrypto,
    utoday, cryptoflies, nftgators, globalcryptopress, crypto_news,
    ambcrypto, turnmycoin, holder_io, coinpedia, nftevening, buildoncronos,
    droomdroom, blockchainalpha, nonfungible)
from crawler_constants.crawl_constants import Coindesk, Cointelegraph, NewsBitcoin, Cryptoflies, Nftgators, DroomDroom, NonFungible


# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    "start_date": datetime(2024, 10, 1),
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}


# Define DAG
with DAG(
    dag_id='Incremental_Crawl_Crypto_News_DAG',
    default_args=default_args,
    tags=["Crawl", "Crypto", "News", "Web"],
    schedule_interval='0 0 * * *',
    catchup=False,
    max_active_tasks=3,
) as dag:
    with TaskGroup(group_id=f"Group_Crawler_Coindesk") as task_coindesk:
        previous_task = None
        for topic in Coindesk.topics:
            crawl_coindesk_task = PythonOperator(task_id=f'crawl_coindesk_{topic}',
                                        python_callable=coindesk.incremental_crawl_articles,
                                        provide_context=True,
                                        op_kwargs={
                                                    'topic': topic
                                                }
                                        )
            if previous_task:
                previous_task >> crawl_coindesk_task
            previous_task = crawl_coindesk_task