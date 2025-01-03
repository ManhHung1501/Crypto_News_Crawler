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
    crawl_utoday_task = PythonOperator(task_id='crawl_utoday',
                                python_callable=utoday.incremental_crawl_articles,
                                provide_context=True
                                )

    crawl_coingape_task = PythonOperator(task_id='crawl_coingape',
                                python_callable=coingape.incremental_crawl_articles,
                                provide_context=True
                                )
    
    crawl_ambcrypto_task = PythonOperator(task_id='crawl_ambcrypto',
                                python_callable=ambcrypto.incremental_crawl_articles,
                                provide_context=True
                                )

    crawl_beincrypto_task = PythonOperator(task_id='crawl_beincrypto',
                                python_callable=beincrypto.incremental_crawl_articles,
                                provide_context=True
                                )

    crawl_cryptonews_task = PythonOperator(task_id='crawl_cryptonews',
                                python_callable=cryptonews.incremental_crawl_articles,
                                provide_context=True
                                )

    crawl_newsbtc_task = PythonOperator(task_id='crawl_newsbtc',
                                python_callable=newsbtc.incremental_crawl_articles,
                                provide_context=True
                                )

    crawl_blockonomi_task = PythonOperator(task_id='crawl_blockonomi',
                                python_callable=blockonomi.incremental_crawl_articles,
                                provide_context=True
                                )

    crawl_bitcoinist_task = PythonOperator(task_id='crawl_bitcoinist',
                                python_callable=bitcoinist.incremental_crawl_articles,
                                provide_context=True
                                )
    
    crawl_cryptoslate_task = PythonOperator(task_id='crawl_cryptoslate',
                                python_callable=cryptoslate.incremental_crawl_articles,
                                provide_context=True
                                )

    with TaskGroup(group_id=f"Group_Crawler_Cointelegraph") as task_cointelegraph:
        previous_task = None
        for tag in Cointelegraph.tags:
            crawl_cointelegraph_task = PythonOperator(task_id=f'crawl_cointelegraph_{tag}',
                                        python_callable=cointelegraph.incremental_crawl_articles,
                                        provide_context=True,
                                        op_kwargs={
                                                    'tag': tag
                                                }
                                        )
            if previous_task:
                previous_task >> crawl_cointelegraph_task
            previous_task = crawl_cointelegraph_task

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