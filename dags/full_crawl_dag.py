from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from crawler import (coindesk,cointelegraph,cryptoslate, 
    bitcoinist, newsbitcoin, cryptonews, blockonomi, newsbtc, 
    decrypt, bankless, beincrypto, coingape, unchainedcrypto,
    utoday, cryptoflies, nftgators, globalcryptopress, crypto_news,
    ambcrypto, turnmycoin)
from crawler_constants.crawl_constants import Coindesk, Cointelegraph, NewsBitcoin, Cryptoflies, Nftgators


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
    dag_id='Full_Crawl_Crypto_News_DAG',
    default_args=default_args,
    tags=["Crawl", "Crypto", "News", "Web"],
    schedule_interval=None,
    catchup=False,
) as dag:
    crawl_ambcrypto_task = PythonOperator(task_id='crawl_ambcrypto',
                                python_callable=ambcrypto.full_crawl_articles,
                                provide_context=True
                                )
    crawl_ambcrypto_task = PythonOperator(task_id='crawl_ambcrypto',
                                python_callable=ambcrypto.full_crawl_articles,
                                provide_context=True
                                )
    crawl_crypto_news_task = PythonOperator(task_id='crawl_crypto_news',
                                python_callable=crypto_news.full_crawl_articles,
                                provide_context=True
                                )
    crawl_globalcryptopress_task = PythonOperator(task_id='crawl_globalcryptopress',
                                python_callable=globalcryptopress.full_crawl_articles,
                                provide_context=True
                                )
    crawl_utoday_task = PythonOperator(task_id='crawl_utoday',
                                python_callable=utoday.full_crawl_articles,
                                provide_context=True
                                )
    crawl_unchainedcrypto_task = PythonOperator(task_id='crawl_unchainedcrypto',
                                python_callable=unchainedcrypto.full_crawl_articles,
                                provide_context=True
                                )
    crawl_coingape_task = PythonOperator(task_id='crawl_coingape',
                                python_callable=coingape.full_crawl_articles,
                                provide_context=True
                                )
    crawl_beincrypto_task = PythonOperator(task_id='crawl_beincrypto',
                                python_callable=beincrypto.full_crawl_articles,
                                provide_context=True
                                )
    
    crawl_bankless_task = PythonOperator(task_id='crawl_bankless',
                                python_callable=bankless.full_crawl_articles,
                                provide_context=True
                                )


    crawl_decrypt_task = PythonOperator(task_id='crawl_decrypt',
                                python_callable=decrypt.full_crawl_articles,
                                provide_context=True
                                )


    crawl_cryptoslate_task = PythonOperator(task_id='crawl_cryptoslate',
                                python_callable=cryptoslate.full_crawl_articles,
                                provide_context=True
                                )
    crawl_bitcoinist_task = PythonOperator(task_id='crawl_bitcoinist',
                                python_callable=bitcoinist.full_crawl_articles,
                                provide_context=True
                                )
    crawl_blockonomi_task = PythonOperator(task_id='crawl_blockonomi',
                                python_callable=blockonomi.full_crawl_articles,
                                provide_context=True
                                )
    crawl_newsbtc_task = PythonOperator(task_id='crawl_newsbtc',
                                python_callable=newsbtc.full_crawl_articles,
                                provide_context=True
                                )
    crawl_cryptonews_task = PythonOperator(task_id='crawl_cryptonews',
                                python_callable=cryptonews.full_crawl_articles,
                                provide_context=True
                                )
    with TaskGroup(group_id=f"Group_Crawler_Nftgators") as task_group_API_android:
        previous_task = None
        for category in Nftgators.categories:
            crawl_nftgators_task = PythonOperator(task_id=f'crawl_nftgators_{category}',
                                        python_callable=nftgators.full_crawl_articles,
                                        provide_context=True,
                                        op_kwargs={
                                                    'category': category
                                                }
                                        )
            if previous_task:
                previous_task >> crawl_nftgators_task
            previous_task = crawl_nftgators_task
    
    with TaskGroup(group_id=f"Group_Crawler_Cryptoflies") as task_group_API_android:
        previous_task = None
        for category in Cryptoflies.categories:
            crawl_cryptoflies_task = PythonOperator(task_id=f'crawl_cryptoflies_{category}',
                                        python_callable=cryptoflies.full_crawl_articles,
                                        provide_context=True,
                                        op_kwargs={
                                                    'category': category
                                                }
                                        )
            if previous_task:
                previous_task >> crawl_cryptoflies_task
            previous_task = crawl_cryptoflies_task

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
   
