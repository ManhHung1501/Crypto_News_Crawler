from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from crawler import (coindesk,cointelegraph,cryptoslate, 
    bitcoinist, newsbitcoin, cryptonews, blockonomi, newsbtc, 
    decrypt, bankless, beincrypto, coingape, unchainedcrypto, blockworks,
    utoday, cryptoflies, nftgators, globalcryptopress, crypto_news,
    ambcrypto, turnmycoin, holder_io, coinpedia, nftevening, buildoncronos,
    droomdroom, blockchainalpha, nonfungible, theblockcrypto, dailycoin)
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
    crawl_blockworks_task = PythonOperator(task_id='crawl_blockworks',
                                python_callable=blockworks.incremental_crawl_articles,
                                provide_context=True
                                )

    crawl_dailycoin_task = PythonOperator(task_id='crawl_dailycoin',
                                python_callable=dailycoin.incremental_crawl_articles,
                                provide_context=True
                                )

    crawl_blockchainalpha_task = PythonOperator(task_id='crawl_blockchainalpha',
                                python_callable=blockchainalpha.incremental_crawl_articles,
                                provide_context=True
                                )

    crawl_holder_io_task = PythonOperator(task_id='crawl_holder_io',
                                python_callable=holder_io.incremental_crawl_articles,
                                provide_context=True
                                )

    crawl_buildoncronos_task = PythonOperator(task_id='crawl_buildoncronos',
                                python_callable=buildoncronos.incremental_crawl_articles,
                                provide_context=True
                                )

    crawl_turnmycoin_task = PythonOperator(task_id='crawl_turnmycoin',
                                python_callable=turnmycoin.incremental_crawl_articles,
                                provide_context=True
                                )

    crawl_crypto_news_task = PythonOperator(task_id='crawl_crypto_news',
                                python_callable=crypto_news.incremental_crawl_articles,
                                provide_context=True
                                )

    crawl_nftevening_task = PythonOperator(task_id='crawl_nftevening',
                                python_callable=nftevening.incremental_crawl_articles,
                                provide_context=True
                                )

    crawl_globalcryptopress_task = PythonOperator(task_id='crawl_globalcryptopress',
                                python_callable=globalcryptopress.incremental_crawl_articles,
                                provide_context=True
                                )

    crawl_unchainedcrypto_task = PythonOperator(task_id='crawl_unchainedcrypto',
                                python_callable=unchainedcrypto.incremental_crawl_articles,
                                provide_context=True
                                )

    crawl_coinpedia_task = PythonOperator(task_id='crawl_coinpedia',
                                python_callable=coinpedia.incremental_crawl_articles,
                                provide_context=True
                                )

    crawl_decrypt_task = PythonOperator(task_id='crawl_decrypt',
                                python_callable=decrypt.incremental_crawl_articles,
                                provide_context=True
                                )

    crawl_theblockcrypto_task = PythonOperator(task_id='crawl_theblockcrypto',
                                python_callable=theblockcrypto.incremental_crawl_articles,
                                provide_context=True
                                )

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
    
    with TaskGroup(group_id=f"Group_Crawler_NonFungible") as task_nonfungible:
        previous_task = None
        for category in NonFungible.categories:
            crawl_nonfungible_task = PythonOperator(task_id=f'crawl_nonfungible_{category}',
                                        python_callable=nonfungible.incremental_crawl_articles,
                                        provide_context=True,
                                        op_kwargs={
                                                    'category': category
                                                }
                                        )
            if previous_task:
                previous_task >> crawl_nonfungible_task
            previous_task = crawl_nonfungible_task

    with TaskGroup(group_id=f"Group_Crawler_Cryptoflies") as task_cryptoflies:
        previous_task = None
        for category in Cryptoflies.categories:
            crawl_cryptoflies_task = PythonOperator(task_id=f'crawl_cryptoflies_{category}',
                                        python_callable=cryptoflies.incremental_crawl_articles,
                                        provide_context=True,
                                        op_kwargs={
                                                    'category': category
                                                }
                                        )
            if previous_task:
                previous_task >> crawl_cryptoflies_task
            previous_task = crawl_cryptoflies_task

    with TaskGroup(group_id=f"Group_Crawler_DroomDroom") as task_droomdroom:
        previous_task = None
        for category in DroomDroom.categories:
            crawl_droomdroom_task = PythonOperator(task_id=f'crawl_droomdroom_{category}',
                                        python_callable=droomdroom.incremental_crawl_articles,
                                        provide_context=True,
                                        op_kwargs={
                                                    'category': category
                                                }
                                        )
            if previous_task:
                previous_task >> crawl_droomdroom_task
            previous_task = crawl_droomdroom_task

    with TaskGroup(group_id=f"Group_Crawler_Nftgators") as task_nftgators:
        previous_task = None
        for category in Nftgators.categories:
            crawl_nftgators_task = PythonOperator(task_id=f'crawl_nftgators_{category}',
                                        python_callable=nftgators.incremental_crawl_articles,
                                        provide_context=True,
                                        op_kwargs={
                                                    'category': category
                                                }
                                        )
            if previous_task:
                previous_task >> crawl_nftgators_task
            previous_task = crawl_nftgators_task

    with TaskGroup(group_id=f"Group_Crawler_NewsBitcoin") as task_newsbitcoin:
        previous_task = None
        for category in NewsBitcoin.categories:
            crawl_newsbitcoin_task = PythonOperator(task_id=f'crawl_newsbitcoin_{category}',
                                        python_callable=newsbitcoin.incremental_crawl_articles,
                                        provide_context=True,
                                        op_kwargs={
                                                    'category': category
                                                }
                                        )
            if previous_task:
                previous_task >> crawl_newsbitcoin_task
            previous_task = crawl_newsbitcoin_task

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