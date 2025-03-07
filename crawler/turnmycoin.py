import time, random, requests, re
from datetime import datetime, timezone
from bs4 import BeautifulSoup
from requests.exceptions import Timeout
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from crawler_utils.minio_utils import upload_json_to_minio, connect_minio
from crawler_utils.common_utils import generate_url_hash,get_last_initial_crawled, get_last_crawled
from crawler_utils.chrome_driver_utils import setup_driver, wait_for_page_load
from crawler_config.storage_config import CRYPTO_NEWS_BUCKET
from crawler_utils.mongo_utils import load_json_from_minio_to_mongodb

def get_detail_article(articles):
    for article in articles:
        content = "No content"
        url = article['url']
        published_at = "1970-01-01 00:00:00"
        content= "No content"
        try:
            for _ in range(3):
                # Make the HTTP request
                try:
                    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0"}
                    response = requests.get(url, timeout=15, headers=headers)
                    response.raise_for_status() 
                    break
                except Timeout:
                    print(f'Retrying ...')
                    print(f"timed out. Sleep for {url}...")
                except requests.exceptions.RequestException as e:
                    print(f'Retrying ...')
                    print(f"Request for {url} failed: {e}")
                    time.sleep(5)
                
            soup = BeautifulSoup(response.content, "html.parser")
            
            meta_tag = soup.find('meta', {'property': 'og:updated_time'})
            if meta_tag:
                dt = datetime.strptime(meta_tag['content'], "%Y-%m-%dT%H:%M:%S%z")
                # Format as 'yyyy-mm-dd hh:mm:ss' in UTC
                published_at = dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
                
            article_card = soup.find("div", class_="elementor-widget-theme-post-content")
            if article_card:
                unwanted_cards = ".wp-block-essential-blocks-table-of-contents, .last-updated"
                for unwanted in article_card.select(unwanted_cards):
                    unwanted.decompose()
                content = ' '.join(article_card.stripped_strings)
            
        except Exception as e: 
            print(f'Error in get content for {url}: ', e)
        if content == "No content":
            print(f"Failed to get content of url: {url}")
        if published_at == "1970-01-01 00:00:00":
            print(f'Failed to get Publish date for {url}')


        article['published_at'] = published_at   
        article['content'] = content
    return articles


def full_crawl_articles():
    driver = setup_driver()
    
    minio_client = connect_minio()
 
    prefix = f'web_crawler/turnmycoin/turnmycoin_initial_batch_'
    last_crawled_id, current_batch = get_last_initial_crawled(minio_client=minio_client, bucket=CRYPTO_NEWS_BUCKET,prefix=prefix)
    URL = f"https://turnmycoin.com/blog/"
    print(f"Crawling URL: {URL}")

    # Open the URL
    driver.get(URL)

    # Wait for the articles to load initially
    wait_for_page_load(driver, 'div.eael-filter-gallery-container')

    not_crawled = last_crawled_id is None
    articles_data = []
    crawled_id = set()
    batch_size = 100
    previous_news = 0 
    count = 0
    while True:
        # Get all the articles on the current page
        container = driver.find_element(By.CSS_SELECTOR, "div.eael-filter-gallery-container")

        # Find all the articles within the container
        data_div = container.find_elements(By.CSS_SELECTOR, "div.item-content")
        current_news = len(data_div)
        if current_news == previous_news:
            if count == 3:
                break
            count += 1
            time.sleep(3)
        else:
            count = 0
        articles = data_div[previous_news: current_news]
        print(f"Crawling news from {previous_news} to {current_news} news")
        
        for article in articles:
            try:
                # Extract title
                title_element = article.find_element(By.CSS_SELECTOR, "h2.title a")
                article_url = title_element.get_attribute("href")
                article_id = generate_url_hash(article_url)
                # Skip if the article URL has already been processed
                if article_id in crawled_id:
                    continue

                if not not_crawled and article_id == last_crawled_id:
                    not_crawled = True
                    continue
                if not_crawled:
                    # Add the article data to the list
                    articles_data.append({
                        "id": article_id,
                        "title": title_element.text.strip(),
                        "url": article_url,
                        "source": "turnmycoin.com"
                    })
                    crawled_id.add(article_id)
                if len(articles_data) == batch_size:
                    articles_data = get_detail_article(articles=articles_data)
                    new_batch = current_batch + batch_size
                    object_key = f'{prefix}{new_batch}.json'
                    upload_json_to_minio(json_data=articles_data,object_key=object_key)
                    current_batch = new_batch
                    articles_data = []
                    crawled_id=set()
            except Exception as e:
                print(f"Error extracting data for an article: {e}")
    
        # Click the "More stories" button to load more articles
        try: 
            load_more_button = driver.find_element(By.CLASS_NAME, "eael-load-more-button")
            actions = ActionChains(driver)
            actions.move_to_element(load_more_button).click().perform()

            previous_news =current_news
        except Exception as e:
            print("Error in click: ", e)
            break
                
        # Wait for new articles to load
        time.sleep(random.uniform(2, 3))
    
    if articles_data:
        articles_data = get_detail_article(articles=articles_data)
        object_key = f'{prefix}{current_batch + len(articles_data)}.json'
        upload_json_to_minio(json_data=articles_data,object_key=object_key)
    driver.quit()
    
def incremental_crawl_articles(max_news:int=500):
    driver = setup_driver()
    
    minio_client = connect_minio()
 
    prefix = f'web_crawler/turnmycoin/turnmycoin_initial_batch_'
    STATE_FILE = f'web_crawler/turnmycoin/turnmycoin_incremental_crawled_at_'
    last_crawled = get_last_crawled(STATE_FILE=STATE_FILE, minio_client=minio_client, bucket=CRYPTO_NEWS_BUCKET, prefix=prefix)
    URL = f"https://turnmycoin.com/blog/"
    print(f"Crawling URL: {URL}")

    # Open the URL
    driver.get(URL)

    # Wait for the articles to load initially
    wait_for_page_load(driver, 'div.eael-filter-gallery-container')

    articles_data = []
    crawled_id = set()
    previous_news = 0 
    count = 0
    complete = False
    while not complete:
        # Get all the articles on the current page
        container = driver.find_element(By.CSS_SELECTOR, "div.eael-filter-gallery-container")

        # Find all the articles within the container
        data_div = container.find_elements(By.CSS_SELECTOR, "div.item-content")
        current_news = len(data_div)
        if current_news == previous_news:
            if count == 3:
                break
            count += 1
            time.sleep(3)
        else:
            count = 0
        articles = data_div[previous_news: current_news]
        print(f"Crawling news from {previous_news} to {current_news} news")
        
        for article in articles:
            try:
                # Extract title
                title_element = article.find_element(By.CSS_SELECTOR, "h2.title a")
                article_url = title_element.get_attribute("href")
                article_id = generate_url_hash(article_url)
                # Skip if the article URL has already been processed
                if article_id in crawled_id:
                    continue

                if article_id in last_crawled or len(articles_data) == max_news:
                    articles_data = get_detail_article(articles=articles_data)
                    object_key = f'{STATE_FILE}{int(datetime.now().timestamp())}.json'
                    upload_json_to_minio(json_data=articles_data, object_key=object_key)
                    complete = True
                    load_json_from_minio_to_mongodb(minio_client, object_key) 
                    break
                # Add the article data to the list
                articles_data.append({
                    "id": article_id,
                    "title": title_element.text.strip(),
                    "url": article_url,
                    "source": "turnmycoin.com"
                })
                crawled_id.add(article_id)
                
            except Exception as e:
                print(f"Error extracting data for an article: {e}")
    
        # Click the "More stories" button to load more articles
        try: 
            load_more_button = driver.find_element(By.CLASS_NAME, "eael-load-more-button")
            actions = ActionChains(driver)
            actions.move_to_element(load_more_button).click().perform()

            previous_news =current_news
        except Exception as e:
            print("Error in click: ", e)
            break
                
        # Wait for new articles to load
        time.sleep(random.uniform(2, 3))
    
    driver.quit()
    print("Crawling completed.")