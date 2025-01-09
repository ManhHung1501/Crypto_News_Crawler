import time, random
from bs4 import BeautifulSoup
from datetime import datetime, timezone
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from crawler_utils.minio_utils import upload_json_to_minio, connect_minio
from crawler_utils.common_utils import generate_url_hash,get_last_initial_crawled, get_last_crawled
from crawler_utils.chrome_driver_utils import setup_driver, wait_for_page_load
from crawler_config.storage_config import CRYPTO_NEWS_BUCKET



# Get publish timestamp
def get_detail_article( articles):
    driver =setup_driver()
    for article in articles:
        url = article['url']
        content = "No content"
        published_at = "1970-01-01 00:00:00" 
        for i in range(3):
            try:
                driver.get(url)
                try:
                    meta_tag = driver.find_element(By.CSS_SELECTOR, "meta[property='article:published_time']")
                    dt = datetime.strptime(meta_tag.get_attribute("content"), "%Y-%m-%dT%H:%M:%S%z")
                    published_at = dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
                except Exception as e:
                    print(f"can't get time_element: {e}")
                # Parse the HTML with BeautifulSoup
                article_content_div = BeautifulSoup(driver.find_element(By.CSS_SELECTOR, "div.post-content").get_attribute("innerHTML"), 'html.parser')

                if article_content_div:
                    unwanteds_card = ".advertised"
                    for unwanted in article_content_div.select(unwanteds_card):
                        unwanted.decompose()
                    content = ' '.join(article_content_div.stripped_strings)
                break
            except Exception as e:
                print(f"Error for URL {url} on {i+1}/3")
                time.sleep(15)

        if content == "No content":
                print(f'Failed to get content for {url}')
            
        if published_at =="1970-01-01 00:00:00" :
            print(f'Failed to get publish date for {url}')
        article['published_at']  = published_at
        article['content']  = content
    driver.quit() 
    return articles


def full_crawl_articles():
    driver = setup_driver()
    
    minio_client = connect_minio()
 
    prefix = f'web_crawler/unchainedcrypto/unchainedcrypto_initial_batch_'
    last_crawled_id, current_batch = get_last_initial_crawled(minio_client=minio_client, bucket=CRYPTO_NEWS_BUCKET,prefix=prefix)
    URL = f"https://unchainedcrypto.com/news/"
    print(f"Crawling URL: {URL}")

    # Open the URL
    driver.get(URL)

    # Wait for the articles to load initially
    wait_for_page_load(driver, 'div.alm-paging-content')

    not_crawled = last_crawled_id is None
    articles_data = []
    crawled_id = set()
    batch_size = 100
    previous_news = 0 
    count = 0
    while True:
        # Get all the articles on the current page
        container = driver.find_element(By.CSS_SELECTOR, "div.alm-paging-content")

        # Find all the articles within the container
        data_div = container.find_elements(By.CSS_SELECTOR, "div.group-posts")
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
                link_element = article.find_element(By.CSS_SELECTOR, "div.title a")
                article_url = link_element.get_attribute("href")
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
                        "title": link_element.text.strip(),
                        "url": article_url,
                        "source": "unchainedcrypto.com"
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
            load_more_button = driver.find_element(By.CLASS_NAME, "alm-load-more-btn")
            actions = ActionChains(driver)
            actions.move_to_element(load_more_button).perform()

            # Wait for the button to become clickable
            WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.CLASS_NAME, "alm-load-more-btn"))
            )

            # Click the button
            driver.execute_script("arguments[0].click();", load_more_button)

            previous_news = current_news        
        
        except Exception as e:
            print("Error in load more: ", e)
            break
                
        # Wait for new articles to load
        time.sleep(random.uniform(2, 3))

    if articles_data:
        articles_data = get_detail_article(articles=articles_data)
        object_key = f'{prefix}{current_batch + len(articles_data)}.json'
        upload_json_to_minio(json_data=articles_data,object_key=object_key)

    driver.quit()
    
def incremental_crawl_articles():
    driver = setup_driver()
    
    minio_client = connect_minio()
 
    prefix = f'web_crawler/unchainedcrypto/unchainedcrypto_initial_batch_'
    STATE_FILE = f'web_crawler/unchainedcrypto/unchainedcrypto_incremental_crawled_at_'
    last_crawled = get_last_crawled(STATE_FILE=STATE_FILE, minio_client=minio_client, bucket=CRYPTO_NEWS_BUCKET, prefix=prefix)
    URL = f"https://unchainedcrypto.com/news/"
    print(f"Crawling URL: {URL}")

    # Open the URL
    driver.get(URL)

    # Wait for the articles to load initially
    wait_for_page_load(driver, 'div.alm-paging-content')

    articles_data = []
    crawled_id = set()
    previous_news = 0 
    count = 0
    complete = False
    while not complete:
        # Get all the articles on the current page
        container = driver.find_element(By.CSS_SELECTOR, "div.alm-paging-content")

        # Find all the articles within the container
        data_div = container.find_elements(By.CSS_SELECTOR, "div.group-posts")
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
                link_element = article.find_element(By.CSS_SELECTOR, "div.title a")
                article_url = link_element.get_attribute("href")
                article_id = generate_url_hash(article_url)
                # Skip if the article URL has already been processed
                if article_id in crawled_id:
                    continue

                if article_id in last_crawled:
                    articles_data = get_detail_article(articles=articles_data)
                    object_key = f'web_crawler/unchainedcrypto/unchainedcrypto_incremental_crawled_at_{int(datetime.now().timestamp())}.json'
                    upload_json_to_minio(json_data=articles_data, object_key=object_key)
                    complete = True
                    break
                # Add the article data to the list
                articles_data.append({
                    "id": article_id,
                    "title": link_element.text.strip(),
                    "url": article_url,
                    "source": "unchainedcrypto.com"
                })
                crawled_id.add(article_id)
                
            except Exception as e:
                print(f"Error extracting data for an article: {e}")
               
        # Click the "More stories" button to load more articles
        try:
            load_more_button = driver.find_element(By.CLASS_NAME, "alm-load-more-btn")
            actions = ActionChains(driver)
            actions.move_to_element(load_more_button).perform()

            # Wait for the button to become clickable
            WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.CLASS_NAME, "alm-load-more-btn"))
            )

            # Click the button
            driver.execute_script("arguments[0].click();", load_more_button)

            previous_news = current_news        
        
        except Exception as e:
            print("Error in load more: ", e)
            break
                
        # Wait for new articles to load
        time.sleep(random.uniform(2, 3))

    driver.quit()
    print("Crawling completed.")