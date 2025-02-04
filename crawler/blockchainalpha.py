import time, random, requests
from bs4 import BeautifulSoup
from requests.exceptions import Timeout
from datetime import datetime
from selenium.webdriver.common.by import By
from crawler_utils.minio_utils import upload_json_to_minio, connect_minio
from crawler_utils.common_utils import generate_url_hash,get_last_initial_crawled, get_last_crawled
from crawler_utils.chrome_driver_utils import setup_driver, wait_for_page_load
from crawler_config.storage_config import CRYPTO_NEWS_BUCKET



def get_detail_article(articles):
    for article in articles:
        content = "No content"
        published_at = "1970-01-01 00:00:00"
        url = article['url']
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
                    time.sleep(10)
                
            soup = BeautifulSoup(response.content, "html.parser")
            date_tag = soup.find("time")
            if date_tag:
                published_at = datetime.strptime(date_tag['datetime'], "%Y-%m-%dT%H:%M:%S.%fZ").strftime("%Y-%m-%d %H:%M:%S")
                
            article_card = soup.find("div", id="post-content-parent")
            if article_card:
                unwanted_tags = ".permalink-heading, .embed-wrapper"
                for unwanted_tag in article_card.select(unwanted_tags):
                    unwanted_tag.decompose()
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
 
    prefix = f'web_crawler/blockchainalpha/blockchainalpha_initial_batch_'
    last_crawled_id, current_batch = get_last_initial_crawled(minio_client=minio_client, bucket=CRYPTO_NEWS_BUCKET,prefix=prefix)
    URL = f"https://blockchainalpha.hashnode.dev/"
    print(f"Crawling URL: {URL}")

    # Open the URL
    driver.get(URL)
    # Wait for the articles to load initially
    wait_for_page_load(driver, 'div.blog-posts-wrapper')
    
    not_crawled = last_crawled_id is None
    articles_data = []
    batch_size = 100
    crawled_id = set()
    previous_news = 0
    count = 0
    while True:
        # Get all the articles on the current page
        container = driver.find_element(By.CSS_SELECTOR, "div.blog-posts-wrapper")
        
        # Find all the articles within the container
        data_div = container.find_elements(By.CSS_SELECTOR, "div.blog-post-card")
        current_news = len(data_div)
        if current_news == previous_news:
            if count == 3:
                break
            count += 1
            time.sleep(3)
        else:
            count = 0
        articles = data_div[previous_news:current_news]
        print(f"Crawling news from {previous_news} to {current_news} news")
        for article in articles:
            try:
                # Extract title
                title_element = article.find_element(By.CSS_SELECTOR, "h2.blog-post-card-title a")
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
                        "source": "blockchainalpha.tech"
                    })
                if len(articles_data) == batch_size:
                    articles_data = get_detail_article(articles=articles_data)
                    new_batch = current_batch + batch_size
                    object_key = f'{prefix}{new_batch}.json'
                    upload_json_to_minio(json_data=articles_data,object_key=object_key)
                    current_batch = new_batch
                    articles_data = []
                    crawled_id = set()
            except Exception as e:
                print(f"Error extracting data for an article: {e}")
            
        
        # Click the "More stories" button to load more articles
        try:
            next_button = driver.find_element(By.XPATH, "//button[.//span[text()='Load more']]")
            driver.execute_script("arguments[0].click();", next_button)  
            previous_news = current_news
        except Exception as e:
            try:
                driver.execute_script("arguments[0].scrollIntoView();", articles[-1])
                print(f"Process from {previous_news} to {current_news}")
                previous_news = current_news
                retries_count = 0
            except IndexError:
                print(f"Get Error in load more news retries {retries_count}/{3}")
                retries_count+=1
                if retries_count > 3:
                    break
          
        time.sleep(random.uniform(2, 4))

    if articles_data:
        articles_data = get_detail_article(articles=articles_data)
        object_key = f'{prefix}{current_batch + len(articles_data)}.json'
        upload_json_to_minio(json_data=articles_data,object_key=object_key)

def incremental_crawl_articles(max_news:int =500):
    driver = setup_driver()
    
    minio_client = connect_minio()
 
    prefix = f'web_crawler/blockchainalpha/blockchainalpha_initial_batch_'
    STATE_FILE = f'web_crawler/blockchainalpha/blockchainalpha_incremental_crawled_at_'
    last_crawled = get_last_crawled(STATE_FILE=STATE_FILE, minio_client=minio_client, bucket=CRYPTO_NEWS_BUCKET, prefix=prefix)
    URL = f"https://blockchainalpha.hashnode.dev/"
    print(f"Crawling URL: {URL}")

    # Open the URL
    driver.get(URL)
    # Wait for the articles to load initially
    wait_for_page_load(driver, 'div.blog-posts-wrapper')
    
    articles_data = []
    crawled_id = set()
    previous_news = 0
    count = 0
    complete = False
    while not complete:
        # Get all the articles on the current page
        container = driver.find_element(By.CSS_SELECTOR, "div.blog-posts-wrapper")
        
        # Find all the articles within the container
        data_div = container.find_elements(By.CSS_SELECTOR, "div.blog-post-card")
        current_news = len(data_div)
        if current_news == previous_news:
            if count == 3:
                break
            count += 1
            time.sleep(3)
        else:
            count = 0
        articles = data_div[previous_news:current_news]
        print(f"Crawling news from {previous_news} to {current_news} news")
        for article in articles:
            try:
                # Extract title
                title_element = article.find_element(By.CSS_SELECTOR, "h2.blog-post-card-title a")
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
                    break
                
                articles_data.append({
                    "id": article_id,
                    "title": title_element.text.strip(),
                    "url": article_url,
                    "source": "blockchainalpha.tech"
                })
                crawled_id.add(article_id)
              
            except Exception as e:
                print(f"Error extracting data for an article: {e}")
            
        
        # Click the "More stories" button to load more articles
        try:
            next_button = driver.find_element(By.XPATH, "//button[.//span[text()='Load more']]")
            driver.execute_script("arguments[0].click();", next_button)  
            previous_news = current_news
        except Exception as e:
            try:
                driver.execute_script("arguments[0].scrollIntoView();", articles[-1])
                print(f"Process from {previous_news} to {current_news}")
                previous_news = current_news
                retries_count = 0
            except IndexError:
                print(f"Get Error in load more news retries {retries_count}/{3}")
                retries_count+=1
                if retries_count > 3:
                    break
          
        time.sleep(random.uniform(2, 4))

    print("Crawling completed.")
