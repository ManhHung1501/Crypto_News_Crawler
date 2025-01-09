from datetime import datetime
import time, random, requests, json
from bs4 import BeautifulSoup
from requests.exceptions import Timeout
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException
from crawler_utils.minio_utils import upload_json_to_minio, connect_minio
from crawler_utils.common_utils import generate_url_hash,get_last_crawled
from crawler_utils.chrome_driver_utils import setup_driver, wait_for_page_load
from crawler_config.storage_config import CRYPTO_NEWS_BUCKET


def get_blockworks_cookies():
    # Initialize the WebDriver
    driver = setup_driver()
    
    try:
        # Open the Blockworks news page
        driver.get("https://blockworks.co/news")
        
        # Wait for the page to load (you can adjust the time if needed)
        driver.implicitly_wait(10)
        
        # Retrieve cookies from the browser
        cookies = driver.get_cookies()
        print(cookies)
        cookie_dict = {cookie['name']: cookie['value'] for cookie in cookies}
        return cookie_dict
    finally:
        driver.quit()

def full_crawl_articles():
    url = "https://eh58zikx8p-dsn.algolia.net/1/indexes/wp_posts_post/query"
    params = {
        'x-algolia-agent': 'Algolia for JavaScript (4.24.0); Browser (lite)',
        'x-algolia-api-key': 'MzQxODQ0M2Y0MTlmMjc3MzRhYTJjMzkyOTEzY2MwNzhlODA4M2E4NjkxZDM5YTk4OGMzNzU4MDRmZjJkYTUyM3ZhbGlkVW50aWw9MTczNjM2Nzk2Mg==',
        'x-algolia-application-id': 'EH58ZIKX8P'
    }
    payload = json.dumps({
    "attributesToRetrieve": [
        "post_title",
        "post_date",
        "permalink",
        "content"
    ],
    "hitsPerPage": 1000,
    "page": 0
    })
    headers = {
    'Content-Type': 'application/json',
    'Origin': 'https://blockworks.co'
    }

    response = requests.request("POST", url,params=params, headers=headers, data=payload)
    if response.status_code == 200:
        articles =response.json()['hits']
        articles_data = []
        for article in articles:
            articles_data.append({
                "id": generate_url_hash(article['permalink']),
                "title": article['post_title'],
                "url": article['permalink'],
                "published_at": datetime.fromtimestamp(article['post_date']).strftime('%Y-%m-%d %H:%M:%S'),
                "content": article['content'],
                "source": "blockworks.co"
            })
        upload_json_to_minio(json_data=articles_data,object_key=f'web_crawler/blockworks/blockworks_initial_batch_1000.json')

def get_detail_article( articles):
    driver = setup_driver()
    for article in articles:
        url = article['url']
        content = "No content"
        
        try:
            driver.get(url)
            wait_for_page_load(driver, "section.w-full")

            # Parse the HTML with BeautifulSoup
            content_element = driver.find_element(By.CSS_SELECTOR, "section.w-full")
            article_content_div = BeautifulSoup(content_element.get_attribute("innerHTML"), 'html.parser')
            
            # content                
            if article_content_div:
                unwanteds_card = ".not-prose, .article-ad, figure"
                for unwanted in article_content_div.select(unwanteds_card):
                    unwanted.decompose()
                content = ' '.join(article_content_div.stripped_strings)
            
        except Exception as e:
            print(f"Error get publish date for URL {url}: {e}")
        
        if content == "No content":
            print(f'Failed to get content for {url}')

        article['content']  = content 
    driver.quit()
    return articles

def incremental_crawl_articles():
    driver = setup_driver()
    
    minio_client = connect_minio()
 
    prefix = f'web_crawler/blockworks/blockworks_initial_batch_'
    STATE_FILE = f'web_crawler/blockworks/blockworks_incremental_crawled_at_'
    last_crawled = get_last_crawled(STATE_FILE=STATE_FILE, minio_client=minio_client, bucket=CRYPTO_NEWS_BUCKET, prefix=prefix)
    URL = f"https://blockworks.co/news"
    print(f"Crawling URL: {URL}")

    # Open the URL
    driver.get(URL)

    # Wait for the articles to load initially
    wait_for_page_load(driver, 'main.w-full')

    articles_data = []
    crawled_id = set()
    previous_news = 0 
    count = 0
    complete = False
    while not complete:
        # Get all the articles on the current page
        container = driver.find_element(By.XPATH, "//div[contains(@class, 'grid lg:grid-cols-4')]")

        # Find all the articles within the container
        data_div = container.find_elements(By.CSS_SELECTOR, "div.grid.grid-cols-1")
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
                title_element = article.find_element(By.CSS_SELECTOR, "a.font-headline")
                article_url = title_element.get_attribute("href")
                article_id = generate_url_hash(article_url)
                if article_id in crawled_id:
                    continue
                # Skip if the article URL has already been processed
                if article_id in last_crawled:
                    articles_data = get_detail_article(articles=articles_data)
                    object_key = f'{STATE_FILE}{int(datetime.now().timestamp())}.json'
                    upload_json_to_minio(json_data=articles_data, object_key=object_key)
                    articles_data = []
                    complete = True
                    break
                date_str = article.find_element(By.CSS_SELECTOR, 'time').get_attribute("datetime").strip()
                articles_data.append({
                    "id": article_id,
                    "title": title_element.text.strip(),
                    "url": article_url,
                    "published_at": datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S.%fZ").strftime("%Y-%m-%d %H:%M:%S"),
                    "source": "blockworks.co"
                })
                crawled_id.add(article_id)
            except Exception as e:
                print(f"Error extracting data for an article: {e}")
            
        retries = 3
        retries_count = 0
        try:
            driver.execute_script("arguments[0].scrollIntoView();", articles[-1])
            previous_news = current_news
            retries_count = 0
        except IndexError:
            print(f"Get Error in load more news retries {retries_count}/{retries}")
            retries_count+=1
            if retries_count > retries:
                break
        # Wait for new articles to load

        time.sleep(random.uniform(2, 4))
    if articles_data:
        articles_data = get_detail_article(articles=articles_data)
        object_key = f'{STATE_FILE}{int(datetime.now().timestamp())}.json'
        upload_json_to_minio(json_data=articles_data, object_key=object_key)

    driver.quit()
    print("Crawling completed.")
