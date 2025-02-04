import time, random, requests, re
from bs4 import BeautifulSoup
from requests.exceptions import Timeout
from datetime import datetime, timezone
from dateutil.relativedelta import relativedelta
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, ElementClickInterceptedException, TimeoutException
from crawler_utils.minio_utils import upload_json_to_minio, connect_minio
from crawler_utils.common_utils import generate_url_hash,get_last_initial_crawled, get_last_crawled
from crawler_utils.chrome_driver_utils import setup_driver, wait_for_page_load
from crawler_config.storage_config import CRYPTO_NEWS_BUCKET


def handle_cookie_consent(driver):
    """
    Handles the cookie consent popup by clicking the "Allow all cookies" button.
    """
    try:
        # Wait for the "Allow all cookies" button to be visible
        wait = WebDriverWait(driver, 10, poll_frequency=0.5)  
        accept_cookies = wait.until(EC.element_to_be_clickable((By.XPATH, '//button[@class="btn btn-cookie"]')))
        accept_cookies.click()
        print("Cookie consent accepted: 'Allow all cookies' button clicked.")
    except NoSuchElementException:
        print("Cookie consent popup not found.")
    except ElementClickInterceptedException:
        print("Could not click the cookie consent button.")
    except TimeoutException:
        print("No cookies prompt displayed.")

# Get publish timestamp
def get_detail_article( articles):
    for article in articles:
        url = article['url']
        content = "No content"
        published_at = "1970-01-01 00:00:00"
        try:
            # Make the HTTP request
            for _ in range(3):
                try:
                    response = requests.get(url, timeout=15)
                    response.raise_for_status()
                    break 
                except Timeout:
                    print(f"timed out for {url}...")
                except requests.exceptions.RequestException as e:
                    print(f"Request for {url} failed: {e}")
                    time.sleep(5)
        
            # Parse the HTML with BeautifulSoup
            soup = BeautifulSoup(response.content, 'html.parser')
            # content                
            article_content_div = soup.select_one('div.content-inner')
            if article_content_div:
                unwanteds_card = ".article-translation, .article-text-banner, .jeg_block_heading, .block-article, .wp-caption-text"
                for unwanted in article_content_div.select(unwanteds_card):
                    unwanted.decompose()
                content = ' '.join(article_content_div.stripped_strings)
            
            meta_tag = soup.find('meta', {'property': 'article:published_time'})
            if meta_tag:
                dt = datetime.strptime(meta_tag['content'].strip(), "%Y-%m-%dT%H:%M:%S%z")
                published_at = dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

        except Exception as e:
            print(f"Error get publish date for URL {url}: {e}")

        if content == "No content":
            print(f'Failed to get content for {url}')
        if published_at == "1970-01-01 00:00:00":
            print(f'Failed to get publish date for {url}')

        article['content']  = content 
        article['published_at']  = published_at
    return articles


def full_crawl_articles():
    driver = setup_driver()
    
    minio_client = connect_minio()
    
    prefix = f'web_crawler/newsbtc/newsbtc_initial_batch_'
    last_crawled_id, current_batch = get_last_initial_crawled(minio_client=minio_client, bucket=CRYPTO_NEWS_BUCKET,prefix=prefix)
    URL = f"https://www.newsbtc.com/news/"
    print(f"Crawling URL: {URL}")

    # Open the URL
    driver.get(URL)

    # Wait for the articles to load initially
    handle_cookie_consent(driver)
    wait_for_page_load(driver, 'div.block-article__content')

    not_crawled = last_crawled_id is None
    articles_data = []
    crawled_id = set()
    batch_size = 100
    previous_news = 0 
    count = 0
    while True:
        # Get all the articles on the current page
        container = driver.find_element(By.CSS_SELECTOR, "div.jeg_main_content ")

        # Find all the articles within the container
        data_div = container.find_elements(By.CSS_SELECTOR, "div.block-article__content")
        current_news = len(data_div)
        if current_news == previous_news:
            if count == 5:
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
                article_url = article.find_element(By.TAG_NAME, "a").get_attribute("href")
                article_id = generate_url_hash(article_url)
                if article_id in crawled_id:
                    continue

                # Skip if the article URL has already been processed
                if not not_crawled and article_id == last_crawled_id:
                    not_crawled = True
                    continue
                if not_crawled:
                    # Add the article data to the list
                    articles_data.append({
                        "id": article_id,
                        "title": article.find_element(By.CSS_SELECTOR, "h4.block-article__title").text,
                        "url": article_url,
                        "source": "newsbtc.com"
                    })
                    crawled_id.add(article_id)
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
            load_more_button = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div.jeg_block_loadmore a"))
            )
            if load_more_button.is_displayed() and load_more_button.is_enabled():
                driver.execute_script("arguments[0].scrollIntoView(true);", load_more_button)
                load_more_button.click()
                previous_news = current_news
            else:
                print("No more articles to load or button not clickable.")
                break
                
            
        except NoSuchElementException as e:
            print(f"No 'More stories' button found")
            break
        except Exception as e:
            print("Error in click more: ", e)
            break
                
        # Wait for new articles to load
        time.sleep(random.uniform(2, 4))

    if articles_data:
        articles_data = get_detail_article(articles=articles_data)
        object_key = f'{prefix}{current_batch + len(articles_data)}.json'
        upload_json_to_minio(json_data=articles_data,object_key=object_key)

    driver.quit()
    
def incremental_crawl_articles(max_news:int=500):
    driver = setup_driver()
    
    minio_client = connect_minio()
    
    prefix = f'web_crawler/newsbtc/newsbtc_initial_batch_'
    STATE_FILE = f'web_crawler/newsbtc/newsbtc_incremental_crawled_at_'
    last_crawled = get_last_crawled(STATE_FILE=STATE_FILE, minio_client=minio_client, bucket=CRYPTO_NEWS_BUCKET, prefix=prefix)
    URL = f"https://www.newsbtc.com/news/"
    print(f"Crawling URL: {URL}")

    # Open the URL
    driver.get(URL)

    # Wait for the articles to load initially
    handle_cookie_consent(driver)
    wait_for_page_load(driver, 'div.block-article__content')

    articles_data = []
    crawled_id = set()
    previous_news = 0 
    complete = False
    while not complete:
        # Get all the articles on the current page
        container = driver.find_element(By.CSS_SELECTOR, "div.jeg_main_content ")

        # Find all the articles within the container
        data_div = container.find_elements(By.CSS_SELECTOR, "div.block-article__content")
        current_news = len(data_div)
        if current_news == previous_news:
            break
        articles = data_div[previous_news: current_news]
        print(f"Crawling news from {previous_news} to {current_news} news")
        for article in articles:
            try:
                # Extract title
                article_url = article.find_element(By.TAG_NAME, "a").get_attribute("href")
                article_id = generate_url_hash(article_url)
                if article_id in crawled_id:
                    continue

                if article_id in last_crawled or len(articles_data) == max_news:
                    articles_data = get_detail_article(articles=articles_data)
                    object_key = f'{STATE_FILE}{int(datetime.now().timestamp())}.json'
                    upload_json_to_minio(json_data=articles_data, object_key=object_key)
                    complete = True
                    break

                # Add the article data to the list
                articles_data.append({
                    "id": article_id,
                    "title": article.find_element(By.CSS_SELECTOR, "h4.block-article__title").text,
                    "url": article_url,
                    "source": "newsbtc.com"
                })
                crawled_id.add(article_id)
               
            except Exception as e:
                print(f"Error extracting data for an article: {e}")
            
        
        # Click the "More stories" button to load more articles
        try:
            load_more_button = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div.jeg_block_loadmore a"))
            )
            if load_more_button.is_displayed() and load_more_button.is_enabled():
                driver.execute_script("arguments[0].scrollIntoView(true);", load_more_button)
                load_more_button.click()
                previous_news = current_news
            else:
                print("No more articles to load or button not clickable.")
                break
                
            
        except NoSuchElementException as e:
            print(f"No 'More stories' button found")
            break
        except Exception as e:
            print("Error in click more: ", e)
            break
                
        # Wait for new articles to load
        time.sleep(random.uniform(2, 4))

    driver.quit()
    print("Crawling completed.")
