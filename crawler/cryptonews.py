import time, random, requests, re
from bs4 import BeautifulSoup
import pandas as pd
from requests.exceptions import Timeout
from datetime import datetime
from dateutil.relativedelta import relativedelta
from urllib.parse import urlparse
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, ElementClickInterceptedException, TimeoutException
from crawler_utils.minio_utils import upload_json_to_minio, connect_minio
from crawler_utils.common_utils import generate_url_hash,get_last_initial_crawled, project_dir
from crawler_utils.chrome_driver_utils import setup_driver, wait_for_page_load
from crawler_config.storage_config import CRYPTO_NEWS_BUCKET

def convert_relative_time_to_datetime(relative_time_str):
    relative_time_str = relative_time_str.lower().strip()

    # Current time
    current_time = datetime.now()

    # Regular expression to match different time units (second, minute, hour, day, week, month, year)
    time_patterns = {
        'second': r"(\d+)\s*(second|seconds)\s*ago",
        'minute': r"(\d+)\s*(minute|minutes)\s*ago",
        'hour': r"(\d+)\s*(hour|hours)\s*ago",
        'day': r"(\d+)\s*(day|days)\s*ago",
        'week': r"(\d+)\s*(week|weeks)\s*ago",
        'month': r"(\d+)\s*(month|months)\s*ago",
        'year': r"(\d+)\s*(year|years)\s*ago",
    }

    # Search for matches and apply the corresponding relativedelta
    for unit, pattern in time_patterns.items():
        match = re.search(pattern, relative_time_str, re.IGNORECASE)
        if match:
            amount = int(match.group(1))
            if unit == 'second':
                calculated_time = current_time - relativedelta(seconds=amount)
            elif unit == 'minute':
                calculated_time = current_time - relativedelta(minutes=amount)
            elif unit == 'hour':
                calculated_time = current_time - relativedelta(hours=amount)
            elif unit == 'day':
                calculated_time = current_time - relativedelta(days=amount)
            elif unit == 'week':
                calculated_time = current_time - relativedelta(weeks=amount)
            elif unit == 'month':
                calculated_time = current_time - relativedelta(months=amount)
            elif unit == 'year':
                calculated_time = current_time - relativedelta(years=amount)
            
            # Return the result in 'yyyy-mm-dd hh:mm:ss' format
            return calculated_time.strftime('%Y-%m-%d %H:%M:%S')
    
    # If no match found, print error and return default value
    print(f"Cannot parse date: {relative_time_str}")
    return "1970-01-01 00:00:00"

def handle_cookie_consent(driver):
    """
    Handles the cookie consent popup by clicking the "Allow all cookies" button.
    """
    try:
        # Wait for the "Allow all cookies" button to be visible
        wait = WebDriverWait(driver, 10, poll_frequency=0.5)  
        accept_cookies = wait.until(EC.element_to_be_clickable((By.XPATH, '//button[@aria-label="Confirm all"]')))
        accept_cookies.click()
        print("Cookie consent accepted: 'Allow all cookies' button clicked.")
    except NoSuchElementException:
        print("Cookie consent popup not found.")
    except ElementClickInterceptedException:
        print("Could not click the cookie consent button.")
    except TimeoutException:
        print("No cookies prompt displayed.")

def get_source(url):
    parsed_url = urlparse(url)
    # Extract netloc and remove 'www.' if present
    domain = parsed_url.netloc.replace('www.', '')
    return domain

# Get publish timestamp
def get_detail_article(articles):
    for article in articles:
        driver = setup_driver()
        url = article['url']
        content = "No content"
        published_at = "1970-01-01 00:00:00"
        try:
            driver.get(url)
            wait_for_page_load(driver, 'div.article-single__content.category_contents_details')
            
            meta_tag = driver.find_element(By.CSS_SELECTOR, 'meta[property="article:published_time"]')
            published_time = meta_tag.get_attribute('content')  # Extract the content attribute

            published_at = datetime.fromisoformat(published_time).strftime('%Y-%m-%d %H:%M:%S') 
            # content                
            article_content_element = driver.find_element(By.CSS_SELECTOR,'div.article-single__content.category_contents_details')
            article_content_div = BeautifulSoup(article_content_element.get_attribute("innerHTML"),  'html.parser')
            if article_content_div:
                unwanteds_card = ".mb-10, .image, .twitter-tweet, .single-post-new__tags, .single-post-new__last-updated-mobile, .single-post-new__author-top, .single-post-new__accordion, .dslot, .news-tab-content, .follow-button, .news-tab"
                for unwanted in article_content_div.select(unwanteds_card):
                    unwanted.decompose()
                content = ' '.join(article_content_div.stripped_strings)
            driver.quit()
        except Exception as e:
            print(f"Error get data for URL {url}: {e}")
            time.sleep(2)

        if content == "No content":
            print(f'Failed to get content for {url}')
        if published_at == "1970-01-01 00:00:00":
            print(f'Failed to get publish date for {url}')

        article['content'] = content
        article['published_at'] =published_at
    return articles


def full_crawl_articles():
    driver = setup_driver()
    
    minio_client = connect_minio()
 
    prefix = f'web_crawler/cryptonews/cryptonews_initial_batch_'
    last_crawled_id, current_batch = get_last_initial_crawled(minio_client=minio_client, bucket=CRYPTO_NEWS_BUCKET,prefix=prefix)
    URL = f"https://cryptonews.com/news/"
    print(f"Crawling URL: {URL}")

    # Open the URL
    driver.get(URL)

    # Wait for the articles to load initially
    wait_for_page_load(driver,'.archive-template-latest-news__wrap')
    handle_cookie_consent(driver)
    not_crawled = last_crawled_id is None
    articles_data = []
    batch_size = 100
    while True:
        articles = driver.find_elements(By.CSS_SELECTOR, "div.archive-template-latest-news__wrap")
        for article in articles:
            try:
                article_element = article.find_element(By.CSS_SELECTOR, '.archive-template-latest-news')
                article_url = article_element.get_attribute("href")
                article_id = generate_url_hash(article_url)
                # Skip if the article URL has already been processed
                if not not_crawled and article_id == last_crawled_id:
                    not_crawled = True
                    continue
                if not_crawled:
                    # Add the article data to the list
                    articles_data.append({
                        "id": article_id,
                        "title": article_element.find_element(By.CSS_SELECTOR, '.archive-template-latest-news__title').text,
                        "url": article_url,
                        "source": "cryptonews.com"
                    })
                if len(articles_data) == batch_size:
                    articles_data = get_detail_article(articles=articles_data)
                    new_batch = current_batch + batch_size
                    object_key = f'{prefix}{new_batch}.json'
                    upload_json_to_minio(json_data=articles_data,object_key=object_key)
                    current_batch = new_batch
                    articles_data = []
            except Exception as e:
                print(f"Error extracting data for an article: {e}")
            
        
        try:
            # Find the "Next" button
            next_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, 'a.next.page-numbers'))
            )
            next_button.click()
            print(f"crawling on: {driver.current_url}")
        except NoSuchElementException:
            # Handle the case where the "Next" button is not present
            print("No 'Next' button found. End of pages.")
            break
        except Exception:
            # Handle the case where the element is visible but cannot be clicked
            print("Unable to click the 'Next' button. It might be disabled.")
            break
                
        # Wait for new articles to load
        time.sleep(random.uniform(2, 4))

    if articles_data:
        articles_data = get_detail_article(articles=articles_data)
        object_key = f'{prefix}{current_batch + len(articles_data)}.json'
        upload_json_to_minio(json_data=articles_data,object_key=object_key)

    driver.quit()
    
# Run the crawling process
if __name__ == "__main__":
    full_crawl_articles()
    
    