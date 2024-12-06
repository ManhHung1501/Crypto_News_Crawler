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

def get_total_page():
    driver = setup_driver()  # Replace with your WebDriver
    driver.get("https://cryptonews.com/news/")  # Replace with the actual URL

    try:
        # Wait for the pagination section to load
        wait = WebDriverWait(driver, 10)
        pagination = wait.until(
            EC.presence_of_element_located((By.CSS_SELECTOR, '.pagination_main'))
        )
        
        # Locate the last page number element
        last_page_element = pagination.find_element(By.XPATH, "(//a[@class='page-numbers'][not(contains(@class, 'next'))])[last()]")
        
        # Extract the total number of pages
        total_pages = int(last_page_element.text.replace(",", ""))
        print(f"Total pages: {total_pages}")
        return total_pages
    except Exception as e:
        print(f"Error: {e}")
    finally:
        driver.quit()

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
    minio_client = connect_minio()
    prefix = f'web_crawler/cryptonews/cryptonews_initial_batch_'
    last_crawled_id, current_batch = get_last_initial_crawled(minio_client=minio_client, bucket=CRYPTO_NEWS_BUCKET,prefix=prefix)

    not_crawled = last_crawled_id is None
    articles_data = []
    batch_size = 100
    page = 1
    total_page = get_total_page()
    while page <= total_page:
        driver = setup_driver()
        if page == 1:
            URL = f"https://cryptonews.com/news/"
        else:
            URL = f"https://cryptonews.com/news/page/{page}/"

        print(f"Crawling URL: {URL}")
        try:
            # Open the URL
            driver.get(URL)
            wait_for_page_load(driver,'.archive-template-latest-news__wrap')

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

            
            print(f'Complete crawled {len(articles)} from page {page}')
            page += 1

        except Exception as e:
            print(f"Error in get data of page {page}: ", e)
            time.sleep(10)
        driver.quit()

    if articles_data:
        articles_data = get_detail_article(articles=articles_data)
        object_key = f'{prefix}{current_batch + len(articles_data)}.json'
        upload_json_to_minio(json_data=articles_data,object_key=object_key)

    driver.quit()
    
# Run the crawling process
if __name__ == "__main__":
    full_crawl_articles()
    
    