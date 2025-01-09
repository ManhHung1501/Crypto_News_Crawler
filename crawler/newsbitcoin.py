import time, random, re
from bs4 import BeautifulSoup
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException
from crawler_utils.minio_utils import upload_json_to_minio, connect_minio
from crawler_utils.common_utils import generate_url_hash,get_last_initial_crawled, get_last_crawled
from crawler_utils.chrome_driver_utils import setup_driver, wait_for_page_load
from crawler_config.storage_config import CRYPTO_NEWS_BUCKET

 
# Get detail content for article
def get_detail_article(articles):
    driver = setup_driver()
    for article in articles:
        url = article['url']
        content = "No content"
        title = "No Title"
        published_at = "1970-01-01 00:00:00"
        try:
            driver.get(url)
    
            wait_for_page_load(driver, "div.article__body")
        
            try:
                meta_tag = driver.find_element(By.CSS_SELECTOR, "meta[property='article:published_time']")
                published_time = meta_tag.get_attribute("content")
                dt = datetime.strptime(published_time, "%Y-%m-%dT%H:%M:%S%z")
                published_at = dt.strftime("%Y-%m-%d %H:%M:%S")
            except NoSuchElementException:
                print("can't get time_element")
            
            try:
                title = driver.find_element(By.CSS_SELECTOR, "main h1").text.strip()
            except NoSuchElementException:
                print("can't get title_element")    
            
            try:
                element = driver.find_element("css selector", ".article__body")

                # Get the inner HTML of the element
                inner_html = element.get_attribute("innerHTML")

                # Parse the HTML with BeautifulSoup
                soup = BeautifulSoup(inner_html, 'html.parser')

                # Extract text content from the div
                content = ' '.join(soup.stripped_strings)

            except NoSuchElementException:
                print("can't get title_element")
            

        except Exception as e:
            print(f'Error in url {url}: {e}')
        
        if title == "No Title":
            print(f'Failed to get Title for {url}')
        
        if published_at == "1970-01-01 00:00:00":
            print(f'Failed to get Publish date for {url}')

        if content in ('No content', ''):
            content = 'No content'
            print(f'Failed to get content for {url}')

        article['title'] = title
        article['published_at'] = published_at
        article['content'] = content
        
    driver.quit()
    return articles

def full_crawl_articles(category):
    driver = setup_driver()
    
    minio_client = connect_minio()
 
    prefix = f'web_crawler/news.bitcoin/{category}/news.bitcoin_{category}_initial_batch_'
    last_crawled_id, current_batch = get_last_initial_crawled(minio_client=minio_client, bucket=CRYPTO_NEWS_BUCKET,prefix=prefix)
    URL = f"https://news.bitcoin.com/category/{category}/"
    print(f"Crawling URL: {URL}")

    # Open the URL
    driver.get(URL)

    # Wait for the articles to load initially
    wait_for_page_load(driver,"div.sc-gtMAan.jyZwKo a.sc-iDJa-DH.cjkVqL")
    
    not_crawled = last_crawled_id is None
    articles_data = []
    batch_size = 100
    while True:
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(1)

        articles = driver.find_elements(By.CSS_SELECTOR, "div.sc-gtMAan.jyZwKo a.sc-iDJa-DH.cjkVqL")
        for article in articles:
            try:
                # Extract title
                article_url = article.get_attribute("href")
                article_id = generate_url_hash(article_url)
                # Skip if the article URL has already been processed
                if not not_crawled and article_id == last_crawled_id:
                    not_crawled = True
                    continue
                if not_crawled:
                    # Add the article data to the list
                    articles_data.append({
                        "id": article_id,
                        "url": article_url,
                        "source": "news.bitcoin.com"
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
            
        
        # Click the "More stories" button to load more articles
        try:
            # Wait for the "next page" button
                next_button = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.XPATH, '//a[contains(@href, "page") and contains(text(), ">")]'))
                )

               
                driver.execute_script("arguments[0].click();", next_button)

                print(f"Scraping page: {driver.current_url}")
                
            
        except NoSuchElementException as e:
            print(f"No 'More stories' button found or could not click on")
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
 

def incremental_crawl_articles(category):
    driver = setup_driver()
    
    minio_client = connect_minio()
 
    prefix = f'web_crawler/news.bitcoin/{category}/news.bitcoin_{category}_initial_batch_'
    STATE_FILE = f'web_crawler/news.bitcoin/{category}/news.bitcoin_{category}_incremental_crawled_at_'
    last_crawled = get_last_crawled(STATE_FILE=STATE_FILE, minio_client=minio_client, bucket=CRYPTO_NEWS_BUCKET, prefix=prefix)
    URL = f"https://news.bitcoin.com/category/{category}/"
    print(f"Crawling URL: {URL}")

    # Open the URL
    driver.get(URL)

    # Wait for the articles to load initially
    wait_for_page_load(driver,"a h6")

    articles_data = []
    complete = False
    try:
        first_article = driver.find_elements(By.CSS_SELECTOR, "a h5").find_element(By.XPATH, "./..")
        article_url = first_article.get_attribute("href")
        article_id = generate_url_hash(article_url)
        if article_id in last_crawled:
            return
        articles_data.append(
            {
                "id": article_id,
                "url": article_url,
                "source": "news.bitcoin.com"
            }
        )
    except NoSuchElementException:
        return

    while not complete:
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(1)

        articles = driver.find_elements(By.CSS_SELECTOR, "a h6")
        for h6_element in articles:
            try:
                # Extract title
                article = h6_element.find_element(By.XPATH, "./..")
                article_url = article.get_attribute("href")
                article_id = generate_url_hash(article_url)
                
                if article_id in last_crawled:
                    articles_data = get_detail_article(articles=articles_data)
                    object_key = f'{STATE_FILE}{int(datetime.now().timestamp())}.json'
                    upload_json_to_minio(json_data=articles_data, object_key=object_key)
                    complete = True
                    break
               
                # Add the article data to the list
                articles_data.append({
                    "id": article_id,
                    "url": article_url,
                    "source": "news.bitcoin.com"
                })
               
            except Exception as e:
                print(f"Error extracting data for an article: {e}")
            
        
        # Click the "More stories" button to load more articles
        try:
            # Wait for the "next page" button
                next_button = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.XPATH, '//a[contains(@href, "page") and contains(text(), ">")]'))
                )
                driver.execute_script("arguments[0].click();", next_button)
                print(f"Scraping page: {driver.current_url}")
        except NoSuchElementException as e:
            print(f"No 'More stories' button found or could not click on")
            break
        except Exception as e:
            print("Error in click more: ", e)
            break
                
        # Wait for new articles to load
        time.sleep(random.uniform(2, 4))
        
    driver.quit()
    print("Crawling completed.")
