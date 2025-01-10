import time
from bs4 import BeautifulSoup
from datetime import datetime, date
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, ElementClickInterceptedException, TimeoutException
from crawler_utils.minio_utils import upload_json_to_minio, connect_minio
from crawler_utils.common_utils import generate_url_hash,get_last_initial_crawled,get_last_crawled
from crawler_utils.chrome_driver_utils import setup_driver, wait_for_page_load
from crawler_config.storage_config import CRYPTO_NEWS_BUCKET


def get_total_page():
    driver = setup_driver()  # Replace with your WebDriver
    driver.get("https://beincrypto.com/news")  # Replace with the actual URL

    try:
        # Wait for the pagination section to load
        wait = WebDriverWait(driver, 10)
        pagination = wait.until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'div.content'))
        )
        
        # Locate the last page number element
        last_page_element = pagination.find_element(By.XPATH, '//a[@data-el_pos="bic-c-pagination-last-page"]')
        # Extract the total number of pages
        total_pages = int(last_page_element.text.strip())
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
        accept_cookies = wait.until(EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'Accept')]")))
        accept_cookies.click()
        print("Cookie consent accepted: 'Allow all' button clicked.")
    except NoSuchElementException:
        print("Cookie consent popup not found.")
    except ElementClickInterceptedException:
        print("Could not click the cookie consent button.")
    except TimeoutException:
        print("No cookies prompt displayed.")

def get_detail_article(articles):
    for article in articles:
        driver = setup_driver()
        url = article['url']
        content = "No content"
        published_at = "1970-01-01 00:00:00"
        try:
            driver.get(url)
            wait_for_page_load(driver, "main.main")
            
            # Extract the 'datetime' attribute
            time_element = driver.find_element(By.TAG_NAME, "time")
            datetime_value = time_element.get_attribute("datetime")
            published_at = datetime.strptime(datetime_value, "%Y-%m-%dT%H:%M:%S%z").strftime("%Y-%m-%d %H:%M:%S")

            # Get the page source and parse it with BeautifulSoup
            article_cards = BeautifulSoup(driver.find_element(By.CSS_SELECTOR, 'div.entry-content-inner').get_attribute('innerHTML'), 'html.parser')
            if article_cards:
                for unwanted in article_cards.select(".wp-block-image, .aff-primary, .aff-secondary, .aff-ternary, .rounded-lg.border"):
                    unwanted.decompose()
                # Extract all text content and concatenate it
                content = ' '.join(article_cards.stripped_strings).replace("\n", " ")
            
        except Exception as e:
            print(f'Error in url {url}: {e}')
        
        if content =='No content':
            print(f'Failed to get content for {url}')
        
        if published_at == "1970-01-01 00:00:00":
            print(f'Failed to get published at for {url}')

        article['content'] = content
        article['published_at'] = published_at
        driver.quit()
    return articles

def full_crawl_articles():
    minio_client = connect_minio()
    prefix = f'web_crawler/beincrypto/beincrypto_initial_batch_'
    last_crawled_id, current_batch = get_last_initial_crawled(minio_client=minio_client, bucket=CRYPTO_NEWS_BUCKET,prefix=prefix)

    # Wait for the articles to load initially
    not_crawled = last_crawled_id is None
    articles_data = []
    batch_size = 100
    page = 1 
    total_page = get_total_page()
    while page <= total_page:
        driver = setup_driver()
        URL = f"https://beincrypto.com/news/page/{page}/"
        print(f"Crawling URL: {URL}")
        try:
            # Open the URL
            driver.get(URL)
            wait_for_page_load(driver,"div.content")
            articles = driver.find_element(By.CSS_SELECTOR, "div.content").find_element(By.CSS_SELECTOR, "div.flex.flex-wrap").find_elements(By.XPATH,'//div[@data-el="bic-c-news-big"]')

            for article in articles:
                try:
                    # Extract title
                    title_element = article.find_element(By.CSS_SELECTOR, 'h5.h-full a')
                    article_url = title_element.get_attribute("href")
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
                            "title": title_element.text.strip(),
                            "source": "beincrypto.com"
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
            print( f"Complete Crawled {len(articles)} on page {page}")  
            page +=1
        except Exception as e:
            print(f"Error crawing {URL}")
            time.sleep(5)

        driver.quit()  
    
    if articles_data:
        articles_data = get_detail_article(articles=articles_data)
        object_key = f'{prefix}{current_batch + len(articles_data)}.json'
        upload_json_to_minio(json_data=articles_data,object_key=object_key)

def incremental_crawl_articles():
    minio_client = connect_minio()
    prefix = f'web_crawler/beincrypto/beincrypto_initial_batch_'
    STATE_FILE = f'web_crawler/beincrypto/beincrypto_incremental_crawled_at_'
    last_crawled = get_last_crawled(STATE_FILE=STATE_FILE, minio_client=minio_client, bucket=CRYPTO_NEWS_BUCKET, prefix=prefix)

    # Wait for the articles to load initially
    articles_data = []
    page = 1 
    complete = False
    while not complete:
        driver = setup_driver()
        URL = f"https://beincrypto.com/news/page/{page}/"
        print(f"Crawling URL: {URL}")
        try:
            # Open the URL
            driver.get(URL)
            wait_for_page_load(driver,"div.content")
            articles = driver.find_element(By.CSS_SELECTOR, "div.content").find_element(By.CSS_SELECTOR, "div.flex.flex-wrap").find_elements(By.XPATH,'//div[@data-el="bic-c-news-big"]')

            for article in articles:
                try:
                    # Extract title
                    title_element = article.find_element(By.CSS_SELECTOR, 'h5.h-full a')
                    article_url = title_element.get_attribute("href")
                    article_id = generate_url_hash(article_url)
                    # Skip if the article URL has already been processed

                    if article_id in last_crawled:
                        articles_data = get_detail_article(articles=articles_data)
                        object_key = f'{STATE_FILE}{int(datetime.now().timestamp())}.json'
                        upload_json_to_minio(json_data=articles_data, object_key=object_key)
                        complete = True
                        break
                    articles_data.append({
                        "id": article_id,
                        "url": article_url,
                        "title": title_element.text.strip(),
                        "source": "beincrypto.com"
                    })
    
                  
                except Exception as e:
                    print(f"Error extracting data for an article: {e}")
            print( f"Complete Crawled {len(articles)} on page {page}")  
            page +=1
        except Exception as e:
            print(f"Error crawing {URL}")
            time.sleep(5)

    driver.quit()  
    print("Crawling completed.")
    


