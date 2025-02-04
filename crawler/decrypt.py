import time, random, requests
from bs4 import BeautifulSoup
from requests.exceptions import Timeout
from datetime import datetime
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
        accept_cookies = wait.until(EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'Allow all')]")))
        accept_cookies.click()
        print("Cookie consent accepted: 'Allow all' button clicked.")
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
        published_at = "1970-01-01 00:00:00"
        content = "No content"
        title = "No Title"
        try:
            # Make the HTTP request
            for _ in range(3):
                try:
                    response = requests.get(url, timeout=10)
                    response.raise_for_status() 
                    break
                except Timeout:
                    print(f"timed out for {url}, retrying...")
                    time.sleep(5)
                except requests.exceptions.RequestException as e:
                    print(f"Request for {url} failed, retrying... : {e}")
                    time.sleep(5)

            # Parse the HTML with BeautifulSoup
            soup = BeautifulSoup(response.content, 'html.parser').find('main')

            title_element = soup.find('header')
            if title_element:
                title = title_element.find("h1").get_text(strip=True)

                datetime_value = title_element.find('time')['datetime']
                published_at = datetime.strptime(datetime_value, "%Y-%m-%dT%H:%M:%SZ").strftime("%Y-%m-%d %H:%M:%S")
            

            # content                
            data_tag = soup.find('div', {'class': 'post-content'})
            if data_tag:
                for unwanted in data_tag.select(".hidden, .relative, .border-decryptGridline"):
                    unwanted.decompose()
            content = ' '.join(data_tag.stripped_strings)
            
        except Exception as e:
            print(f"Error get publish date for URL {url}: {e}")
        
        if published_at == "1970-01-01 00:00:00":
            print(f'Failed to get publish date for {url}')
        if content == "No content":
            print(f'Failed to get content for {url}')
        if title == "No Title":
            print(f'Failed to get Title for {url}')
        
        article['title']  = title 
        article['content']  = content 
        article['published_at']  = published_at
    return articles

def full_crawl_articles():
    driver = setup_driver()
    minio_client = connect_minio()
    prefix = f'web_crawler/decrypt/decrypt_initial_batch_'
    last_crawled_id, current_batch = get_last_initial_crawled(minio_client=minio_client, bucket=CRYPTO_NEWS_BUCKET,prefix=prefix)
    URL = f"https://decrypt.co/news/"
    print(f"Crawling URL: {URL}")

    # Open the URL
    driver.get(URL)
    handle_cookie_consent(driver)
    # Wait for the articles to load initially
    wait_for_page_load(driver,"main")
    crawled_id = set()
    not_crawled = last_crawled_id is None
    articles_data = []
    batch_size = 100
    previous_news = 0
    count = 0
    while True:
        data_div = driver.find_elements(By.CSS_SELECTOR, "main a.linkbox__overlay")
        current_news = (len(data_div))
        if current_news == previous_news:
            if count == 3:
                break
            count += 1
            time.sleep(3)
        else:
            count = 0
        articles = data_div[previous_news:current_news]
        print( f"Crawling news from {previous_news} to {current_news} news")
        for article in articles:
            try:
                # Extract title
                article_url = article.get_attribute("href")
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
                        "url": article_url,
                        "source": "decrypt.co"
                    })
                    crawled_id.add(article_id)
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
                loadmore_button = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.XPATH, "//button[contains(@class, 'mr-4')]/span[text()='Load More']"))
                )

                driver.execute_script("arguments[0].click();", loadmore_button)
                previous_news = current_news
        except NoSuchElementException as e:
            print(f"No 'Load More' button found or could not click on")
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
    prefix = f'web_crawler/decrypt/decrypt_initial_batch_'
    STATE_FILE = f'web_crawler/decrypt/decrypt_incremental_crawled_at_'
    last_crawled = get_last_crawled(STATE_FILE=STATE_FILE, minio_client=minio_client, bucket=CRYPTO_NEWS_BUCKET, prefix=prefix)
    URL = f"https://decrypt.co/news/"
    print(f"Crawling URL: {URL}")

    # Open the URL
    driver.get(URL)
    handle_cookie_consent(driver)
    # Wait for the articles to load initially
    wait_for_page_load(driver,"main")
    crawled_id = set()
    articles_data = []
    previous_news = 0
    count = 0
    complete = False
    while not complete:
        data_div = driver.find_elements(By.CSS_SELECTOR, "main a.linkbox__overlay")
        current_news = (len(data_div))
        if current_news == previous_news:
            if count == 3:
                break
            count += 1
            time.sleep(3)
        else:
            count = 0
        articles = data_div[previous_news:current_news]
        print( f"Crawling news from {previous_news} to {current_news} news")
        for article in articles:
            try:
                # Extract title
                article_url = article.get_attribute("href")
                article_id = generate_url_hash(article_url)
                # Skip if the article URL has already been processed
                if article_id in crawled_id:
                    continue

                if article_id in last_crawled or len(articles_data) == max_news:
                    articles_data = get_detail_article(articles=articles_data)
                    object_key = f'web_crawler/decrypt/decrypt_incremental_crawled_at_{int(datetime.now().timestamp())}.json'
                    upload_json_to_minio(json_data=articles_data, object_key=object_key)
                    complete = True
                    break
                # Add the article data to the list
                articles_data.append({
                    "id": article_id,
                    "url": article_url,
                    "source": "decrypt.co"
                })
                crawled_id.add(article_id)
                
            except Exception as e:
                print(f"Error extracting data for an article: {e}")
            
        # Click the "More stories" button to load more articles
        try:
            # Wait for the "next page" button
                loadmore_button = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.XPATH, "//button[contains(@class, 'mr-4')]/span[text()='Load More']"))
                )

                driver.execute_script("arguments[0].click();", loadmore_button)
                previous_news = current_news
        except NoSuchElementException as e:
            print(f"No 'Load More' button found or could not click on")
            break
        except Exception as e:
            print("Error in click more: ", e)
            break
                
        # Wait for new articles to load
        time.sleep(random.uniform(2, 4))
      
    driver.quit()
    print("Crawling completed.")

