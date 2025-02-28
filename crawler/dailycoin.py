import time, random, pytz
from datetime import datetime
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (NoSuchElementException, 
            ElementClickInterceptedException, TimeoutException)
from crawler_utils.minio_utils import upload_json_to_minio, connect_minio
from crawler_utils.common_utils import generate_url_hash,get_last_initial_crawled, get_last_crawled, project_dir
from crawler_utils.chrome_driver_utils import wait_for_page_load
from crawler_config.storage_config import CRYPTO_NEWS_BUCKET
from crawler_config.chrome_config import CHROME_DRIVER_PATH
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium import webdriver
from crawler_utils.mongo_utils import load_json_from_minio_to_mongodb


def setup_vpn_driver():
    service = Service(CHROME_DRIVER_PATH)
    # Path to the VPN extension .crx file
    vpn_extension_path = f"{project_dir}/Touch-VPN-Secure-and-unlimited-VPN-proxy-Chrome-Web-Store.crx"
    chrome_options = Options()
    chrome_options.add_argument("--headless") 
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")

    chrome_options.add_extension(vpn_extension_path)
    driver = webdriver.Chrome(service=service, options=chrome_options)

    driver.get("chrome-extension://bihmplhobchoageeokmgbdihknkjbknd/panel/index.html")
    time.sleep(5)  # Wait for the extension to load (adjust the sleep time as necessary)
    
    tabs = driver.window_handles
    driver.switch_to.window(tabs[0])  # Switch to the first tab (main page)
    # Click on the connection button in the extension
    connect_button = driver.find_element(By.ID, "ConnectionButton")
    connect_button.click()
    # Wait for the connection to establish
    time.sleep(10)
    return driver

def handle_cookie_consent(driver):
    """
    Handles the cookie consent popup by clicking the "Allow all cookies" button.
    """
    try:
        # Wait for the "Allow all cookies" button to be visible
        wait = WebDriverWait(driver, 10, poll_frequency=0.5)  
        accept_cookies = wait.until(EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'Allow all')]")))
        accept_cookies.click()
        print("Cookie consent accepted: 'Allow all cookies' button clicked.")
    except NoSuchElementException:
        print("Cookie consent popup not found.")
    except ElementClickInterceptedException:
        print("Could not click the cookie consent button.")
    except TimeoutException:
        print("No cookies prompt displayed.")

def get_detail_article(articles):
    driver = setup_vpn_driver()
    for article in articles:
        content = "No content"
        published_at = "1970-01-01 00:00:00"
        url = article['url']
        try:
            driver.get(url)            
            soup = BeautifulSoup(driver.page_source, "html.parser")
            date_tag = soup.find("div", class_="mkd-post-info-date").find("time")
            if date_tag:
                raw_date = date_tag['datetime'].strip()
                published_at = datetime.fromisoformat(raw_date).astimezone(pytz.utc).strftime("%Y-%m-%d %H:%M:%S")
                
            article_card = soup.find("div", class_="mkd-post-text-inner clearfix")
            if article_card:
                unwanted_tags = "figure, .code-block, .social-share-icons"
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
    driver.quit()
    return articles

def full_crawl_articles():
    driver = setup_vpn_driver()
    
    minio_client = connect_minio()
 
    prefix = f'web_crawler/dailycoin/dailycoin_initial_batch_'
    last_crawled_id, current_batch = get_last_initial_crawled(minio_client=minio_client, bucket=CRYPTO_NEWS_BUCKET,prefix=prefix)
    URL = f"https://dailycoin.com/crypto-news"
    print(f"Crawling URL: {URL}")

    # Open the URL
    driver.get(URL)
    handle_cookie_consent(driver)
    # Wait for the articles to load initially
    wait_for_page_load(driver, 'div.wpb_wrapper')

    not_crawled = last_crawled_id is None
    articles_data = []
    crawled_id = set()
    batch_size = 100

    while True:
        # Get all the articles on the current page
        container = driver.find_element(By.CSS_SELECTOR, "div.wpb_wrapper")

        # Find all the articles within the container
        articles = container.find_elements(By.CSS_SELECTOR, "div.dc-post-wrapper")
        print(f"Crawling on {driver.current_url}")
        for article in articles:
            try:
                # Extract title
                link_element = article.find_element(By.CSS_SELECTOR, "a.dc-title-link")
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
                        "source": "dailycoin.com"
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
            load_more_button = driver.find_element(By.CSS_SELECTOR, "a.next.page-numbers")
            # Click the button
            driver.execute_script("arguments[0].click();", load_more_button)        
        except Exception as e:
            print("Error in load more: ", e)
            break
                
        # Wait for new articles to load
        time.sleep(random.uniform(2, 3))

    if articles_data:
        # print(f'Len data: {len(articles_data)}')
        # print(articles_data[0])
        # print(articles_data[-1])
        articles_data = get_detail_article(articles=articles_data)
        object_key = f'{prefix}{current_batch + len(articles_data)}.json'
        upload_json_to_minio(json_data=articles_data,object_key=object_key)
    driver.quit()

def incremental_crawl_articles(max_news:int=500):
    driver = setup_vpn_driver()
    
    minio_client = connect_minio()
 
    prefix = f'web_crawler/dailycoin/dailycoin_initial_batch_'
    STATE_FILE = f'web_crawler/dailycoin/dailycoin_incremental_crawled_at_'
    last_crawled = get_last_crawled(STATE_FILE=STATE_FILE, minio_client=minio_client, bucket=CRYPTO_NEWS_BUCKET, prefix=prefix)
    URL = f"https://dailycoin.com/crypto-news"
    print(f"Crawling URL: {URL}")

    # Open the URL
    driver.get(URL)
    handle_cookie_consent(driver)
    # Wait for the articles to load initially
    wait_for_page_load(driver, 'div.wpb_wrapper')

    articles_data = []
    crawled_id = set()

    complete = False
    while not complete:
        # Get all the articles on the current page
        container = driver.find_element(By.CSS_SELECTOR, "div.wpb_wrapper")

        # Find all the articles within the container
        articles = container.find_elements(By.CSS_SELECTOR, "div.dc-post-wrapper")
        print(f"Crawling on {driver.current_url}")
        for article in articles:
            try:
                # Extract title
                link_element = article.find_element(By.CSS_SELECTOR, "a.dc-title-link")
                article_url = link_element.get_attribute("href")
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
                articles_data.append({
                    "id": article_id,
                    "title": link_element.text.strip(),
                    "url": article_url,
                    "source": "dailycoin.com"
                })
                crawled_id.add(article_id)
                
            except Exception as e:
                print(f"Error extracting data for an article: {e}")
    
        # Click the "More stories" button to load more articles
        try:
            load_more_button = driver.find_element(By.CSS_SELECTOR, "a.next.page-numbers")
            # Click the button
            driver.execute_script("arguments[0].click();", load_more_button)        
        except Exception as e:
            print("Error in load more: ", e)
            break
                
        # Wait for new articles to load
        time.sleep(random.uniform(2, 3))

  
    driver.quit()
    print("Crawling completed.")
