import time, random, pytz
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
from crawler_utils.mongo_utils import load_json_from_minio_to_mongodb


def parse_date(date_str):
    # Replace the timezone abbreviations with full names
    if "EST" in date_str:
        date_str = date_str.replace("EST", "-0500")
    elif "EDT" in date_str:
        date_str = date_str.replace("EDT", "-0400")
    elif "UTC" in date_str:
        date_str = date_str.replace("UTC", "-0000")
    else:
        print(f"Invalid time zone in {date_str}")
        return "1970-01-01 00:00:00"
    try:
        # Parse the date string with the offset
        date_obj = datetime.strptime(date_str, "%b %d, %Y, %I:%M%p %z")

        # Convert to UTC
        utc_date = date_obj.astimezone(pytz.utc)

        # Format the date as yyyy:mm:dd hh:mm:ss
        return utc_date.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        print(f'Error in Parse Date {date_str}')
        return "1970-01-01 00:00:00"
    
def handle_cookie_consent(driver):
    """
    Handles the cookie consent popup by clicking the "Allow all cookies" button.
    """
    try:
        wait = WebDriverWait(driver, 10, poll_frequency=0.5)  
        accept_cookies = wait.until(EC.element_to_be_clickable((By.XPATH, '//button[@id="onetrust-accept-btn-handler"]')))
        accept_cookies.click()
        print("Cookie consent accepted: 'I Accept' button clicked.")
    except NoSuchElementException:
        print("Cookie consent popup not found.")
    except ElementClickInterceptedException:
        print("Could not click the cookie consent button.")
    except TimeoutException:
        print("No cookies prompt displayed.")

# Get detail content for article
def get_detail_article(articles):
    for article in articles:
        driver = setup_driver()
        url = article['url']
        content = "No content"
        for retry in range(3):
            try:
                driver.get(url)
                wait_for_page_load(driver, "section.article")
                try:
                    content_element = driver.find_element(By.CSS_SELECTOR, "div.articleContent").find_element(By.ID, "articleContent")
                    soup = BeautifulSoup(content_element.get_attribute("innerHTML"), 'html.parser')
                    for unwanted in soup.select(".copyright"):
                        unwanted.decompose()
                    content = ' '.join(soup.stripped_strings)
                except NoSuchElementException:
                    print(f'Can not get content element')
            except Exception as e:
                print(f"Error get content for URL {url}: {e}")

            article['content'] = content
            
            if content == "No content":
                print(f'Failed to get content for {url} attempt {retry+1}')
                driver.quit()  
                driver = setup_driver()
            else:
                break
        driver.quit()  
            
    return articles

def full_crawl_articles():
    driver = setup_driver()
    
    minio_client = connect_minio()
 
    prefix = f'web_crawler/theblockcrypto/theblockcrypto_initial_batch_'
    last_crawled_id, current_batch = get_last_initial_crawled(minio_client=minio_client, bucket=CRYPTO_NEWS_BUCKET,prefix=prefix)
    URL = f"https://www.theblock.co/latest?start=20000"
    print(f"Crawling URL: {URL}")

    # Open the URL
    driver.get(URL)

    # Wait for the articles to load initially
    wait_for_page_load(driver,"div.articles")
    handle_cookie_consent(driver)
    
    not_crawled = last_crawled_id is None
    articles_data = []
    batch_size = 100
    while True:
        articles = driver.find_element(By.CSS_SELECTOR, "div.articles").find_elements(By.CSS_SELECTOR, "div.articleCard__content")
        for article in articles:
            try:
                # Extract title
                article_url = article.find_element(By.CSS_SELECTOR, "a.appLink.articleCard__link").get_attribute("href")
                article_id = generate_url_hash(article_url)
                # Skip if the article URL has already been processed
                if not not_crawled and article_id == last_crawled_id:
                    not_crawled = True
                    continue
                if not_crawled:
                    date_element = article.find_element(By.CSS_SELECTOR, "div.meta__wrapper")
                    date_text = date_element.text.split("•")[0].strip()
                    # Add the article data to the list
                    articles_data.append({
                        "id": article_id,
                        "title": article.find_element(By.CSS_SELECTOR, "h2.articleCard__headline span").text,
                        "url": article_url,
                        "published_at": parse_date(date_text),
                        "source": "theblockcrypto.com"
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
                EC.element_to_be_clickable((By.CSS_SELECTOR, "li.page-item a.page-link[aria-label='Go to next page']"))
            )
            if next_button.is_displayed() and next_button.is_enabled():
                driver.execute_script("arguments[0].scrollIntoView(true);", next_button)
                print(f"Navigated to the next page {next_button.get_attribute('href')}")
                driver.execute_script("arguments[0].click();", next_button)           
            else:
                print("Next button is disabled or hidden.")
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
    
def incremental_crawl_articles(max_news:int =500):
    driver = setup_driver()
    
    minio_client = connect_minio()
 
    prefix = f'web_crawler/theblockcrypto/theblockcrypto_initial_batch_'
    STATE_FILE = f'web_crawler/theblockcrypto/theblockcrypto_incremental_crawled_at_'
    last_crawled = get_last_crawled(STATE_FILE=STATE_FILE, minio_client=minio_client, bucket=CRYPTO_NEWS_BUCKET, prefix=prefix)
    
    URL = f"https://www.theblock.co/latest?start=0"
    print(f"Crawling URL: {URL}")

    # Open the URL
    driver.get(URL)

    # Wait for the articles to load initially
    wait_for_page_load(driver,"div.articles")
    handle_cookie_consent(driver)
    
    articles_data = []
    complete = False
    while not complete:
        articles = driver.find_element(By.CSS_SELECTOR, "div.articles").find_elements(By.CSS_SELECTOR, "div.articleCard__content")
        for article in articles:
            try:
                # Extract title
                article_url = article.find_element(By.CSS_SELECTOR, "a.appLink.articleCard__link").get_attribute("href")
                article_id = generate_url_hash(article_url)
                if article_id in last_crawled or len(articles_data) == max_news:
                    articles_data = get_detail_article(articles=articles_data)
                    object_key = f'web_crawler/theblockcrypto/theblockcrypto_incremental_crawled_at_{int(datetime.now().timestamp())}.json'
                    upload_json_to_minio(json_data=articles_data, object_key=object_key)
                    complete = True
                    load_json_from_minio_to_mongodb(minio_client, object_key) 
                    break
                
                date_element = article.find_element(By.CSS_SELECTOR, "div.meta__wrapper")
                date_text = date_element.text.split("•")[0].strip()
                # Add the article data to the list
                articles_data.append({
                    "id": article_id,
                    "title": article.find_element(By.CSS_SELECTOR, "h2.articleCard__headline span").text,
                    "url": article_url,
                    "published_at": parse_date(date_text),
                    "source": "theblockcrypto.com"
                })
                
            except Exception as e:
                print(f"Error extracting data for an article: {e}")
            
        
        try:
            # Find the "Next" button
            next_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "li.page-item a.page-link[aria-label='Go to next page']"))
            )
            if next_button.is_displayed() and next_button.is_enabled():
                driver.execute_script("arguments[0].scrollIntoView(true);", next_button)
                print(f"Navigated to the next page {next_button.get_attribute('href')}")
                driver.execute_script("arguments[0].click();", next_button)           
            else:
                print("Next button is disabled or hidden.")
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
        
    driver.quit()
    print("Crawling completed.")