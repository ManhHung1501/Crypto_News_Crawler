import time, random, requests
from bs4 import BeautifulSoup
from requests.exceptions import Timeout
from datetime import date, datetime
from selenium.webdriver.common.by import By
from concurrent.futures import ThreadPoolExecutor
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (NoSuchElementException, 
            ElementClickInterceptedException, TimeoutException, 
            StaleElementReferenceException)
from crawler_utils.minio_utils import upload_json_to_minio, connect_minio
from crawler_utils.common_utils import generate_url_hash, get_last_crawled, save_last_crawled, get_last_initial_crawled
from crawler_utils.chrome_driver_utils import setup_driver
from crawler_config.storage_config import CRYPTO_NEWS_BUCKET
from crawler_constants.crawl_constants import Coindesk

topics = Coindesk.topics


def handle_cookie_consent(driver):
    """
    Handles the cookie consent popup by clicking the "Allow all cookies" button.
    """
    try:
        # Wait for the "Allow all cookies" button to be visible
        wait = WebDriverWait(driver, 10, poll_frequency=0.5)  
        accept_cookies = wait.until(EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'Allow all cookies')]")))
        accept_cookies.click()
        print("Cookie consent accepted: 'Allow all cookies' button clicked.")
    except NoSuchElementException:
        print("Cookie consent popup not found.")
    except ElementClickInterceptedException:
        print("Could not click the cookie consent button.")
    except TimeoutException:
        print("No cookies prompt displayed.")

# Function to extract articles from the page
def extract_articles(driver, last_crawled: list = [], max_records: int = None, max_retries: int = 5) -> list:
    """Extract articles from the page, return a list of articles data."""
    articles_data = []
    processed_urls = set()  # To avoid reprocessing the same article
    last_article_count = 0

    while True:
        # Get all the articles on the current page
        timeline_module = driver.find_element(By.CSS_SELECTOR, 'div[data-module-name="timeline-module"]')
        articles = timeline_module.find_elements(By.CSS_SELECTOR, "div.flex.gap-4")
        for article in articles:
            try:
                # Extract title
                title_element = article.find_element(By.CSS_SELECTOR, "a.text-color-charcoal-900")
                article_url = title_element.get_attribute("href")
                article_id = generate_url_hash(article_url)
                # Skip if the article URL has already been processed
                if article_id in last_crawled:
                    return articles_data
                if article_id in processed_urls:
                    continue
                title = title_element.text
    

                # Add the article data to the list
                articles_data.append({
                    "id": article_id,
                    "title": title,
                    "url": article_url,
                    "source": "coindesk.com"
                })
                if max_records:
                    if len(articles_data) == max_records:
                        return articles_data

                # Mark the URL as processed
                processed_urls.add(article_id)

            except Exception as e:
                print(f"Error extracting data for an article: {e}")

        current_article_count = len(articles)
        if current_article_count == last_article_count:
            retries += 1
            if retries >= max_retries:
                print("No more articles to load after multiple retries.")
                return articles_data
        else:
            retries = 0
        last_article_count = current_article_count

        # Click the "More stories" button to load more articles
        try:
            more_button = driver.find_element(By.CSS_SELECTOR, "button.bg-white.hover\\:opacity-80.cursor-pointer")
            ActionChains(driver).move_to_element(more_button).click().perform()
            print("Clicked on 'More stories' button.")
        except Exception as e:
            print("No 'More stories' button found or could not click: ", e)
            break  
        # Wait for new articles to load
        time.sleep(random.uniform(2, 4))

    return articles_data

# Get publish timestamp
def get_detail_article( articles):
    for article in articles:
        url = article['url']
        published_at = "1970-01-01 00:00:00"
        content = "No content"
        try:
            # Make the HTTP request
            try:
                response = requests.get(url, timeout=15)
                response.raise_for_status() 
            except Timeout:
                print(f"timed out for {url}...")
                time.sleep(10)
            except requests.exceptions.RequestException as e:
                print(f"Request for {url} failed: {e}")
                continue

            # Parse the HTML with BeautifulSoup
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Find the header container
            date_elements = soup.select_one('div[data-module-name="article-header"]').select_one("div.Noto_Sans_xs_Sans-400-xs").select("span")
            if date_elements:
                for span in date_elements:
                    date_text = span.get_text(strip=True).replace(" p.m.", " PM").replace(" a.m.", " AM").replace("UTC", "")
                    if "Published" in date_text:
                        # Process "Published" date
                        cleaned_date_text = date_text.replace("Published", "").replace("UTC", "").strip()
                        try:
                            parsed_date = datetime.strptime(cleaned_date_text, "%b %d, %Y, %I:%M %p")
                            published_at = parsed_date.strftime("%Y-%m-%d %H:%M:%S")
                            break  # Stop after finding "Published"
                        except ValueError:
                            print(f"Unable to parse Published date of {url}: {cleaned_date_text}")
                    elif "Updated" not in date_text:
                        try:
                            parsed_date = datetime.strptime(date_text, "%b %d, %Y, %I:%M %p")
                            published_at = parsed_date.strftime("%Y-%m-%d %H:%M:%S")
                        except ValueError:
                            print(f"Unable to parse fallback date of {url}: {date_text}")

            # content                
            article_header_div = soup.find('div', {'data-module-name': 'article-body'})
            if article_header_div:
                for unwanted in article_header_div.select(".border, .playlistThumb, .article-ad"):
                    unwanted.decompose()
            content = ' '.join(article_header_div.stripped_strings)
            
        except Exception as e:
            print(f"Error get publish date for URL {url}: {e}")
        
        if published_at == "1970-01-01 00:00:00":
            print(f'Failed to get publish date for {url}')
        if content == "No content":
            print(f'Failed to get content for {url}')

        article['content']  = content 
        article['published_at']  = published_at
    return articles

# Main function to orchestrate the crawling
def crawl_articles_by_topic(topic: str, max_records: int = None):
    """function to set up the driver, crawl articles, and save them."""
    # URL to scrape
    URL = f"https://www.coindesk.com/{topic}"
    # Set up the WebDriver
    driver = setup_driver()
       
    # Open the URL
    driver.get(URL)

    # Wait for the articles to load initially
    handle_cookie_consent(driver)
    
    # Crawl articles
    
    STATE_FILE = f'last_crawled/coindesk/{topic}.json'
    minio_client = connect_minio()
    prefix = f'web_crawler/coindesk/{topic}/coindesk_{topic}_initial_batch_'
    last_crawled = get_last_crawled(STATE_FILE=STATE_FILE, minio_client=minio_client, bucket=CRYPTO_NEWS_BUCKET, prefix=prefix)
    

    articles_data = extract_articles(driver=driver, max_records=max_records, last_crawled=last_crawled)
    articles_data = get_detail_article(articles_data)

    print(f"Success crawled {len(articles_data)} news of {topic}")
    
    # Save last crawled news
    save_last_crawled([article['id'] for article in articles_data[:5]], STATE_FILE= STATE_FILE)

    # Close the driver after crawling
    driver.quit()

    # Check if there were any articles found after the target date
    if not articles_data:
        print(f"No new articles found.")
    else:
        # Save the extracted articles to a JSON file
        object_key = f'web_crawler/coindesk/{topic}/coindesk_{topic}_incremental_crawled_at_{date.today()}.json'
        upload_json_to_minio(json_data=articles_data,object_key=object_key)

def multithreading_crawler(max_records: int = None):
    
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = [executor.submit(crawl_articles_by_topic, topic, max_records) for topic in topics]
        for future in futures:
            try:
                future.result()
            except Exception as e:
                print(f"Error in thread of: {e}")

def full_crawl_articles(topic):
    driver = setup_driver()
    batch_size = 1000
    minio_client = connect_minio()
 
    prefix = f'web_crawler/coindesk/{topic}/coindesk_{topic}_initial_batch_'
    last_crawled_id, current_batch = get_last_initial_crawled(minio_client=minio_client, bucket=CRYPTO_NEWS_BUCKET,prefix=prefix)
    URL = f"https://www.coindesk.com/{topic}"
    print(f"Crawling URL: {URL}")

    # Open the URL
    driver.get(URL)

    # Wait for the articles to load initially
    handle_cookie_consent(driver)

    not_crawled = last_crawled_id is None
    article_num = 0
    articles_data = []
    page_size = 10
    retries = 3
    retries_count =1
    previous_news = 0 
    while True:
        # Get all the articles on the current page
        data_div = driver.find_element(By.CSS_SELECTOR, 'div[data-module-name="timeline-module"]').find_elements(By.CSS_SELECTOR, "div.flex.gap-4")
        current_news = len(data_div)
        articles = data_div[article_num: article_num+ page_size]
        print(f"Crawling news from {previous_news} to {current_news} news of {topic} ")
        for article in articles:
            try:
                # Extract title
                title_element = article.find_element(By.CSS_SELECTOR, "a.text-color-charcoal-900")
                article_url = title_element.get_attribute("href")
                article_id = generate_url_hash(article_url)
                # Skip if the article URL has already been processed
                if not not_crawled and article_id == last_crawled_id:
                    not_crawled = True
                    continue
                if not_crawled:
                    title = title_element.text
        
                    # Add the article data to the list
                    articles_data.append({
                        "id": article_id,
                        "title": title,
                        "url": article_url,
                        "source": "coindesk.com"
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
                
                driver.save_screenshot(f'image/error_{current_news}.png')
                time.sleep(3)
        
        # Click the "More stories" button to load more articles
        try:
            more_button = driver.find_element(By.CSS_SELECTOR, "button.bg-white.hover\\:opacity-80.cursor-pointer")
            ActionChains(driver).move_to_element(more_button).click().perform()
            if  current_news == previous_news:
                time.sleep(3)
            else:
                previous_news = current_news
                article_num += page_size
                retries_count = 0
        except NoSuchElementException as e:
            print(f"No 'More stories' button found or could not click on {retries_count}/{retries}")
            retries_count +=1
            if retries_count > retries:
                articles_data = get_detail_article(articles=articles_data)
                object_key = f'{prefix}{current_news}.json'
                upload_json_to_minio(json_data=articles_data,object_key=object_key)
                driver.quit()
                break
        
        except Exception as e:
            print("Error in click more: ", e)
            driver.quit()
            break
                
        # Wait for new articles to load
        time.sleep(random.uniform(2, 4))
    driver.quit()
    
# Run the crawling process
if __name__ == "__main__":
    multithreading_crawler()
