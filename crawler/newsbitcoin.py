import time, random, requests, re
from bs4 import BeautifulSoup
from requests.exceptions import Timeout
from datetime import datetime
from dateutil.relativedelta import relativedelta
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, ElementClickInterceptedException, TimeoutException
from crawler_utils.minio_utils import upload_json_to_minio, connect_minio
from crawler_utils.common_utils import generate_url_hash,get_last_initial_crawled, project_dir
from crawler_utils.chrome_driver_utils import setup_driver, wait_for_page_load
from crawler_config.storage_config import CRYPTO_NEWS_BUCKET

# Function to convert relative time to a datetime object
def parse_date(date_str):
    date_str = date_str.lower().strip()

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
        match = re.match(pattern, date_str, re.IGNORECASE)
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
    try:
        date_obj = datetime.strptime(date_str, "%b %d, %Y")
        # Format to desired string format
        formatted_date = date_obj.strftime("%Y:%m:%d %H:%M:%S")
        return formatted_date
    except ValueError:
        # If no match or valid date, return the default value
        return "1970-01-01 00:00:00"
    

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
                published_at = parse_date(driver.find_element(By.CSS_SELECTOR ,".sc-haIJcD").text.strip())
            except NoSuchElementException:
                print("can't get time_element")
            
            try:
                title = driver.find_element(By.CSS_SELECTOR, ".sc-ePrEVR").text.strip()
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
    wait_for_page_load(driver,"div.sc-iDJa-DH.fTIdPq a.sc-gtMAan.eBbAic")
    
    not_crawled = last_crawled_id is None
    articles_data = []
    batch_size = 100
    while True:
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(1)

        articles = driver.find_elements(By.CSS_SELECTOR, "div.sc-iDJa-DH.fTIdPq a.sc-gtMAan.eBbAic")
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

                # Scroll to the element
                driver.execute_script("arguments[0].scrollIntoView(true);", next_button)

                # Try clicking the button (use JS if necessary)
                try:
                    next_button.click()
                except Exception:
                    print("Click intercepted, using JavaScript click")
                    driver.execute_script("arguments[0].click();", next_button)

                print(f"Scraping page: {driver.current_url}")
                time.sleep(2)
                
            
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
    
# Run the crawling process
if __name__ == "__main__":
    full_crawl_articles('market-updates')
