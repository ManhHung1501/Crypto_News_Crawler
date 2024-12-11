import time, random, requests, re
from bs4 import BeautifulSoup
from requests.exceptions import Timeout
from datetime import datetime
from dateutil.relativedelta import relativedelta
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, ElementClickInterceptedException, TimeoutException
from crawler_utils.minio_utils import upload_json_to_minio, connect_minio
from crawler_utils.common_utils import generate_url_hash,get_last_initial_crawled
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
    
def handle_cookie_consent(driver):
    """
    Handles the cookie consent popup by clicking the "Allow all cookies" button.
    """
    try:
        # Wait for the "Allow all cookies" button to be visible
        wait = WebDriverWait(driver, 10, poll_frequency=0.5)  
        accept_cookies = wait.until(EC.element_to_be_clickable((By.XPATH, '//button[@id="cookies-eu-accept"]')))
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
            soup = BeautifulSoup(response.content, 'html.parser').find('div', {'class': 'articleMainLoad'})


            author_element = soup.find('span', {'class': 'authorContent'})
            if author_element:
                first_span = author_element.find("span")
                published_at = datetime.strptime(first_span.get_text(strip=True), "%b %d, %Y").strftime("%Y:%d:%m %H:%M:%S") if first_span else "1970-01-01 00:00:00"

            # content                
            data_tag = soup.find('div', {'class': 'contents'})
            if data_tag:
                for unwanted in data_tag.select(".postSubscribe, .kg-card, .border-decryptGridline"):
                    unwanted.decompose()
            content = ' '.join(data_tag.stripped_strings)
            
        except Exception as e:
            print(f"Error get publish date for URL {url}: {e}")
        
        if published_at == "1970-01-01 00:00:00":
            print(f'Failed to get publish date for {url}')
        if content == "No content":
            print(f'Failed to get content for {url}')

        article['content']  = content 
        article['published_at']  = published_at
    return articles

def full_crawl_articles():
    driver = setup_driver()
    minio_client = connect_minio()
    prefix = f'web_crawler/bankless/bankless_initial_batch_'
    last_crawled_id, current_batch = get_last_initial_crawled(minio_client=minio_client, bucket=CRYPTO_NEWS_BUCKET,prefix=prefix)
    URL = f"https://www.bankless.com/read"
    print(f"Crawling URL: {URL}")

    # Open the URL
    driver.get(URL)
    handle_cookie_consent(driver)
    driver.save_screenshot('a.png')
    # Wait for the articles to load initially
    wait_for_page_load(driver,"div.contentList")
    crawled_id = set()
    not_crawled = last_crawled_id is None
    articles_data = []
    batch_size = 100
    previous_news = 0

    while True:
        data_div = driver.find_element(By.CSS_SELECTOR, "div.contentList").find_elements(By.CSS_SELECTOR, "a.articleBlockSmall")
        current_news = (len(data_div))
        if current_news == previous_news:
            time.sleep(3)
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
                        "title": article.find_element(By.CSS_SELECTOR, ".content .title").text.strip(),
                        "source": "bankless.com"
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
            load_more_button = WebDriverWait(driver, 10,poll_frequency=0.5).until(
                EC.element_to_be_clickable((By.CLASS_NAME, "loadMoreFilterBtn"))
            )
            driver.execute_script("arguments[0].click();", load_more_button)
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
    
# Run the crawling process
if __name__ == "__main__":
    full_crawl_articles()



