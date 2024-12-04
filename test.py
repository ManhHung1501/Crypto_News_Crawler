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
def convert_relative_time_to_datetime(relative_time_str):
    relative_time_str = relative_time_str.lower()

    # Current time (you can adjust this as needed)
    current_time = datetime.now()

    # Regular expression to match different time units (minute, hour, day, week, month, year)
    time_patterns = {
        'second': r"(\d+)\s*(second|second[s]?)\s*ago",
        'minute': r"(\d+)\s*(minute|minute[s]?)\s*ago",
        'hour': r"(\d+)\s*(hour|hour[s]?)\s*ago",
        'day': r"(\d+)\s*(day|day[s]?)\s*ago",
        'week': r"(\d+)\s*(week|week[s]?)\s*ago",
        'month': r"(\d+)\s*(month|month[s]?)\s*ago",
        'year': r"(\d+)\s*(year|year[s]?)\s*ago",
    }

    # Search for matches and apply the corresponding relativedelta
    for unit, pattern in time_patterns.items():
        match = re.match(pattern, relative_time_str, re.IGNORECASE)
        if match:
            amount = int(match.group(1))
            if unit == 'second':
                return current_time - relativedelta(minutes=amount)
            elif unit == 'minute':
                return current_time - relativedelta(minutes=amount)
            elif unit == 'hour':
                return current_time - relativedelta(hours=amount)
            elif unit == 'day':
                return current_time - relativedelta(days=amount)
            elif unit == 'week':
                return current_time - relativedelta(weeks=amount)
            elif unit == 'month':
                return current_time - relativedelta(months=amount)
            elif unit == 'year':
                return current_time - relativedelta(years=amount)

    # Return None if no match is found
    return "1970-01-01 00:00:00"

def handle_cookie_consent(driver):
    """
    Handles the cookie consent popup by clicking the "Allow all cookies" button.
    """
    try:
        # Wait for the "Allow all cookies" button to be visible
        wait = WebDriverWait(driver, 10, poll_frequency=0.5)  
        accept_cookies = wait.until(EC.element_to_be_clickable((By.XPATH, '//button[@class="btn btn-cookie"]')))
        accept_cookies.click()
        print("Cookie consent accepted: 'Allow all cookies' button clicked.")
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
            date_element = soup.select_one("div.jeg_meta_date").select("span")
            if date_element:
                published_at = convert_relative_time_to_datetime(date_element.get_text(strip=True))      

            # content                
            article_content_div = soup.select_one('div.jeg_main_content')
            if article_content_div:
                for unwanted in article_content_div.select(".related-reading-shortcode, .playlistThumb, .article-ad"):
                    unwanted.decompose()
                content = ' '.join(article_content_div.stripped_strings)
            
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
    batch_size = 1000
    minio_client = connect_minio()
 
    prefix = f'web_crawler/bitcoinist/bitcoinist_initial_batch_'
    last_crawled_id, current_batch = get_last_initial_crawled(minio_client=minio_client, bucket=CRYPTO_NEWS_BUCKET,prefix=prefix)
    URL = f"https://bitcoinist.com/"
    print(f"Crawling URL: {URL}")

    # Open the URL
    driver.get(URL)

    # Wait for the articles to load initially
    handle_cookie_consent(driver)
    wait_for_page_load(driver, 'div.jeg_posts')

    not_crawled = last_crawled_id is None
    article_num = 0
    articles_data = []
    page_size = 12
    retries = 3
    retries_count =1
    previous_news = 0 
    while True:
        # Get all the articles on the current page
        container = driver.find_element(By.CSS_SELECTOR, "div.jeg_posts")

        # Find all the articles within the container
        data_div = container.find_elements(By.CSS_SELECTOR, "article.jeg_post")
        current_news = len(data_div)
        articles = data_div[article_num: article_num+ page_size]
        print(f"Crawling news from {previous_news} to {current_news} news")
        for article in articles:
            try:
                # Extract title
                link_element = article.find_element(By.CSS_SELECTOR, ".jeg_postblock_content .jeg_post_title a")
                article_url = link_element.get_attribute("href")
                article_id = generate_url_hash(article_url)
                # Skip if the article URL has already been processed
                if not not_crawled and article_id == last_crawled_id:
                    not_crawled = True
                    continue
                if not_crawled:
                    title = link_element.text

                    # Add the article data to the list
                    articles_data.append({
                        "id": article_id,
                        "title": title,
                        "url": article_url,
                        "source": "bitcoinist.com"
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
                # Wait for the preloader to disappear
                WebDriverWait(driver, 10).until(
                    EC.invisibility_of_element_located((By.CSS_SELECTOR, "div.module-preloader"))
                )

                # Handle potential overlay or popup
                try:
                    overlay = WebDriverWait(driver, 5).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, "div.slidedown-body-message"))
                    )
                    print("Overlay detected. Attempting to close...")
                    close_button = overlay.find_element(By.CSS_SELECTOR, "button[class*='dismiss-button']")
                    close_button.click()
                    print("Overlay dismissed.")
                except TimeoutException:
                    print("No overlay detected. Proceeding...")

                # Locate the 'Load More' button
                load_more_button = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "div.jeg_block_loadmore a"))
                )

                # Scroll into view and click the button
                driver.execute_script("arguments[0].scrollIntoView(true);", load_more_button)

                # Use JavaScript click to bypass interception
                driver.execute_script("arguments[0].click();", load_more_button)
                print("'Load More' button clicked successfully.")
                    
                    # Wait a bit to allow all new articles to load
                    time.sleep(2)
                    driver.save_screenshot(f'{project_dir}/image/te.png')
            
            else:
                print("No more articles to load.")
                break  

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
    full_crawl_articles()
