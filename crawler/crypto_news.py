import time, random, requests
from bs4 import BeautifulSoup
from requests.exceptions import Timeout
from datetime import datetime, timezone
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, ElementClickInterceptedException, TimeoutException
from crawler_utils.minio_utils import upload_json_to_minio, connect_minio
from crawler_utils.common_utils import generate_url_hash,get_last_initial_crawled
from crawler_utils.chrome_driver_utils import setup_driver, wait_for_page_load
from crawler_config.storage_config import CRYPTO_NEWS_BUCKET

def handle_cookie_consent(driver):
    """
    Handles the cookie consent popup by clicking the "Allow all cookies" button.
    """
    try:
        # Wait for the "Allow all cookies" button to be visible
        wait = WebDriverWait(driver, 10, poll_frequency=0.5)  
        accept_cookies = wait.until(EC.element_to_be_clickable((By.XPATH, '//button[@id="cookie-consent-button"]')))
        accept_cookies.click()
        print("Cookie consent accepted: 'Allow all cookies' button clicked.")
    except NoSuchElementException:
        print("Cookie consent popup not found.")
    except ElementClickInterceptedException:
        print("Could not click the cookie consent button.")
    except TimeoutException:
        print("No cookies prompt displayed.")

# Get content article
def get_detail_article(articles):
    for article in articles:
        content = "No content"
        url = article['url']
        try:
            for _ in range(3):
                # Make the HTTP request
                try:
                    response = requests.get(url, timeout=15)
                    response.raise_for_status() 
                    break
                except Timeout:
                    print(f'Retrying ...')
                    print(f"timed out. Sleep for {url}...")
                except requests.exceptions.RequestException as e:
                    print(f'Retrying ...')
                    print(f"Request for {url} failed: {e}")
                    time.sleep(5)
                
            soup = BeautifulSoup(response.content, "html.parser")
            
            article_card = soup.find("div", class_="post-detail__content")
            if article_card:
                unwanted_cards = ".post-detail__share, figure, .cn-block-related-link"
                for unwanted in article_card.select(unwanted_cards):
                    unwanted.decompose()

                content = ' '.join(article_card.stripped_strings)
            
        except Exception as e: 
            print(f'Error in get content for {url}: ', e)
        if content == "No content":
            print(f"Failed to get content of url: {url}")

        article['content'] = content
    return articles

def full_crawl_articles():
    driver = setup_driver()
    
    minio_client = connect_minio()
 
    prefix = f'web_crawler/crypto.news/crypto.news_initial_batch_'
    last_crawled_id, current_batch = get_last_initial_crawled(minio_client=minio_client, bucket=CRYPTO_NEWS_BUCKET,prefix=prefix)
    URL = f"https://crypto.news/news/"
    print(f"Crawling URL: {URL}")

    # Open the URL
    driver.get(URL)

    # Wait for the articles to load initially
    wait_for_page_load(driver, 'div.post-archive__content')
    handle_cookie_consent(driver)

    not_crawled = last_crawled_id is None
    articles_data = []
    crawled_id = set()
    batch_size = 100
    previous_news = 0 
    count = 0

    while True:
        # Get all the articles on the current page
        container = driver.find_element(By.CSS_SELECTOR, "div.post-archive__content")

        # Find all the articles within the container
        data_div = container.find_elements(By.CSS_SELECTOR, "div.post-loop")
        current_news = len(data_div)
        if current_news == previous_news:
            if count == 3:
                break
            count += 1
            time.sleep(3)
        else:
            count = 0
        articles = data_div[previous_news: current_news]
        print(f"Crawling news from {previous_news} to {current_news} news")
        
        for article in articles:
            try:
                # Extract title
                link_element = article.find_element(By.CSS_SELECTOR, "a.post-loop__link")
                article_url = link_element.get_attribute("href")
                article_id = generate_url_hash(article_url)
                # Skip if the article URL has already been processed
                if article_id in crawled_id:
                    continue

                if not not_crawled and article_id == last_crawled_id:
                    not_crawled = True
                    continue
                if not_crawled:
                    date_str = article.find_element(By.CSS_SELECTOR, 'time').get_attribute('datetime')
                    # Add the article data to the list
                    articles_data.append({
                        "id": article_id,
                        "title": link_element.text.strip(),
                        "url": article_url,
                        "published_at": datetime.fromisoformat(date_str).astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                        "source": "crypto.news"
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
            iframe_id = "google_ads_iframe_/22877104442/cn-header-sticky_0"
            iframe = driver.find_elements(By.ID, iframe_id)  # Use `find_elements` to avoid exceptions
            if iframe:
                driver.execute_script(f"document.getElementById('{iframe_id}').style.display = 'none';")
            load_more_button = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "button.alm-load-more-btn.more"))
            )
            if load_more_button.is_displayed() and load_more_button.is_enabled():
                driver.execute_script("arguments[0].scrollIntoView(true);", load_more_button)
                load_more_button.click()
                previous_news = current_news
            else:
                print("No more articles to load or button not clickable.")
                break
                
        except Exception as e:
            print("Error in click more: ", e)
            break
                
        # Wait for new articles to load
        time.sleep(random.uniform(2, 3))

    if articles_data:
    
        articles_data = get_detail_article(articles=articles_data)
        object_key = f'{prefix}{current_batch + len(articles_data)}.json'
        upload_json_to_minio(json_data=articles_data,object_key=object_key)

    
# Run the crawling process
if __name__ == "__main__":
    full_crawl_articles()