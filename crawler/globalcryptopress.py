import time, random, requests, re
from bs4 import BeautifulSoup
from requests.exceptions import Timeout
from datetime import datetime, timezone
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from crawler_utils.minio_utils import upload_json_to_minio, connect_minio
from crawler_utils.common_utils import generate_url_hash,get_last_initial_crawled
from crawler_utils.chrome_driver_utils import setup_driver, wait_for_page_load
from crawler_config.storage_config import CRYPTO_NEWS_BUCKET

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
            
            article_card = soup.find("div", class_="post-body entry-content")
            if article_card:
                unwanted_cards = "i, span"
                for unwanted in article_card.select(unwanted_cards):
                    unwanted.decompose()

                content = ' '.join(article_card.stripped_strings)
                content =  re.sub(r'-{2,}', '', content)
            
        except Exception as e: 
            print(f'Error in get content for {url}: ', e)
        if content == "No content":
            print(f"Failed to get content of url: {url}")

        article['content'] = content
    return articles

def full_crawl_articles():
    driver = setup_driver()
    
    minio_client = connect_minio()
 
    prefix = f'web_crawler/globalcryptopress/globalcryptopress_initial_batch_'
    last_crawled_id, current_batch = get_last_initial_crawled(minio_client=minio_client, bucket=CRYPTO_NEWS_BUCKET,prefix=prefix)
    URL = f"https://www.globalcryptopress.com/search"
    print(f"Crawling URL: {URL}")

    # Open the URL
    driver.get(URL)

    # Wait for the articles to load initially
    wait_for_page_load(driver,"div.main.section")

    not_crawled = last_crawled_id is None
    articles_data = []
    crawl_id = set()
    batch_size = 100
    while True:
        articles = driver.find_element(By.CSS_SELECTOR, "div.main.section").find_elements(By.CSS_SELECTOR, "div.panel-post")

        for article in articles:
            try:
                # Extract title
                title_element = article.find_element(By.CSS_SELECTOR, "h3.title-post.entry-title a")
                article_url = title_element.get_attribute("href")
                article_id = generate_url_hash(article_url)
                # Skip if the article URL has already been processed
                if article_id in crawl_id:
                    continue
                if not not_crawled and article_id == last_crawled_id:
                    not_crawled = True
                    continue
                if not_crawled:
                    abbr_element = article.find_element(By.CSS_SELECTOR, "abbr.published")
                    date_str = abbr_element.get_attribute("title").strip()
                    # Add the article data to the list
                    articles_data.append({
                        "id": article_id,
                        "title": title_element.text.strip(),
                        "url": article_url,
                        "published_at": datetime.fromisoformat(date_str).astimezone(timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
                        "source": "globalcryptopress.com"
                    })
                    crawl_id.add(article_id)
                if len(articles_data) == batch_size:
                    articles_data = get_detail_article(articles=articles_data)
                    new_batch = current_batch + batch_size
                    object_key = f'{prefix}{new_batch}.json'
                    upload_json_to_minio(json_data=articles_data,object_key=object_key)
                    current_batch = new_batch
                    articles_data = []
                    crawl_id = set()
            except Exception as e:
                print(f"Error extracting data for an article: {e}")
            
        
        try:
            # Find the "Next" button
            next_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, "//span[@class='displaypageNum']/a[contains(text(),'Next')]"))
            )
            if next_button.is_displayed() and next_button.is_enabled():
                driver.execute_script("arguments[0].scrollIntoView(true);", next_button)
                driver.execute_script("arguments[0].click();", next_button)
                print(f"LEN DATA is {len(articles_data)} after navigated {driver.current_url}")           
            else:
                print("Next button is disabled or hidden.")
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
    
# Run the crawling process
if __name__ == "__main__":
    full_crawl_articles()