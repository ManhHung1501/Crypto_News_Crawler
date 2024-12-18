import time, random
from datetime import date
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException
from concurrent.futures import ThreadPoolExecutor
from crawler_utils.minio_utils import upload_json_to_minio, connect_minio
from crawler_utils.common_utils import generate_url_hash, get_last_crawled, save_last_crawled, get_last_initial_crawled
from crawler_utils.chrome_driver_utils import setup_driver, wait_for_page_load
from crawler_config.storage_config import CRYPTO_NEWS_BUCKET
from bs4 import BeautifulSoup
from crawler_constants.crawl_constants import Cointelegraph

tags = Cointelegraph.tags

# Function to extract articles from the page
def extract_articles(driver, last_crawled : list =[], max_records: int = None, max_retries: int = 5) -> list:
    """Extract articles from the page, return a list of articles data."""
    articles_data = []
    processed_urls = set()  # To avoid reprocessing the same article
    last_article_count = 0
    
    while True:    
        articles = driver.find_elements(By.CSS_SELECTOR, "div.post-card-inline__content")
        for article in articles:
            try:
                # Extract title
                title_element = article.find_element(By.CSS_SELECTOR, "a.post-card-inline__title-link")
                article_url = title_element.get_attribute("href")
                article_id = generate_url_hash(article_url)

                # check for new article
                if article_id in last_crawled:
                    return articles_data

                # Skip if the article URL has already been processed
                if article_id in processed_urls:
                    continue

                title = title_element.text
                
                # Extract published date
                date_element = article.find_element(By.CSS_SELECTOR, "time")
                published_at = date_element.get_attribute("datetime")

                # Extract content snippet (if available)
                content_element = article.find_element(By.CSS_SELECTOR, "p.post-card-inline__text")
                content = content_element.text

                # Add the article data to the list
                articles_data.append({
                    "id": article_id,
                    "title": title,
                    "published_at": f"{published_at} 00:00:00",
                    "content": content,
                    "url": article_url,
                    "source": "cointelegraph.com"
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

        driver.execute_script("arguments[0].scrollIntoView();", articles[-1])
        time.sleep(random.uniform(2, 4))

# Get detail content for article
def get_detail_article(articles):
    driver = setup_driver()
    for article in articles:
        url = article['url']
        content = "No content"
        try:
            driver.get(url)
            wait_for_page_load(driver, ".post__article")

            # Get the page source and parse it with BeautifulSoup
            soup = BeautifulSoup(driver.page_source, 'html.parser')

            # Find the article content
            article_cards = soup.find("article", class_="post__article")
            if article_cards:
                for unwanted in article_cards.select(".post-meta, .post__title, .newsletter-subscription-form_k9oQq, .tags-list, .related-list, .reactions_3eiuR"):
                    unwanted.decompose()
                # Extract all text content and concatenate it
                content = ' '.join(article_cards.stripped_strings)
            else:
                explained_blocks = soup.find_all('div', attrs={'data-ct-widget': 'explained-block'})
                if explained_blocks:
                    content = ""
                    for block in explained_blocks:
                        title = block.find('h2', attrs={'data-ct-widget': 'explained-block-title'})
                        title_text = title.get_text(strip=True) if title else "No Title"

                        # Extract the content from paragraphs
                        paragraphs = block.find_all('p')
                        content += title_text + " " + " ".join(p.get_text(strip=True) for p in paragraphs if p.get_text(strip=True))

        except Exception as e:
            print(f'Error in url {url}: {e}')
        
        if content in ('No content', ''):
            content = 'No content'
            print(f'Failed to get content for {url}')

        article['content'] = content
    driver.quit()
    return articles

# Main function to orchestrate the crawling
def crawl_articles_by_tag(tag: str, max_records: int = None):
    """function to set up the driver, crawl articles, and save them."""
    # URL to scrape
    URL = f"https://cointelegraph.com/tags/{tag}"
    # Set up the WebDriver
    driver = setup_driver()
       
    # Open the URL
    driver.get(URL)
    
    # Wait for the articles to load initially
    wait_for_page_load(driver,  "div.tag-page")
    accept_cookies = driver.find_element(By.XPATH, '//button[@class="btn privacy-policy__accept-btn"]')
    accept_cookies.click()
    
    STATE_FILE = f'last_crawled/cointelegraph/{tag}.json'
    minio_client = connect_minio()
    prefix = f'web_crawler/cointelegraph/{tag}/cointelegraph_{tag}_initial_batch_'
    last_crawled = get_last_crawled(STATE_FILE=STATE_FILE, minio_client=minio_client, bucket=CRYPTO_NEWS_BUCKET, prefix=prefix)
    
    # Crawl articles by scrolling and extracting data
    articles_data = extract_articles(driver=driver, max_records=max_records, last_crawled=last_crawled)
    print(f"Success crawled {len(articles_data)} news of {tag}")
    
    # Save last crawled news
    save_last_crawled([article['id'] for article in articles_data[:5]], STATE_FILE= STATE_FILE)

    # Close the driver after crawling
    driver.quit()

    # Check if there were any articles found after the target date
    if not articles_data:
        print(f"No new articles found.")
    else:
        # Save the extracted articles to a JSON file
        object_key = f'web_crawler/cointelegraph/{tag}/cointelegraph_{tag}_incremental_crawled_at_{date.today()}.json'
        upload_json_to_minio(json_data=articles_data,object_key=object_key)

def multithreading_crawler(max_records: int = None):
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(crawl_articles_by_tag, tag, max_records) for tag in tags]
        for future in futures:
            try:
                future.result()
            except Exception as e:
                print(f"Error in thread of: {e}")

def full_crawl_articles(tag):
    driver = setup_driver()
    batch_size = 100
    minio_client = connect_minio()
    
    prefix = f'web_crawler/cointelegraph/{tag}/cointelegraph_{tag}_initial_batch_'
    last_crawled_id, current_batch = get_last_initial_crawled(minio_client=minio_client, bucket=CRYPTO_NEWS_BUCKET,prefix=prefix)
    URL = f"https://www.cointelegraph.com/tags/{tag}"
    print(f"Crawling URL: {URL}")
    # Set up the WebDriver

    # Open the URL
    driver.get(URL)

    # Wait for the articles to load initially
    wait_for_page_load(driver,"div.tag-page")
    try:
        accept_cookies = driver.find_element(By.XPATH, '//button[@class="btn privacy-policy__accept-btn"]')
        accept_cookies.click()
    except NoSuchElementException:
        print("No Accept cookies to click")

    if last_crawled_id:
        not_crawled = False
    else:
        not_crawled = True
    articles_data = []
    crawled_id = set()
    retries = 3
    retries_count =1 
    previous_news = 0
    while True:
        # Get all the articles on the current page
        data_div = driver.find_elements(By.CSS_SELECTOR, "div.post-card-inline__content")
        current_news = len(data_div)
        articles = data_div[previous_news: current_news]
        for article in articles:
            try:
                # Extract title
                title_element = article.find_element(By.CSS_SELECTOR, "a.post-card-inline__title-link")
                article_url = title_element.get_attribute("href")
                article_id = generate_url_hash(article_url)

                if article_id in crawled_id:
                    continue

                # Skip if the article URL has already been processed
                if not not_crawled and article_id == last_crawled_id:
                    not_crawled = True
                    continue
                if not_crawled:
                    # Extract published date
                    date_element = article.find_element(By.CSS_SELECTOR, "time")
                    published_at = date_element.get_attribute("datetime")

                    # Add the article data to the list
                    articles_data.append({
                        "id": article_id,
                        "title": title_element.text,
                        "published_at": f"{published_at} 00:00:00",
                        "url": article_url,
                        "source": "cointelegraph.com"
                    })
                    crawled_id.add(article_id)
                if len(articles_data) == batch_size:
                    articles_data = get_detail_article(articles_data)
                    new_batch = current_batch + batch_size
                    object_key = f'{prefix}{new_batch}.json'
                    upload_json_to_minio(json_data=articles_data,object_key=object_key)
                    
                    current_batch = new_batch
                    articles_data = []
                    crawled_id = set()
            except Exception as e:
                print(f"Error extracting data for an article: {e}")
        
        
        try:
            driver.execute_script("arguments[0].scrollIntoView();", articles[-1])
            print(f"Process from {previous_news} to {current_news}")
            previous_news = current_news
            retries_count = 0
        except IndexError:
            print(f"Get Error in load more news retries {retries_count}/{retries}")
            retries_count+=1
            if retries_count > retries:
                break
        # Wait for new articles to load
        time.sleep(random.uniform(2, 4))
        
    if articles_data:
        articles_data = get_detail_article(articles_data)
        object_key = f'{prefix}{current_batch+len(articles_data)}.json'
        upload_json_to_minio(json_data=articles_data,object_key=object_key)
    driver.quit()

# Run the crawling process
if __name__ == "__main__":
    multithreading_crawler()
