import time, random
from datetime import date
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException
from concurrent.futures import ThreadPoolExecutor
from crawler_utils.minio_utils import upload_json_to_minio, connect_minio
from crawler_utils.common_utils import generate_url_hash, get_last_crawled, get_last_initial_crawled
from crawler_utils.chrome_driver_utils import setup_driver, wait_for_page_load
from crawler_config.storage_config import CRYPTO_NEWS_BUCKET
from bs4 import BeautifulSoup
from crawler_constants.crawl_constants import Cointelegraph

tags = Cointelegraph.tags


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

def incremental_crawl_articles(tag):
    driver = setup_driver()
    minio_client = connect_minio()

    prefix = f'web_crawler/cointelegraph/{tag}/cointelegraph_{tag}_initial_batch_'
    STATE_FILE = f'web_crawler/cointelegraph/{tag}/cointelegraph_{tag}_incremental_crawled_at_'
    last_crawled = get_last_crawled(STATE_FILE=STATE_FILE, minio_client=minio_client, bucket=CRYPTO_NEWS_BUCKET, prefix=prefix)
    URL = f"https://www.cointelegraph.com/tags/{tag}"
    print(f"Crawling URL: {URL}")

    # Open the URL
    driver.get(URL)
    wait_for_page_load(driver,"div.tag-page")
    try:
        accept_cookies = driver.find_element(By.XPATH, '//button[@class="btn privacy-policy__accept-btn"]')
        accept_cookies.click()
    except NoSuchElementException:
        print("No Accept cookies to click")

    articles_data = []
    crawled_id = set()
    previous_news = 0
    complete = False
    while not complete:
        # Dynamically re-locate articles to avoid stale element issues
        data_div = driver.find_elements(By.CSS_SELECTOR, "div.post-card-inline__content")
        current_news = len(data_div)
        if current_news == previous_news:
            time.sleep(3)
        articles = data_div[previous_news : current_news]
        print(f"Crawling news from {previous_news} to {current_news} news of {tag}")

        for article in articles:
            try:
                # Extract the article data
                title_element = article.find_element(By.CSS_SELECTOR, "a.post-card-inline__title-link")
                article_url = title_element.get_attribute("href")
                article_id = generate_url_hash(article_url)

                if article_id in crawled_id:
                    continue

                if article_id in last_crawled:
                    articles_data = get_detail_article(articles=articles_data)
                    object_key = f'web_crawler/cointelegraph/{tag}/cointelegraph_{tag}_incremental_crawled_at_{date.today()}.json'
                    upload_json_to_minio(json_data=articles_data, object_key=object_key)
                    complete = True
                    break
                
                date_element = article.find_element(By.CSS_SELECTOR, "time")
                published_at = date_element.get_attribute("datetime")

                # Add the article data to the list
                articles_data.append(
                    {
                        "id": article_id,
                        "title": title_element.text,
                        "published_at": f"{published_at} 00:00:00",
                        "url": article_url,
                        "source": "cointelegraph.com",
                    }
                )
                crawled_id.add(article_id)

            except Exception as e:
                print(f"Error extracting data for an article: {e}")

        retries = 3        
        retries_count = 0
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

    # Ensure the driver quits properly
    driver.quit()
    print("Crawling completed.")



