import time, random
from bs4 import BeautifulSoup
from datetime import datetime
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from crawler_utils.minio_utils import upload_json_to_minio, connect_minio
from crawler_utils.common_utils import generate_url_hash,get_last_initial_crawled
from crawler_utils.chrome_driver_utils import setup_driver, wait_for_page_load
from crawler_config.storage_config import CRYPTO_NEWS_BUCKET

def remove_unwanted_text(html_parse, unwanted_texts):
    # Iterate over unwanted texts and remove matching elements
    for text in unwanted_texts:
        for tag in html_parse.find_all(string=text):
            tag.extract()  # Remove the matching tag or text

    return html_parse

def get_detail_article(articles):
    driver = setup_driver()
    for article in articles:
        url = article['url']
        content = "No content"
        try:
            driver.get(url)
            wait_for_page_load(driver, "section.w-full")
            # Get the page source and parse it with BeautifulSoup
            article_cards = BeautifulSoup(driver.find_element(By.CSS_SELECTOR, 'section.w-full').get_attribute('innerHTML'), 'html.parser')
            if article_cards:
                for unwanted in article_cards.select(".not-prose, .ads, .subscription, .related-articles"):
                    unwanted.decompose()
        
                unwanted_phrases = [
                    "This is a segment from the Forward Guidance newsletter",
                    "Start your day with top crypto insights",
                    "Explore the growing intersection between crypto",
                    "Get alpha directly in your inbox",
                    "The Lightspeed newsletter is all things Solana"
                ]

                for p_tag in article_cards.find_all("p"):
                    if any(phrase in p_tag.text for phrase in unwanted_phrases):
                        p_tag.decompose()

                # Extract all text content and concatenate it
                content = ' '.join(article_cards.stripped_strings)
            

        except Exception as e:
            print(f'Error in url {url}: {e}')
        
        if content == 'No content':
            print(f'Failed to get content for {url}')

        article['content'] = content
    driver.quit()
    return articles

def close_subcribe_popup(driver):
    try:
        # Wait for the popup to appear
        wait = WebDriverWait(driver, 15, poll_frequency=0.5) 
        popup = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div[role='dialog'][aria-label='Modal Overlay Box']")))
        
        if popup.is_displayed():
            print("Popup detected. Attempting to close it.")
            # Locate the close button within the popup
            close_button = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "div[title='Close']")))
            # Click the close button to close the popup
            close_button.click()
            print("Popup closed successfully.")
        else:
            print("Popup not visible.")
    except TimeoutException:
        print("Popup did not appear within the timeout period.")
    except NoSuchElementException:
        print("Popup or close button not found.")
        
def full_crawl_articles():
    driver = setup_driver()
    minio_client = connect_minio()
    
    prefix = f'web_crawler/blockworks/blockworks_initial_batch_'
    last_crawled_id, current_batch = get_last_initial_crawled(minio_client=minio_client, bucket=CRYPTO_NEWS_BUCKET,prefix=prefix)
    URL = f"https://www.blockworks.com/news/"
    print(f"Crawling URL: {URL}")
    # Set up the WebDriver

    # Open the URL
    driver.get(URL)

    # Wait for the articles to load initially
    wait_for_page_load(driver,"div.grid.grid-cols-1")
    close_subcribe_popup(driver)

    not_crawled = last_crawled_id is None
    articles_data = []
    crawled_id = set()
    batch_size = 100
    retries = 3
    retries_count =1 
    previous_news = 0
    count = 0
    while True:
        # Get all the articles on the current page
        data_div = driver.find_elements(By.CSS_SELECTOR, "div.grid.grid-cols-1.h-full")
        current_news = len(data_div)
        if current_news == previous_news:
            driver.save_screenshot('a.png')
            close_subcribe_popup(driver)
            if count == 3:
                break
            count += 1
            time.sleep(3)
        else:
            count = 0
        print(f"Crawling news from {previous_news} to {current_news} news")
        articles = data_div[previous_news: current_news]
        for article in articles:
            try:
                # Extract title
                title_element = article.find_element(By.CSS_SELECTOR, "div.flex.justify-start.items-start.flex-grow-0").find_element(By.CSS_SELECTOR, "a")
  
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
                    published_at = date_element.get_attribute("datetime").strip()

                    # Add the article data to the list
                    articles_data.append({
                        "id": article_id,
                        "title": title_element.text,
                        "url": article_url,
                        "published_at": datetime.strptime(published_at, "%Y-%m-%dT%H:%M:%S.%fZ").strftime("%Y-%m-%d %H:%M:%S"),
                        "source": "blockworks.co"
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
            previous_news = current_news
            retries_count = 0
        except Exception:
            print(f"Get Error in load more news retries {retries_count}/{retries}")
            retries_count+=1
            if retries_count > retries:
                break
        # Wait for new articles to load
        time.sleep(random.uniform(2, 4))

    driver.quit()
    if articles_data:
        print(f'Len data: {len(articles_data)}')
        print(articles_data[0])
        print(articles_data[-1])
        # articles_data = get_detail_article(articles=articles_data)
        
        # object_key = f'{prefix}{current_batch + len(articles_data)}.json'
        # upload_json_to_minio(json_data=articles_data,object_key=object_key)
  
# Run the crawling process
if __name__ == "__main__":
    full_crawl_articles()

