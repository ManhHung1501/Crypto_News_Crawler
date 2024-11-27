import time
from datetime import datetime, date
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException
from utils.minio_utils import upload_json_to_minio
from utils.common_utils import generate_url_hash, get_last_crawled,save_last_crawled
from utils.chrome_driver_utils import setup_driver, wait_for_page_load


# Get total page
def get_total_page(driver):
    try:
        driver.get(f"https://cryptoslate.com/news")
        time.sleep(3)
        # Locate the element containing the total pages (hidden link with the highest number)
        last_page_element = driver.find_element(By.CSS_SELECTOR, 'div.pagination a.page-numbers[style="display: none;"]')
        href_value = last_page_element.get_attribute("href")
        # Extract and return the total pages as an integer
        total_pages = int(href_value.split('/')[-2].replace(',', ''))
        return total_pages
    except Exception as e:
        print(f"Error while finding total pages: {e}")
        return None

# Function to extract articles from the page
def extract_articles(driver, last_crawled: list = [], max_records: int = None) -> list:
    """Extract articles from the page, return a list of articles data."""
    articles_data = []
    # Open the URL
    total_page = get_total_page(driver)
    if total_page == None:
        return articles_data
    for i in range(total_page):
        page = i + 1
        URL = f"https://cryptoslate.com/news/page/{page}/"
        driver.get(URL)
        wait_for_page_load(driver, 'section.news-feed')

        # Get all the articles on the current page
        news_feed = driver.find_element(By.CSS_SELECTOR, 'section.news-feed')
        articles = news_feed.find_elements(By.CSS_SELECTOR, "div.list-post")
        for article in articles:
            try:
                # Extract title
                title = article.find_element(By.CSS_SELECTOR, "div.title h2").text
                article_url = article.find_element(By.CSS_SELECTOR, "a").get_attribute("href")
                article_id = generate_url_hash(article_url)
                
                # check for new article
                if article_id in last_crawled:
                    return articles_data

                estimate_time = article.find_element(By.CSS_SELECTOR, "div.post-meta span:nth-child(2)").text
                # Extract content
                content = article.find_element(By.CSS_SELECTOR, "div.excerpt p").text
                
                # Add the article data to the list
                articles_data.append({
                    "id": article_url,
                    "title": title,
                    "published_at": estimate_time,
                    "content": content,
                    "url": article_url,
                    "source": "cryptoslate.com"
                })

                if max_records:
                    if len(articles_data) == max_records:
                        return articles_data
                    
            except Exception as e:
                print(f"Error extracting data for an article: {e}")
        
    return articles_data

def get_publish_at(driver, articles):
    for article in articles:
        retries = 3
        published_at = "1970-01-01 00:00:00"
        url = article['url']
        for attempt in range(retries):
            # Navigate to article URL
            driver.get(url)
            try:
                post_header_div = driver.find_element(By.CSS_SELECTOR, "div.post-header.article")
            except NoSuchElementException :
                print(f"Retry to get publish date of {url} (Attempt {attempt + 1}/{retries})")
                if attempt < retries - 1:
                    driver.refresh()
                    time.sleep(2)
                continue

            try:   
                published_element = post_header_div.find_element(By.CSS_SELECTOR, ".post-meta-single.sponsored .text span")
                raw_date = published_element.text.replace("Published", "").strip()
                published_at = datetime.strptime(raw_date, "%b. %d, %Y at %I:%M %p UTC").strftime("%Y-%m-%d %H:%M:%S")
            except NoSuchElementException:
                try:
                    post_date_element = driver.find_element(By.CSS_SELECTOR, ".author-info .post-date")
                    time_element = post_date_element.find_element(By.CSS_SELECTOR, ".time")
                    raw_date = post_date_element.text.replace(time_element.text, "").strip() + " " + time_element.text.replace("at ", "").strip()
                    published_at = datetime.strptime(raw_date, "%b. %d, %Y %I:%M %p UTC").strftime("%Y-%m-%d %H:%M:%S")
                except NoSuchElementException:
                    pass

            if published_at == "1970-01-01 00:00:00":
                print(f"Failed to get publish date of {url}")
            article['published_at'] = published_at
        
    return articles

# Main function to orchestrate the crawling
def crawl_articles(max_records: int = None, type_crawl: str = 'incremental'):
    """function to set up the driver, crawl articles, and save them."""
    # URL to scrape
    
    # Set up the WebDriver
    driver = setup_driver()
    # Crawl articles
    
    STATE_FILE = f'last_crawled/cryptoslate/last_crawled_id.json'
    if type_crawl == 'incremental':
        last_crawled = get_last_crawled(STATE_FILE=STATE_FILE)
    elif type_crawl == 'full':
        last_crawled = []
    else:
        raise Exception("Invalid type crawl")

    articles_data = extract_articles(driver=driver, max_records=max_records, last_crawled=last_crawled)
    articles_data = get_publish_at(driver, articles_data)

    print(f"Success crawled {len(articles_data)} news")
    
    # Save last crawled news
    save_last_crawled([article['id'] for article in articles_data[:5]], STATE_FILE= STATE_FILE)

    # Close the driver after crawling
    driver.quit()

    # Check if there were any articles found after the target date
    if not articles_data:
        print(f"No new articles found.")
    else:
        # Save the extracted articles to a JSON file
        object_key = f'web_crawler/cryptoslate/cryptoslate_{type_crawl}_crawled_at_{date.today()}.json'
        upload_json_to_minio(json_data=articles_data,object_key=object_key)

# Run the crawling process
if __name__ == "__main__":
    crawl_articles()
