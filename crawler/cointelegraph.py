import time, random
from selenium.webdriver.common.by import By
from concurrent.futures import ThreadPoolExecutor
from utils.minio_utils import upload_json_to_minio
from utils.common_utils import generate_url_hash
from utils.chrome_driver_utils import setup_driver, wait_for_page_load

# Function to extract articles from the page
def extract_articles(driver, TARGET_DATE: str, max_retries: int = 5) -> list:
    """Extract articles from the page, return a list of articles data."""
    articles_data = []
    processed_urls = set()  # To avoid reprocessing the same article
    last_article_count = 0
    retries = 0
    while True:    
        articles = driver.find_elements(By.CSS_SELECTOR, "div.post-card-inline__content")
        for article in articles:
            try:
                # Extract title
                title_element = article.find_element(By.CSS_SELECTOR, "a.post-card-inline__title-link")
                article_url = title_element.get_attribute("href")
                article_id = generate_url_hash(article_url)
                # Skip if the article URL has already been processed
                if article_id in processed_urls:
                    continue
                title = title_element.text
                
                # Extract published date
                date_element = article.find_element(By.CSS_SELECTOR, "time")
                published_at = date_element.get_attribute("datetime")

                # Stop extraction if the article is older than the target date
                if TARGET_DATE:
                    if published_at < TARGET_DATE:
                        print(f"No more articles to load after {TARGET_DATE}.")
                        return articles_data  # No more articles to process

                # Extract content snippet (if available)
                content_element = article.find_element(By.CSS_SELECTOR, "p.post-card-inline__text")
                content = content_element.text

                # Add the article data to the list
                articles_data.append({
                    "id": article_id,
                    "title": title,
                    "published_at": published_at,
                    "content": content,
                    "url": article_url,
                    "source": "cointelegraph.com"
                })

                # Mark the URL as processed
                processed_urls.add(article_url)
        
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

# Main function to orchestrate the crawling
def crawl_tag_articles(tag: str, TARGET_DATE: str):
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
    
    # Crawl articles by scrolling and extracting data
    articles_data = extract_articles(driver, TARGET_DATE)

    # Close the driver after crawling
    driver.quit()

    # Check if there were any articles found after the target date
    if not articles_data:
        print(f"No new articles found after {TARGET_DATE}.")
    else:
        # Save the extracted articles to a JSON file
        object_key = f'web_crawler/cointelegraph/{tag}/cointelegraph_{tag}_news_after_{TARGET_DATE}.json'
        upload_json_to_minio(json_data=articles_data,object_key=object_key)

def multithreading_crawler(TARGET_DATE: str = None):
    tags = ['bitcoin', 'ethereum', 'altcoin', 'blockchain', 'defi', 'regulation', 'business', 'nft', 'ai', 'adoption']
    if 
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(crawl_tag_articles, tag, TARGET_DATE) for tag in tags]
        for future in futures:
            try:
                future.result()
            except Exception as e:
                print(f"Error in thread of: {e}")

# Run the crawling process
if __name__ == "__main__":
    TARGET_DATE = "2024-11-20"
    multithreading_crawler(TARGET_DATE)
