import json
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
import time
from concurrent.futures import ThreadPoolExecutor

def setup_driver():
    """Set up the Chrome WebDriver with the appropriate service."""
    options = Options()
    options.add_argument("--headless") 
    options.add_argument("--disable-gpu") 
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service)
    return driver

# Function to wait for articles to load
def wait_for_articles(driver):
    """Wait for the articles to load on the page."""
    wait = WebDriverWait(driver, 10)
    wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "div.post-card-inline__content")))

# Function to extract articles from the page
def extract_articles(driver, TARGET_DATE: str, max_retries: int = 5) -> list:
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
                
                # Skip if the article URL has already been processed
                if article_url in processed_urls:
                    continue
                title = title_element.text
                
                # Extract published date
                date_element = article.find_element(By.CSS_SELECTOR, "time")
                published_at = date_element.get_attribute("datetime")

                # Stop extraction if the article is older than the target date
                if published_at < TARGET_DATE:
                    print(f"No more articles to load after {TARGET_DATE}.")
                    return articles_data  # No more articles to process

                # Extract content snippet (if available)
                content_element = article.find_element(By.CSS_SELECTOR, "p.post-card-inline__text")
                content = content_element.text

                # Add the article data to the list
                articles_data.append({
                    "id": article_url,
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

        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(2)

# Function to save articles data to a JSON file
def save_to_json(articles_data, filename="articles.json"):
    """Save the extracted articles data to a JSON file."""
    if articles_data:
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(articles_data, f, ensure_ascii=False, indent=4)
        print(f"Data has been written to {filename}")
    else:
        print("No articles to save.")

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
    wait_for_articles(driver)

    # Crawl articles by scrolling and extracting data
    articles_data = extract_articles(driver, TARGET_DATE)

    # Close the driver after crawling
    driver.quit()

    # Check if there were any articles found after the target date
    if not articles_data:
        print(f"No new articles found after {TARGET_DATE}.")
    else:
        # Save the extracted articles to a JSON file
        save_to_json(articles_data, filename=f"data/{tag}.json")

def multithreading_tag_crawler(TARGET_DATE: str):
    tags = ['bitcoin', 'ethereum', 'altcoin', 'blockchain', 'defi', 'regulation', 'business', 'nft', 'ai', 'adoption']
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
    multithreading_tag_crawler(TARGET_DATE)
