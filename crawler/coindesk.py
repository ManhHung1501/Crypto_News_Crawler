import time, random
from selenium.webdriver.common.by import By
from concurrent.futures import ThreadPoolExecutor
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, ElementClickInterceptedException
from selenium.common.exceptions import TimeoutException
from utils.minio_utils import upload_json_to_minio
from utils.common_utils import parse_coindesk_date, generate_url_hash
from utils.chrome_driver_utils import setup_driver

def handle_cookie_consent(driver):
    """
    Handles the cookie consent popup by clicking the "Allow all cookies" button.
    """
    try:
        # Wait for the "Allow all cookies" button to be visible
        wait = WebDriverWait(driver, 10, poll_frequency=0.5)  
        accept_cookies = wait.until(EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'Allow all cookies')]")))
        accept_cookies.click()
        print("Cookie consent accepted: 'Allow all cookies' button clicked.")
    except NoSuchElementException:
        print("Cookie consent popup not found.")
    except ElementClickInterceptedException:
        print("Could not click the cookie consent button.")
    except TimeoutException:
        print("No cookies prompt displayed.")

# Function to extract articles from the page
def extract_articles(driver, TARGET_DATE: str, max_retries: int = 5) -> list:
    """Extract articles from the page, return a list of articles data."""
    articles_data = []
    processed_urls = set()  # To avoid reprocessing the same article
    last_article_count = 0
    retries = 0

    while True:
        # Get all the articles on the current page
        timeline_module = driver.find_element(By.CSS_SELECTOR, 'div[data-module-name="timeline-module"]')
        articles = timeline_module.find_elements(By.CSS_SELECTOR, "div.flex.gap-4")
        for article in articles:
            try:
                # Extract title
                title_element = article.find_element(By.CSS_SELECTOR, "a.text-color-charcoal-900")
                article_url = title_element.get_attribute("href")
                article_id = generate_url_hash(article_url)
                # Skip if the article URL has already been processed
                if article_id in processed_urls:
                    continue
                title = title_element.text
            
                # Extract published date
                time_element = article.find_element(By.CSS_SELECTOR, "p.flex.gap-2.flex-col span")
                published_at = parse_coindesk_date(time_element.text)

                # Stop extraction if the article is older than the target date
                if TARGET_DATE:
                    if published_at < TARGET_DATE:
                        print(f"No more articles to load after {TARGET_DATE}.")
                        return articles_data  # No more articles to process

                # Extract content 
                content_element = article.find_element(By.XPATH, ".//p[contains(@class, 'hidden') and contains(@class, 'md:block')]")
                content = content_element.text
    
                # Add the article data to the list
                articles_data.append({
                    "id": article_id,
                    "title": title,
                    "published_at": published_at,
                    "content": content,
                    "url": article_url,
                    "source": "coindesk.com"
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

        # Click the "More stories" button to load more articles
        try:
            more_button = driver.find_element(By.CSS_SELECTOR, "button.bg-white.hover\\:opacity-80.cursor-pointer")
            ActionChains(driver).move_to_element(more_button).click().perform()
            print("Clicked on 'More stories' button.")
        except Exception as e:
            print("No 'More stories' button found or could not click: ", e)
            break  
        # Wait for new articles to load
        time.sleep(random.uniform(2, 4))

    return articles_data

# Main function to orchestrate the crawling
def crawl_articles_by_topic(topic: str, TARGET_DATE: str):
    """function to set up the driver, crawl articles, and save them."""
    # URL to scrape
    URL = f"https://www.coindesk.com/{topic}"
    # Set up the WebDriver
    driver = setup_driver()
       
    # Open the URL
    driver.get(URL)

    # Wait for the articles to load initially
    handle_cookie_consent(driver)
    
    # Crawl articles by scrolling and extracting data
    articles_data = extract_articles(driver, TARGET_DATE)

    # Close the driver after crawling
    driver.quit()

    # Check if there were any articles found after the target date
    if not articles_data:
        print(f"No new articles found after {TARGET_DATE}.")
    else:
        # Save the extracted articles to a JSON file
        object_key = f'web_crawler/coindesk/{topic}/coindesk_{topic}_news_after_{TARGET_DATE}.json'
        upload_json_to_minio(json_data=articles_data,object_key=object_key)

def multithreading_crawler(TARGET_DATE: str):
    topics = ['markets', 'business', 'policy', 'tech', 'opinion', 'consensus-magazine', 'learn']
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = [executor.submit(crawl_articles_by_topic, topic, TARGET_DATE) for topic in topics]
        for future in futures:
            try:
                future.result()
            except Exception as e:
                print(f"Error in thread of: {e}")

# Run the crawling process
if __name__ == "__main__":
    TARGET_DATE = "2024-11-20"
    multithreading_crawler(TARGET_DATE)
