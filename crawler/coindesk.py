import time, random, requests
from bs4 import BeautifulSoup
from requests.exceptions import Timeout
from datetime import date, datetime
from selenium.webdriver.common.by import By
from concurrent.futures import ThreadPoolExecutor
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (NoSuchElementException, 
            ElementClickInterceptedException, TimeoutException, 
            StaleElementReferenceException)
from crawler_utils.minio_utils import upload_json_to_minio, connect_minio
from crawler_utils.common_utils import generate_url_hash, get_last_crawled, get_last_initial_crawled
from crawler_utils.chrome_driver_utils import setup_driver
from crawler_config.storage_config import CRYPTO_NEWS_BUCKET
from crawler_constants.crawl_constants import Coindesk

topics = Coindesk.topics


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

# Get publish timestamp
def get_detail_article( articles):
    for article in articles:
        url = article['url']
        published_at = "1970-01-01 00:00:00"
        content = "No content"
        try:
            # Make the HTTP request
            for _ in range(3):
                try:
                    response = requests.get(url, timeout=10)
                    response.raise_for_status() 
                    break
                except Timeout:
                    print(f"timed out for {url}, retrying...")
                    time.sleep(5)
                except requests.exceptions.RequestException as e:
                    print(f"Request for {url} failed, retrying... : {e}")
                    time.sleep(5)

            # Parse the HTML with BeautifulSoup
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Find the header container
            date_elements = soup.select_one('div[data-module-name="article-header"]').select_one("div.Noto_Sans_xs_Sans-400-xs").select("span")
            if date_elements:
                for span in date_elements:
                    date_text = span.get_text(strip=True).replace(" p.m.", " PM").replace(" a.m.", " AM").replace("UTC", "")
                    if "Published" in date_text:
                        # Process "Published" date
                        cleaned_date_text = date_text.replace("Published", "").replace("UTC", "").strip()
                        try:
                            parsed_date = datetime.strptime(cleaned_date_text, "%b %d, %Y, %I:%M %p")
                            published_at = parsed_date.strftime("%Y-%m-%d %H:%M:%S")
                            break  # Stop after finding "Published"
                        except ValueError:
                            print(f"Unable to parse Published date of {url}: {cleaned_date_text}")
                    elif "Updated" not in date_text:
                        try:
                            parsed_date = datetime.strptime(date_text, "%b %d, %Y, %I:%M %p")
                            published_at = parsed_date.strftime("%Y-%m-%d %H:%M:%S")
                        except ValueError:
                            print(f"Unable to parse fallback date of {url}: {date_text}")

            # content                
            article_header_div = soup.find('div', {'data-module-name': 'article-body'})
            if article_header_div:
                for unwanted in article_header_div.select(".border, .playlistThumb, .article-ad"):
                    unwanted.decompose()
            content = ' '.join(article_header_div.stripped_strings)
            
        except Exception as e:
            print(f"Error get publish date for URL {url}: {e}")
        
        if published_at == "1970-01-01 00:00:00":
            print(f'Failed to get publish date for {url}')
        if content == "No content":
            print(f'Failed to get content for {url}')

        article['content']  = content 
        article['published_at']  = published_at
    return articles

def incremental_crawl_articles(topic, max_news:int = 500):
    driver = setup_driver()
    minio_client = connect_minio()

    
    prefix = f'web_crawler/coindesk/{topic}/coindesk_{topic}_initial_batch_'
    STATE_FILE = f'web_crawler/coindesk/{topic}/coindesk_{topic}_incremental_crawled_at_'
    last_crawled = get_last_crawled(STATE_FILE=STATE_FILE, minio_client=minio_client, bucket=CRYPTO_NEWS_BUCKET, prefix=prefix)
    URL = f"https://www.coindesk.com/{topic}"
    print(f"Crawling URL: {URL}")

    # Open the URL
    driver.get(URL)

    # Wait for the articles to load initially
    handle_cookie_consent(driver)

    articles_data = []
    crawled_id = set()
    previous_news = 0
    complete = False
    while not complete:
        try:
            # Dynamically re-locate articles to avoid stale element issues
            data_div = driver.find_element(
                By.CSS_SELECTOR, 'div[data-module-name="timeline-module"]'
            ).find_elements(By.CSS_SELECTOR, "div.flex.gap-4")
            current_news = len(data_div)
            if current_news == previous_news:
                time.sleep(3)
            articles = data_div[previous_news : current_news]
            print(
                f"Crawling news from {previous_news} to {current_news} news of {topic}"
            )

            for article in articles:
                try:
                    # Extract the article data
                    title_element = article.find_element(
                        By.CSS_SELECTOR, "a.text-color-charcoal-900"
                    )
                    article_url = title_element.get_attribute("href")
                    article_id = generate_url_hash(article_url)

                    if article_id in crawled_id:
                        continue

                    if article_id in last_crawled or len(articles_data) == max_news:
                        articles_data = get_detail_article(
                            articles=articles_data
                        )
                        object_key = f'web_crawler/coindesk/{topic}/coindesk_{topic}_incremental_crawled_at_{int(datetime.now().timestamp())}.json'
                        upload_json_to_minio(
                            json_data=articles_data, object_key=object_key
                        )
                        complete = True
                        break

                    title = title_element.text
                    # Add the article data to the list
                    articles_data.append(
                        {
                            "id": article_id,
                            "title": title,
                            "url": article_url,
                            "source": "coindesk.com",
                        }
                    )
                    crawled_id.add(article_id)

                except Exception as e:
                    print(f"Error extracting data for an article: {e}")
                    

            # Click the "More stories" button to load more articles
            try:
                more_button = driver.find_element(
                    By.CSS_SELECTOR,
                    "button.bg-white.hover\\:opacity-80.cursor-pointer",
                )
                ActionChains(driver).move_to_element(more_button).click().perform()
                # print("Clicked 'More stories' button successfully.")

                previous_news = current_news
            except NoSuchElementException:
                print(
                    f"No 'More stories' button found"
                )
                break

            except Exception as e:
                print(f"Error clicking 'More stories' button: {e}")
                break

            # Wait for new articles to load
            time.sleep(random.uniform(2, 4))

        except Exception as e:
            print(f"Error during crawling loop: {e}")
            break

    # Ensure the driver quits properly
    driver.quit()
    print("Crawling completed.")

def full_crawl_articles(topic):
    driver = setup_driver()
    batch_size = 100
    minio_client = connect_minio()

    prefix = f'web_crawler/coindesk/{topic}/coindesk_{topic}_initial_batch_'
    last_crawled_id, current_batch = get_last_initial_crawled(
        minio_client=minio_client, bucket=CRYPTO_NEWS_BUCKET, prefix=prefix
    )
    URL = f"https://www.coindesk.com/{topic}"
    print(f"Crawling URL: {URL}")

    # Open the URL
    driver.get(URL)

    # Wait for the articles to load initially
    handle_cookie_consent(driver)

    not_crawled = last_crawled_id is None
    articles_data = []
    crawled_id = set()
    previous_news = 0
    while True:
        try:
            # Dynamically re-locate articles to avoid stale element issues
            data_div = driver.find_element(
                By.CSS_SELECTOR, 'div[data-module-name="timeline-module"]'
            ).find_elements(By.CSS_SELECTOR, "div.flex.gap-4")
            current_news = len(data_div)
            if current_news == previous_news:
                time.sleep(3)
            articles = data_div[previous_news : current_news]
            print(
                f"Crawling news from {previous_news} to {current_news} news of {topic}"
            )

            for article in articles:
                retries_left = 3  # Retry mechanism for stale elements
                while retries_left > 0:
                    try:
                        # Extract the article data
                        title_element = article.find_element(
                            By.CSS_SELECTOR, "a.text-color-charcoal-900"
                        )
                        article_url = title_element.get_attribute("href")
                        article_id = generate_url_hash(article_url)

                        if article_id in crawled_id:
                            continue

                        # Skip if the article URL has already been processed
                        if not not_crawled and article_id == last_crawled_id:
                            not_crawled = True
                            continue

                        if not_crawled:
                            title = title_element.text
                            # Add the article data to the list
                            articles_data.append(
                                {
                                    "id": article_id,
                                    "title": title,
                                    "url": article_url,
                                    "source": "coindesk.com",
                                }
                            )
                            crawled_id.add(article_id)
                        # Upload in batches
                        if len(articles_data) == batch_size:
                            articles_data = get_detail_article(
                                articles=articles_data
                            )
                            new_batch = current_batch + batch_size
                            object_key = f"{prefix}{new_batch}.json"
                            upload_json_to_minio(
                                json_data=articles_data, object_key=object_key
                            )
                            current_batch = new_batch
                            articles_data = []
                            crawled_id = set()
                        break  # Exit retry loop if successful
                    except StaleElementReferenceException:
                        retries_left -= 1
                        print(
                            f"Stale element encountered. Retrying {3 - retries_left}/3."
                        )
                        time.sleep(random.uniform(1, 2))  # Short wait before retrying
                        # Refresh article reference
                        data_div = driver.find_element(
                            By.CSS_SELECTOR, 'div[data-module-name="timeline-module"]'
                        ).find_elements(By.CSS_SELECTOR, "div.flex.gap-4")
                        articles = data_div[previous_news : current_news]
                    except Exception as e:
                        print(f"Error extracting data for an article: {e}")
                        break

            # Click the "More stories" button to load more articles
            try:
                more_button = driver.find_element(
                    By.CSS_SELECTOR,
                    "button.bg-white.hover\\:opacity-80.cursor-pointer",
                )
                ActionChains(driver).move_to_element(more_button).click().perform()
                # print("Clicked 'More stories' button successfully.")

                previous_news = current_news
            except NoSuchElementException:
                print(
                    f"No 'More stories' button found"
                )
                break

            except Exception as e:
                print(f"Error clicking 'More stories' button: {e}")
                break

            # Wait for new articles to load
            time.sleep(random.uniform(2, 4))

        except Exception as e:
            print(f"Error during crawling loop: {e}")
            break

    # Upload remaining data
    if articles_data:
        articles_data = get_detail_article(articles=articles_data)
        object_key = f"{prefix}{current_batch + len(articles_data)}.json"
        upload_json_to_minio(json_data=articles_data, object_key=object_key)

    # Ensure the driver quits properly
    driver.quit()
    print("Crawling completed.")

