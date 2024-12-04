import time, random, requests
from bs4 import BeautifulSoup
from requests.exceptions import Timeout
from datetime import date, datetime
from selenium.webdriver.common.by import By
from concurrent.futures import ThreadPoolExecutor
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, ElementClickInterceptedException, TimeoutException
from crawler_utils.minio_utils import upload_json_to_minio, connect_minio
from crawler_utils.common_utils import generate_url_hash, get_last_crawled, save_last_crawled, get_last_initial_crawled
from crawler_utils.chrome_driver_utils import setup_driver
from crawler_config.storage_config import CRYPTO_NEWS_BUCKET

# Get publish timestamp
def get_detail_article( articles):
    for article in articles:
        url = article['url']
        published_at = "1970-01-01 00:00:00"
        content = "No content"
        try:
            # Make the HTTP request
            for attempt in range(3):
                try:
                    response = requests.get(url, timeout=15)
                    response.raise_for_status() 
                except Timeout:
                    print(f"Attempt {attempt + 1} timed out. Retrying for {url}...")
                    time.sleep(10)
                except requests.exceptions.RequestException as e:
                    print(f"Request for {url} failed: {e}")
                    continue

            # Parse the HTML with BeautifulSoup
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Find the header container
            date_elements = soup.select_one('div[data-module-name="article-header"]').select_one("div.Noto_Sans_xs_Sans-400-xs").select("span")
            if date_elements:
                for span in date_elements:
                    date_text = span.get_text(strip=True).replace(" p.m.", " PM").replace(" a.m.", " AM")
                    if "Published" in date_text:
                        # Process "Published" date
                        cleaned_date_text = date_text.replace("Published", "").strip()
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

def full_crawl_articles():
    
    driver = uc.Chrome()
    batch_size = 1000
    minio_client = connect_minio()
 
    prefix = f'web_crawler/bitcoinmagazine/bitcoinmagazine_initial_batch_'
    last_crawled_id, current_batch = get_last_initial_crawled(minio_client=minio_client, bucket=CRYPTO_NEWS_BUCKET,prefix=prefix)
    URL = f"https://bitcoinmagazine.com/articles"
    print(f"Crawling URL: {URL}")

    # Open the URL
    driver.get(URL)
    wait_for_page_load(driver, "div.m-page")
    # Wait for the articles to load initially
    not_crawled = last_crawled_id is None
    article_num = 0
    articles_data = []
    page_size = 10
    retries = 3
    retries_count =1
    previous_news = 0 
    while True:
        # Get all the articles on the current page
        driver.save_screenshot("screenshot.png")
        html = driver.page_source
        print(html)
        data_div = driver.find_element(By.CSS_SELECTOR, 'phoenix-hub.m-card-group--container').find_elements(By.CSS_SELECTOR, "section.m-card-group")
        current_news = len(data_div)
        articles = data_div[article_num: article_num+ page_size]
        print(f"Crawling news from {previous_news} to {current_news} news")
        for article in articles:
            try:
                # Extract title
                title_element = article.find_element(By.CSS_SELECTOR, "a.text-color-charcoal-900")
                article_url = title_element.get_attribute("href")
                article_id = generate_url_hash(article_url)
                # Skip if the article URL has already been processed
                if not not_crawled and article_id == last_crawled_id:
                    not_crawled = True
                    continue
                if not_crawled:
                    title = title_element.text
        
                    # Add the article data to the list
                    articles_data.append({
                        "id": article_id,
                        "title": title,
                        "url": article_url,
                        "source": "coindesk.com"
                    })

                # if len(articles_data) == batch_size:
                #     articles_data = get_detail_article(articles=articles_data)
                #     new_batch = current_batch + batch_size
                #     object_key = f'{prefix}{new_batch}.json'
                #     upload_json_to_minio(json_data=articles_data,object_key=object_key)
                    
                #     current_batch = new_batch
                #     articles_data = []
            except Exception as e:
                print(f"Error extracting data for an article: {e}")
            
        
        # Click the "More stories" button to load more articles
        try:
            more_button = driver.find_element(By.CSS_SELECTOR, "button.bg-white.hover\\:opacity-80.cursor-pointer")
            ActionChains(driver).move_to_element(more_button).click().perform()
            if  current_news == previous_news:
                time.sleep(3)
            else:
                previous_news = current_news
                article_num += page_size
                retries_count = 0
        except NoSuchElementException as e:
            print(f"No 'More stories' button found or could not click on {retries_count}/{retries}")
            retries_count +=1
            # if retries_count > retries:
            #     articles_data = get_detail_article(articles=articles_data)
            #     object_key = f'{prefix}{current_news}.json'
            #     upload_json_to_minio(json_data=articles_data,object_key=object_key)
            #     driver.quit()
            #     break
        except Exception as e:
            print("Error in click more: ", e)
            driver.quit()
            break
                
        # Wait for new articles to load
        time.sleep(random.uniform(2, 4))
    driver.quit()




