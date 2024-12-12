import time, random, requests
from requests.exceptions import Timeout
from datetime import datetime, date
from selenium.webdriver.common.by import By
from crawler_utils.minio_utils import upload_json_to_minio, connect_minio
from crawler_utils.common_utils import generate_url_hash, get_last_crawled,save_last_crawled, get_last_initial_crawled
from crawler_utils.chrome_driver_utils import setup_driver, wait_for_page_load
from crawler_config.storage_config import CRYPTO_NEWS_BUCKET
from bs4 import BeautifulSoup

# Get total page
def get_total_page():
    try:
        # Fetch the webpage content
        URL = "https://blockonomi.com/all"
        response = requests.get(URL, timeout=10)
        response.raise_for_status()

        # Parse the HTML content
        soup = BeautifulSoup(response.text, 'html.parser')
        # Find the pagination element with the highest number
        page_links = soup.find_all('a', class_='page-numbers')
        if not page_links:
            return None


        total_pages = int(page_links[-2].text)
        print('Total pages:', total_pages)
        return total_pages

    except Exception as e:
        print(f"Error while finding total pages: {e}")
        return None

# Function to extract articles from the page
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
            
            article_card = soup.find("div", class_="post-content")
            if article_card:
                unwanted_cards = ".adver"
                for unwanted in article_card.select(unwanted_cards):
                    unwanted.decompose()
                content = ' '.join(article_card.stripped_strings)
            
                
            if content == "No content":
                print(f"Failed to get content of url: {url}")
        except Exception as e: 
            print(f'Error in get content for {url}: ', e)
            
        article['content'] = content
    return articles


def full_crawl_articles():
    minio_client = connect_minio()
    
    prefix = f'web_crawler/blockonomi/blockonomi_initial_batch_'
    last_crawled_id, current_batch = get_last_initial_crawled(
        minio_client=minio_client, 
        bucket=CRYPTO_NEWS_BUCKET,
        prefix=prefix
    )
    
    batch_size = 100
    not_crawled = last_crawled_id is None
    articles_data = []
    crawled_id = set()
    page = 1
    total_page = get_total_page() 

    while page <= total_page:
        print(f'Crawling news on page {page}')
        URL = f"https://blockonomi.com/all/page/{page}/"
        
        try:
            # Fetch the HTML content
            response = requests.get(URL, timeout=10)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')

            # Select the news-feed section and articles
            news_feed = soup.select_one('section.block-wrap')
            articles = news_feed.select("article.l-post div.content") if news_feed else []

            for article in articles:
                try:
                    # Extract article title
                    title_element = article.find('h2', class_='is-title')
                    title = title_element.text.strip()

                    # Extract article URL
                    article_url = title_element.select_one('a')['href']
                    article_id = generate_url_hash(article_url)

                    if article_id in crawled_id:
                        continue

                    # Skip already crawled articles
                    if not not_crawled and article_id == last_crawled_id:
                        not_crawled = True
                        continue
        
                    if not_crawled:      
                        # Add the article data to the list
                        articles_data.append({
                            "id": article_id,
                            "title": title,
                            "published_at": datetime.fromisoformat(article.find('time', class_='post-date')['datetime'].strip()).strftime("%Y-%m-%d %H:%M:%S"),
                            "url": article_url,
                            "source": "blockonomi.com"
                        })
                        crawled_id.add(article_id)

                    # Process and upload the batch
                    if len(articles_data) == batch_size:
                        articles_data = get_detail_article(articles=articles_data)  # Adjust to parse publish dates
                        new_batch = current_batch + batch_size
                        object_key = f'{prefix}{new_batch}.json'
                        upload_json_to_minio(json_data=articles_data, object_key=object_key)
                        
                        current_batch = new_batch
                        articles_data = []
                except Exception as e:
                    print(f"Error extracting data for {article_url}: {e}")
            print(f"Total News Crawled After page {page} is {len(articles_data)}")
            page += 1 
        except requests.RequestException as e:
            print(f"Error fetching page {page}: {e}")
            time.sleep(10)

    # Final upload if there are remaining articles
    if articles_data:
        articles_data = get_detail_article(articles=articles_data)
        object_key = f'{prefix}{current_batch + len(articles_data)}.json'
        upload_json_to_minio(json_data=articles_data, object_key=object_key)
        
        print(f"Uploaded final batch: {object_key}")

# Run the crawling process
if __name__ == "__main__":
    full_crawl_articles()
