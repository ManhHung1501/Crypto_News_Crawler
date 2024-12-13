import time, requests
from bs4 import BeautifulSoup
from requests.exceptions import Timeout
from datetime import datetime
from crawler_utils.minio_utils import upload_json_to_minio, connect_minio
from crawler_utils.common_utils import generate_url_hash,get_last_initial_crawled
from crawler_config.storage_config import CRYPTO_NEWS_BUCKET

# Get total page
def get_total_page(URL):
    for _ in range(3):
        try:
            # Fetch the webpage content
            response = requests.get(URL, timeout=10)
            response.raise_for_status()

            # Parse the HTML content
            soup = BeautifulSoup(response.text, 'html.parser')
            # Find the pagination element with the highest number
            last_page_element = soup.find_all('a', class_='page-numbers')[-2]

            total_pages = int(last_page_element.get_text().strip())
            print('Total pages:', total_pages)
            return total_pages

        except Exception as e:
            print(f"Error while finding total pages: {e}")
    return None
    
# Get content article
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
            
            article_card = soup.find("div", class_="inner-post-entry entry-content")
            if article_card:
                unwanted_cards = ".post-tags, .hot, .penci-post-countview-number-check, .penci-ilrltpost-insert"
                for unwanted in article_card.select(unwanted_cards):
                    unwanted.decompose()
                content = ' '.join(article_card.stripped_strings)
            
        except Exception as e: 
            print(f'Error in get content for {url}: ', e)

  
        if content == "No content":
            print(f"Failed to get content of url: {url}")

        article['content'] = content
    return articles

def full_crawl_articles(category):
    minio_client = connect_minio()
 
    prefix = f'web_crawler/cryptoflies/{category}/cryptoflies_{category}_initial_batch_'
    last_crawled_id, current_batch = get_last_initial_crawled(minio_client=minio_client, bucket=CRYPTO_NEWS_BUCKET,prefix=prefix)
    URL = f"https://blog.cryptoflies.com/category/{category}/"
    print(f"Crawling URL: {URL}")

    not_crawled = last_crawled_id is None
    articles_data = []
    batch_size = 100
    page = 1
    total_page = get_total_page(URL) 

    while page <= total_page:
        print(f'Crawling news on page {page}')
        URL = f"https://blog.cryptoflies.com/category/{category}/page/{page}/"
        
        try:
            # Fetch the HTML content
            response = requests.get(URL, timeout=10)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            # Select the news-feed section and articles
            news_feed = soup.find('ul', class_='penci-wrapper-data penci-grid')
            articles = news_feed.find_all('li', class_='list-post pclist-layout')
            for article in articles:
                try:
                    # Extract article title
                    title_element = article.find('h2', class_='penci-entry-title').find('a')
                    if title_element:
                        title = title_element.text.strip() 
                        # Extract article URL
                        article_url = title_element['href']
                        article_id = generate_url_hash(article_url)

                    # Skip already crawled articles
                    if not not_crawled and article_id == last_crawled_id:
                        not_crawled = True
                        continue
                    if not_crawled:
                        time_element = article.find('time', class_='entry-date published')
                        if time_element:
                            datetime_str = time_element['datetime']
                            published_at = datetime.fromisoformat(datetime_str).strftime('%Y-%m-%d %H:%M:%S')
                        else:
                            published_at ="1970-01-01 00:00:00"
                            print(f'Failed to get publish date for {article_url}')
                        # Add the article data to the list
                        articles_data.append({
                            "id": article_id,
                            "title": title,
                            "url": article_url,
                            "published_at": published_at,
                            "source": "blog.cryptoflies.com"
                        })
                    
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

    if articles_data:
        articles_data = get_detail_article(articles=articles_data)
        object_key = f'{prefix}{current_batch + len(articles_data)}.json'
        upload_json_to_minio(json_data=articles_data,object_key=object_key)

    
# Run the crawling process
if __name__ == "__main__":
    full_crawl_articles('nft-news')