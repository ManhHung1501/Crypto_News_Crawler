import time, requests,pytz
from bs4 import BeautifulSoup
from requests.exceptions import Timeout
from crawler_utils.minio_utils import upload_json_to_minio, connect_minio
from crawler_utils.common_utils import generate_url_hash,get_last_initial_crawled
from crawler_config.storage_config import CRYPTO_NEWS_BUCKET

# Get total page
def get_total_page():
    try:
        # Fetch the webpage content
        URL = "https://holder.io/"
        response = requests.get(URL, timeout=10)
        response.raise_for_status()

        # Parse the HTML content
        soup = BeautifulSoup(response.text, 'html.parser')
        # Find the pagination element with the highest number
        last_page_element = soup.select('a.hlpagination__numbers')[-2]
        if not last_page_element:
            return None


        total_pages = int(last_page_element.get_text())
        print('Total pages:', total_pages)
        return total_pages

    except Exception as e:
        print(f"Error while finding total pages: {e}")
        return None

def get_detail_article(articles):
    for article in articles:
        content = "No content"
        url = article['url']
        published_at = "1970-01-01 00:00:00"
        try:
            for _ in range(3):
                # Make the HTTP request
                try:
                    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0"}
                    response = requests.get(url, timeout=15, headers=headers)
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
            
            date_tag = soup.find("meta", itemprop="datePublished")
            if date_tag:
                published_at = datetime.fromisoformat(date_tag['content']).astimezone(pytz.utc).strftime("%Y-%m-%d %H:%M:%S")

            article_card = soup.find("article")
            if article_card:
                unwanted_cards = ".postinfo, figure, blockquote"
                for unwanted in article_card.select(unwanted_cards):
                    unwanted.decompose()
                content = ' '.join(article_card.stripped_strings)
            
        except Exception as e: 
            print(f'Error in get content for {url}: ', e)
        if content == "No content":
            print(f"Failed to get content of url: {url}")
        if published_at == "1970-01-01 00:00:00":
            print(f'Failed to get Publish date for {url}')

        article['published_at'] = published_at   
        article['content'] = content
    return articles

def full_crawl_articles():
    batch_size = 1000
    minio_client = connect_minio()
    
    prefix = f'web_crawler/holder.io/holder.io_initial_batch_'
    last_crawled_id, current_batch = get_last_initial_crawled(
        minio_client=minio_client, 
        bucket=CRYPTO_NEWS_BUCKET,
        prefix=prefix
    )
    
    not_crawled = last_crawled_id is None
    articles_data = []
    page = 1
    total_page = get_total_page() 

    while page <= total_page:
        print(f'Crawling news on page {page}')
        URL = f"https://holder.io//page/{page}/"
        
        try:
            # Fetch the HTML content
            response = requests.get(URL, timeout=10)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')

            # Select the news-feed section and articles
            news_feed = soup.select_one('div.post-cards')
            articles = news_feed.select("section.post-card") if news_feed else []

            for article in articles:
                try:
                    # Extract article title
                    title_element = article.select_one("h2.post-card__title a")

                    # Extract article URL
                    article_url = title_element['href']
                    article_id = generate_url_hash(article_url)

                    # Skip already crawled articles
                    if not not_crawled and article_id == last_crawled_id:
                        not_crawled = True
                        continue
                    if not_crawled:                 
                        # Add the article data to the list
                        articles_data.append({
                            "id": article_id,
                            "title": title_element.text.strip(),
                            "url": article_url,
                            "source": "holder.io"
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
    
    if articles_data:
        articles_data = get_detail_article(articles=articles_data)
        object_key = f'{prefix}{current_batch + len(articles_data)}.json'
        upload_json_to_minio(json_data=articles_data,object_key=object_key)

    
# Run the crawling process
if __name__ == "__main__":
    full_crawl_articles()
    
