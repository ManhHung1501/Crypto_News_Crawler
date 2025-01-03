import time, requests
from requests.exceptions import Timeout
from datetime import datetime, date
from crawler_utils.minio_utils import upload_json_to_minio, connect_minio
from crawler_utils.common_utils import generate_url_hash, get_last_crawled, get_last_initial_crawled
from crawler_config.storage_config import CRYPTO_NEWS_BUCKET
from bs4 import BeautifulSoup

# Get total page
def get_total_page():
    try:
        # Fetch the webpage content
        URL = "https://cryptoslate.com/news"
        response = requests.get(URL, timeout=10)
        response.raise_for_status()

        # Parse the HTML content
        soup = BeautifulSoup(response.text, 'html.parser')
        # Find the pagination element with the highest number
        last_page_element = soup.select('div.pagination a.page-numbers')[-2]
        if not last_page_element:
            return None


        total_pages = int(last_page_element.get_text().replace(',', ''))
        print('Total pages:', total_pages)
        return total_pages

    except Exception as e:
        print(f"Error while finding total pages: {e}")
        return None

def get_detail_article(articles):
    for article in articles:
        published_at = "1970-01-01 00:00:00"
        content = "No content"
        url = article['url']
        try:
            # Make the HTTP request
            try:
                response = requests.get(url, timeout=15)
                response.raise_for_status() 
            except Timeout:
                print(f"timed out. Sleep for {url}...")
                time.sleep(10)
            except requests.exceptions.RequestException as e:
                print(f"Request for {url} failed: {e}")
                continue
                
            soup = BeautifulSoup(response.content, "html.parser")
            post_header_div = soup.select_one(".post-header.article")
            author_element = soup.select_one(".author-info")
            if author_element:
                post_date_element = author_element.select_one(".post-date")
                time_element = author_element.select_one(".time")
                raw_date = post_date_element.get_text(strip=True).replace(time_element.get_text(), "") + " " + time_element.get_text(strip=True).replace("at ", "")
                published_at = datetime.strptime(raw_date, "%b. %d, %Y %I:%M %p UTC").strftime("%Y-%m-%d %H:%M:%S")
            else:
                try:
                    published_element = post_header_div.select_one(".post-meta-single.sponsored .text span")
                    raw_date = published_element.get_text(strip=True).replace("Published", "").strip()
                    published_at = datetime.strptime(raw_date, "%b. %d, %Y at %I:%M %p UTC").strftime("%Y-%m-%d %H:%M:%S")
                except Exception as e:
                    print('Error in get publish date: ', e)
                    pass
        except Exception as e: 
            print('Error in get publish date: ', e)
            
        if published_at == "1970-01-01 00:00:00":
            print(f'Failed to get publish date for {url}')

        article['published_at'] = published_at

        article_card = soup.find("article", class_="full-article")
        if article_card:
            unwanted_cards = ".disclaimer, .posted-in, .post-meta-flex, .podcast-box, .unit-widgets, .link-page, .related-articles, .footer-disclaimer"
            for unwanted in article_card.select(unwanted_cards):
                unwanted.decompose()
            content = ' '.join(article_card.stripped_strings)
        else:
            content = ' '.join([p.get_text(strip=True) for p in soup.select('article > p')])
            
        if content == "No content":
            print(f"Failed to get content of url: {url}")
            
        article['content'] = content
    return articles

def incremental_crawl_articles():
    minio_client = connect_minio()
    
    prefix = f'web_crawler/cryptoslate/cryptoslate_initial_batch_'
    STATE_FILE = f'web_crawler/cryptoslate/cryptoslate_incremental_crawled_at_'
    last_crawled = get_last_crawled(STATE_FILE=STATE_FILE, minio_client=minio_client, bucket=CRYPTO_NEWS_BUCKET, prefix=prefix)

    articles_data = []
    page = 1
    complete = False
    while not complete:
        print(f'Crawling news on page {page}')
        URL = f"https://cryptoslate.com/news/page/{page}/"
        
        try:
            # Fetch the HTML content
            response = requests.get(URL, timeout=10)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')

            # Select the news-feed section and articles
            news_feed = soup.select_one('section.news-feed')
            articles = news_feed.select("div.list-post") if news_feed else []

            for article in articles:
                try:
                    # Extract article title
                    title_element = article.select_one("div.title h2")
                    title = title_element.text.strip() if title_element else None
                    
                    # Extract article URL
                    link_element = article.select_one("a")
                    article_url = link_element['href'] if link_element else None
                    article_id = generate_url_hash(article_url)

                    if article_id in last_crawled:
                        articles_data = get_detail_article(articles=articles_data)
                        object_key = f'web_crawler/cryptoslate/cryptoslate_incremental_crawled_at_{date.today()}.json'
                        upload_json_to_minio(json_data=articles_data, object_key=object_key)
                        complete = True
                        break
                    
                    # Add the article data to the list
                    articles_data.append({
                        "id": article_id,
                        "title": title,
                        "url": article_url,
                        "source": "cryptoslate.com"
                    })
                    
                except Exception as e:
                    print(f"Error extracting data for {article_url}: {e}")
            print(f"Total News Crawled After page {page} is {len(articles_data)}")
            page += 1 
        except requests.RequestException as e:
            print(f"Error fetching page {page}: {e}")

    print("Crawling completed.")

def full_crawl_articles():
    batch_size = 1000
    minio_client = connect_minio()
    
    prefix = f'web_crawler/cryptoslate/cryptoslate_initial_batch_'
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
        URL = f"https://cryptoslate.com/news/page/{page}/"
        
        try:
            # Fetch the HTML content
            response = requests.get(URL, timeout=10)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')

            # Select the news-feed section and articles
            news_feed = soup.select_one('section.news-feed')
            articles = news_feed.select("div.list-post") if news_feed else []

            for article in articles:
                try:
                    # Extract article title
                    title_element = article.select_one("div.title h2")
                    title = title_element.text.strip() if title_element else None

                    # Extract article URL
                    link_element = article.select_one("a")
                    article_url = link_element['href'] if link_element else None
                    article_id = generate_url_hash(article_url)

                    # Skip already crawled articles
                    if not not_crawled and article_id == last_crawled_id:
                        not_crawled = True
                        continue
                    if not_crawled:                 
                        # Add the article data to the list
                        articles_data.append({
                            "id": article_id,
                            "title": title,
                            "url": article_url,
                            "source": "cryptoslate.com"
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

    # Final upload if there are remaining articles
    if articles_data:
        articles_data = get_detail_article(articles=articles_data)
        new_batch = current_batch + batch_size
        object_key = f'{prefix}{new_batch}.json'
        upload_json_to_minio(json_data=articles_data, object_key=object_key)
        print(f"Uploaded final batch: {object_key}")


