import time, random, requests
from bs4 import BeautifulSoup
from requests.exceptions import Timeout
from datetime import datetime
from selenium.webdriver.common.by import By
from crawler_utils.minio_utils import upload_json_to_minio, connect_minio
from crawler_utils.common_utils import generate_url_hash,get_last_initial_crawled
from crawler_utils.chrome_driver_utils import setup_driver, wait_for_page_load
from crawler_config.storage_config import CRYPTO_NEWS_BUCKET



def get_detail_article(articles):
    for article in articles:
        content = "No content"
        published_at = "1970-01-01 00:00:00"
        url = article['url']
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
                    time.sleep(10)
                
            soup = BeautifulSoup(response.content, "html.parser")
            date_tag = soup.find("span", class_="MuiBox-root css-k008qs")
            if date_tag:
                raw_date = date_tag.get_text(strip=True).replace("·", "").strip()
                published_at = datetime.strptime(raw_date,"%m/%d/%y").strftime("%Y-%m-%d %H:%M:%S")
                
            article_card = soup.find("div", class_="MuiBox-root css-xdym45")
            if not article_card:
                article_card = soup.find("div", class_="MuiGrid-root MuiGrid-container css-1d3bbye")
            
            unwanted_tags = ".permalink-heading, .embed-wrapper"
            for unwanted_tag in article_card.select(unwanted_tags):
                unwanted_tag.decompose()
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

def full_crawl_articles(category):
    driver = setup_driver()
    
    minio_client = connect_minio()
 
    prefix = f'web_crawler/nonfungible/{category}/nonfungible_{category}_initial_batch_'
    last_crawled_id, current_batch = get_last_initial_crawled(minio_client=minio_client, bucket=CRYPTO_NEWS_BUCKET,prefix=prefix)
    URL = f"https://nonfungible.com/news/{category}"
    print(f"Crawling URL: {URL}")

    # Open the URL
    driver.get(URL)
    # Wait for the articles to load initially
    wait_for_page_load(driver, 'section')
    
    not_crawled = last_crawled_id is None
    articles_data = []
    batch_size = 100
    crawled_id = set()
    previous_news =0
    while True:
        soup = BeautifulSoup(driver.page_source, "html.parser")
        # Find all the articles within the container
        articles = soup.find_all("a", class_="MuiTypography-root MuiTypography-inherit MuiLink-root MuiLink-underlineHover css-3pyhqk")
        current_news = len(articles)
        if current_news == previous_news:
            if count == 3:
                break
            count += 1
            time.sleep(3)
        else:
            count = 0
        for article in articles:
            try:
                # Extract title
                article_url = article_url = article.get("href", "")
                if article_url in ('/about',""):
                    continue
                article_id = generate_url_hash(article_url)
                # Skip if the article URL has already been processed
                if article_id in crawled_id:
                    continue
                if not not_crawled and article_id == last_crawled_id:
                    not_crawled = True
                    continue
                if not_crawled:
                    title_element = article.select_one("p")
    
                    if title_element: 
                        title_text = title_element.get_text(strip=True) 
                    else:
                        continue
                    # Add the article data to the list
                    articles_data.append({
                        "id": article_id,
                        "title": title_text,
                        "url": f"https://nonfungible.com{article_url}",
                        "source": "nonfungible.com"
                    })
                    crawled_id.add(article_id)
           
                if len(articles_data) == batch_size:
                    articles_data = get_detail_article(articles=articles_data)
                    new_batch = current_batch + batch_size
                    object_key = f'{prefix}{new_batch}.json'
                    upload_json_to_minio(json_data=articles_data,object_key=object_key)
                    current_batch = new_batch
                    articles_data = []
            except Exception as e:
                print(f"Error extracting data for an article: {e}")
                break
    
        try:
            last = driver.find_element(By.CSS_SELECTOR, "div.MuiBox-root.css-1gktaos")
            driver.execute_script("arguments[0].scrollIntoView();",last)
       
            previous_news = current_news 
            retries_count = 0
        except Exception:
            print(f"Get Error in load more news retries {retries_count}/{3}")
            retries_count+=1
            if retries_count > 3:
                break
          
        time.sleep(random.uniform(2, 4))

    if articles_data:
        articles_data = get_detail_article(articles=articles_data)
        object_key = f'{prefix}{current_batch + len(articles_data)}.json'
        upload_json_to_minio(json_data=articles_data,object_key=object_key)
    driver.quit()
    
# Run the crawling process
if __name__ == "__main__":
    full_crawl_articles('corporate')