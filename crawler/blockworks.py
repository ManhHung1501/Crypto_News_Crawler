import requests, json
from datetime import datetime
from crawler_utils.minio_utils import upload_json_to_minio, connect_minio
from crawler_utils.common_utils import generate_url_hash, get_last_crawled
from crawler_utils.chrome_driver_utils import setup_driver


def get_blockworks_cookies():
    # Initialize the WebDriver
    driver = setup_driver()
    
    try:
        # Open the Blockworks news page
        driver.get("https://blockworks.co/news")
        
        # Wait for the page to load (you can adjust the time if needed)
        driver.implicitly_wait(10)
        
        # Retrieve cookies from the browser
        cookies = driver.get_cookies()
        cookie_dict = {cookie['name']: cookie['value'] for cookie in cookies}
        
        return cookie_dict
    finally:
        driver.quit()

def full_crawl_articles():
    url = "https://eh58zikx8p-dsn.algolia.net/1/indexes/wp_posts_post/query?x-algolia-agent=Algolia for JavaScript (4.24.0); Browser (lite)&x-algolia-api-key=Nzk0ODE5NzhjOTM4ODQ5YjczZTg0NWU4ZDcyNjliMjllNzFhM2QzYmI0NDdhMzA4ZTZlMDJkOWZkYmYwODhlZXZhbGlkVW50aWw9MTczNDM4MDYxMA==&x-algolia-application-id=EH58ZIKX8P&paginationLimitedTo=20000"

    payload = json.dumps({
    "attributesToRetrieve": [
        "post_title",
        "post_date",
        "permalink",
        "content"
    ],
    "hitsPerPage": 1000,
    "page": 0
    })
    headers = {
    'Content-Type': 'application/json',
    'Origin': 'https://blockworks.co'
    }

    response = requests.request("POST", url, headers=headers, data=payload)
    articles =response.json()['hits']
    articles_data = []
    for article in articles:
        articles_data.append({
            "id": generate_url_hash(article['permalink']),
            "title": article['post_title'],
            "url": article['permalink'],
            "published_at": datetime.fromtimestamp(article['post_date']).strftime('%Y-%m-%d %H:%M:%S'),
            "content": article['content'],
            "source": "blockworks.co"
        })
    upload_json_to_minio(json_data=articles_data,object_key=f'web_crawler/blockworks/blockworks_initial_batch_1000.json')
  
def incremental_crawl_articles():
    url = "https://eh58zikx8p-dsn.algolia.net/1/indexes/wp_posts_post/query?x-algolia-agent=Algolia for JavaScript (4.24.0); Browser (lite)&x-algolia-application-id=EH58ZIKX8P&paginationLimitedTo=20000"

    payload = json.dumps({
    "attributesToRetrieve": [
        "post_title",
        "post_date",
        "permalink",
        "content"
    ],
    "hitsPerPage": 1000,
    "page": 0
    })
    headers = {
    'Content-Type': 'application/json',
    'Origin': 'https://blockworks.co'
    }
    cookies = get_blockworks_cookies()

    response = requests.post(url, headers=headers, data=payload, cookies=cookies)
    if response.status_code == 200:
        articles =response.json()['hits']
        articles_data = []
        for article in articles:
            articles_data.append({
                "id": generate_url_hash(article['permalink']),
                "title": article['post_title'],
                "url": article['permalink'],
                "published_at": datetime.fromtimestamp(article['post_date']).strftime('%Y-%m-%d %H:%M:%S'),
                "content": article['content'],
                "source": "blockworks.co"
            })
        print(f"{len(articles_data)}")
    else:
        print(f"Request Failed with status code {response.status_code}: {response.text}")
    # upload_json_to_minio(json_data=articles_data,object_key=f'web_crawler/blockworks/blockworks_initial_batch_1000.json')
  

