import requests, json
from datetime import datetime
from crawler_utils.minio_utils import upload_json_to_minio, connect_minio
from crawler_utils.common_utils import generate_url_hash


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
  
# Run the crawling process
if __name__ == "__main__":
    full_crawl_articles()

