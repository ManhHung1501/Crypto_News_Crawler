import time, random, requests, re
from bs4 import BeautifulSoup
from requests.exceptions import Timeout
from datetime import datetime
from dateutil.relativedelta import relativedelta
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, ElementClickInterceptedException, TimeoutException
from crawler_utils.minio_utils import upload_json_to_minio, connect_minio
from crawler_utils.common_utils import generate_url_hash,get_last_initial_crawled, project_dir
from crawler_utils.chrome_driver_utils import setup_driver, wait_for_page_load
from crawler_config.storage_config import CRYPTO_NEWS_BUCKET

url = 'https://decrypt.co/295852/coinbase-robinhood-users-see-red-as-dogwifhat-plunges-1-2-billion-in-three-days'
published_at = "1970-01-01 00:00:00"
content = "No content"
title = "No Title"
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
    soup = BeautifulSoup(response.content, 'html.parser').find('main')

    title_element = soup.find('header')
    if title_element:
        title = title_element.find("h1").get_text(strip=True)

        datetime_value = title_element.find('time')['datetime']
        published_at = datetime.strptime(datetime_value, "%Y-%m-%dT%H:%M:%SZ").strftime("%Y:%m:%d %H:%M:%S")
    

    # content                
    data_tag = soup.find('div', {'class': 'post-content'})
    if data_tag:
        for unwanted in data_tag.select(".hidden, .relative, .border-decryptGridline"):
            unwanted.decompose()
    content = ' '.join(data_tag.stripped_strings)
    
except Exception as e:
    print(f"Error get publish date for URL {url}: {e}")

if published_at == "1970-01-01 00:00:00":
    print(f'Failed to get publish date for {url}')
if content == "No content":
    print(f'Failed to get content for {url}')
if title == "No Title":
    print(f'Failed to get Title for {url}')
print(published_at)
print(title)
print('----------------------')
print(content)