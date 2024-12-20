import time, random, requests, re, pytz
from bs4 import BeautifulSoup
from requests.exceptions import Timeout
from datetime import datetime, timezone
from dateutil.relativedelta import relativedelta
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (NoSuchElementException, 
            ElementClickInterceptedException, TimeoutException, 
            StaleElementReferenceException)
from crawler_utils.minio_utils import upload_json_to_minio, connect_minio
from crawler_utils.common_utils import generate_url_hash,get_last_initial_crawled, project_dir
from crawler_utils.chrome_driver_utils import setup_driver, wait_for_page_load
from crawler_config.storage_config import CRYPTO_NEWS_BUCKET
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium import webdriver

def setup_cookie_driver(url, cookie_string):
    driver = setup_driver()

    # Parse the cookie string into a dictionary
    # cookies = {item.split("=")[0].strip(): item.split("=")[1].strip() for item in cookie_string.split(";")}
    cookies = []
    for item in cookie_string.split("; "):
        key, value = item.split("=", 1)
        cookies.append({"name": key.strip(), "value": value.strip()})

    driver.get(url)

    # Add cookies
    for cookie in cookies:
        driver.add_cookie(cookie)

    # Refresh the page to apply cookies
    driver.refresh()
    return driver

def get_detail_article(articles, cookie_string):
    cookies = {item.split("=")[0].strip(): item.split("=")[1].strip() for item in cookie_string.split(";")}
    for article in articles:
        content = "No content"
        url = article['url']
        try:
            for _ in range(3):
                # Make the HTTP request
                try:
                    headers = {
                        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
                    }
                    response = requests.get(url, cookies=cookies, headers=headers)
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
            
            article_card = soup.find("div", class_="m-detail--body")
            if article_card:
                content = ' '.join(article_card.stripped_strings)
            
        except Exception as e: 
            print(f'Error in get content for {url}: ', e)

        if content == "No content":
                print(f"Failed to get content of url: {url}")

        article['content'] = content
    return articles

def full_crawl_articles():
    # Cookie string provided
    cookie_string = "_aren_ab=g=13/; ArenaID=tDmqHbJCz3Nc3Ihc8uWvfw; muid=tDmqHbJCz3Nc3Ihc8uWvfw; _lc2_fpi=93eaded31b81--01jfc8yz8c0bhak2njd20w2ey1; _lc2_fpi_meta=%7B%22w%22%3A1734505102604%7D; _lr_env_src_ats=false; _ga=GA1.1.2135129009.1734505103; permutive-id=fd61cdf0-6b0c-48eb-a775-4d3d8d1b1bf1; _scor_uid=70d007fcfbaf4c21a7e6b9b950c8a357; _lr_sampling_rate=100; _cc_id=1d2de090c7008cc9936f152b39782017; panoramaId_expiry=1734599977426; panoramaId=c4085a1eb1bc0b2675fe861011cca9fb927a8f9914d4a89974d80efe56322394; panoramaIdType=panoDevice; _omappvp=sEHrKduc7viPEBg7fJrHr0luvbK0wNSLQIUQT0jSjJwwS30HYXhFJoJQVpahW5WY8GGqxIXbThs661fOdvguMFvNxDOpIVGq; __qca=P0-1216592980-1734515075799; ArenaGeo=eyJjb3VudHJ5Q29kZSI6IkhLIiwicmVnaW9uQ29kZSI6Ik5PIFJFR0lPTiIsImluRUVBIjpmYWxzZX0=; _li_dcdm_c=.bitcoinmagazine.com; _igt=519c30cd-4929-4ed6-90cc-020d892257f1; _ig=tDmqHbJCz3Nc3Ihc8uWvfw; _ga_DQMZGMPHXN=GS1.1.1734578815.5.1.1734579970.43.0.0; cto_bundle=sWy2Nl85VTMyQW1XYXFMUzB2OXVSVXBPRHpPSmslMkZ4VFNKTUZSQ3FPQWZab2hRWmR3OWdHcVdEQXRVVkpQd29adU5HTERnQjVzRlN1R2hYdm5FYVE0JTJGSHBacW9GVm1vR0wlMkZSJTJCNkxrT0V1RUJxMkd4T25MYTF3aFg0VGNnaG9IdkNkSTRldUJVYm1rV1d5TyUyQjNwREplNGJrRTZIcGNuQ3BEcklCQ0UwSzdHampraTdvJTNE; cto_bidid=eXW_Wl82QUEwWnB1RDR4aFBFJTJGM1AlMkZ1elNpWjZFZCUyRllpcWJEaGNHdnhSejBkVGhvOXhiRyUyRjhxa2JxSHJZc2tsSXBTOGlNSlpTNjVKRll0NzFBY3FOWFpDWXVhSUNzS3I1Y29qWTlscGlTakVLWTZMSkowNThVJTJGY3dGZHFBcUw0SEx0Yzc; _ga_QNM28863L0=GS1.1.1734578816.5.1.1734579970.0.0.0; _ga_KVNBLRQ1PG=GS1.1.1734578816.5.1.1734579970.0.0.0; _awl=2.1734579974.5-5ac493ab6e5b125949781f829c293ae5-6763652d617369612d6561737431-0; cto_bundle=5p2Kzl85VTMyQW1XYXFMUzB2OXVSVXBPRHpDJTJCTWNxWXgxdlFXeEJNcSUyRk5xbkFmMG16NWxjenhKTnB0TDdIbFFFdW9Dc3pHc2wyczFoJTJCN2p0VGRxYiUyQjZsNGZzMUw2eHN1MmZ1dmElMkYwN2hibjdFJTJCdmhUMm5WYlVRamxyQXBicWVUUURIWnVnTFNaYzdPWWhEbmdBUEh4dSUyQklZOVFkUDVSc2t4MzFLNndCb09KN2N3RSUzRA; datadome=KaECM4WXhp43bywlzCTQ6EP9rmqmgRhiVzPpkiw84uxSz9cMmDmoN_N~kGc9~ADEqUf4m1KXjdT4y9np5cl6OB1m4PwMKbPdAaVNN0yN6x_0vAMDPJ7h8reTWNKk1dQx; _sp_id.350a=fbcd7ee3-ed44-4fc8-8ac2-7321af5432a3.1734505103.5.1734580010.1734573968.54c55dae-ef9a-4960-a3d4-e40d58f24351.3dd037ac-91c4-40f9-9065-faa63191b0bf.aa178608-1c1c-469e-b78a-333efea5ddc3.1734578815375.15; sp_debug_s=dq3kcylgffhhwcgnvhy9yj; __gads=ID=ad4f27d82f88ed55:T=1734505116:RT=1734589224:S=ALNI_MZ2WFvC6A78fPGvt2omUZRmBfzd3A; __gpi=UID=00000fa99b3baaab:T=1734505116:RT=1734589224:S=ALNI_MaN8leKEWgi4Yx1NihDqzTfwU_-Lw; __eoi=ID=c331bb2441147f6e:T=1734505116:RT=1734589224:S=AA-AfjY81h3k4wbgL210X31aalsM"
    
    minio_client = connect_minio()
 
    prefix = f'web_crawler/bitcoinmagazine/bitcoinmagazine_initial_batch_'
    last_crawled_id, current_batch = get_last_initial_crawled(minio_client=minio_client, bucket=CRYPTO_NEWS_BUCKET,prefix=prefix)
    URL = f"https://bitcoinmagazine.com/articles"
    print(f"Crawling URL: {URL}")

    # Open the URL
    driver = setup_cookie_driver(URL, cookie_string)
    # Wait for the articles to load initially
    wait_for_page_load(driver, 'section.m-card-group')

    not_crawled = last_crawled_id is None
    articles_data = []
    crawled_id = set()
    batch_size = 100
    previous_news = 0 
    count = 0

    while True:
        # Find all the articles within the container
        try:
            data_div = driver.find_elements(By.CSS_SELECTOR, "div.m-card--content")
        except Exception as e:
            break
        current_news = len(data_div)
        if current_news == previous_news:
            if count == 3:
                break
            count += 1
            time.sleep(3)
        else:
            count = 0
        articles = data_div[previous_news: current_news]
        print(f"Crawling news from {previous_news} to {current_news} news")
        
        for article in articles:
            try:
                # Extract title
                link_element = article.find_element(By.CSS_SELECTOR, "phoenix-ellipsis.m-ellipsis.m-card--header a")
                article_url = link_element.get_attribute("href")
                article_id = generate_url_hash(article_url)
                # Skip if the article URL has already been processed
                if article_id in crawled_id:
                    continue

                if not not_crawled and article_id == last_crawled_id:
                    not_crawled = True
                    continue
                if not_crawled:
                    date_str = article.find_element(By.CSS_SELECTOR, 'phoenix-timeago time').get_attribute("datetime")
                    # Add the article data to the list
                    articles_data.append({
                        "id": article_id,
                        "title": link_element.find_element(By.CSS_SELECTOR,'h2').text.strip(),
                        "url": article_url,
                        "published_at": datetime.fromisoformat(date_str).astimezone(pytz.utc).strftime("%Y-%m-%d %H:%M:%S"),
                        "source": "bitcoinmagazine.com"
                    })
                    crawled_id.add(article_id)
                if len(articles_data) == batch_size:
                    articles_data = get_detail_article(articles=articles_data, cookie_string=cookie_string)
                    new_batch = current_batch + batch_size
                    object_key = f'{prefix}{new_batch}.json'
                    upload_json_to_minio(json_data=articles_data,object_key=object_key)
                    current_batch = new_batch
                    articles_data = []
                    crawled_id=set()
            except Exception as e:
                print(f"Error extracting data for an article: {e}")
    
        # Click the "More stories" button to load more articles
        try:
            load_more_button = driver.find_element(By.CLASS_NAME, "m-footer-loader--button")
            # Click the button
            driver.execute_script("arguments[0].click();", load_more_button)

            previous_news = current_news        
        
        except Exception as e:
            print("Error in load more: ", e)
            break
                
        # Wait for new articles to load
        time.sleep(random.uniform(2, 3))

    if articles_data:
        # print(f'Len data: {len(articles_data)}')
        # print(articles_data[0])
        # print(articles_data[-1])
        articles_data = get_detail_article(articles=articles_data, cookie_string=cookie_string)
        object_key = f'{prefix}{current_batch + len(articles_data)}.json'
        upload_json_to_minio(json_data=articles_data,object_key=object_key)
    driver.quit()

if __name__ == "__main__":
    full_crawl_articles()