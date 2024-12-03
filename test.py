from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.action_chains import ActionChains
from bs4 import BeautifulSoup
from selenium.webdriver.chrome.options import Options
from crypto_utils.chrome_driver_utils import setup_driver, wait_for_page_load
import requests
from datetime import datetime

# driver = setup_driver()


# URL of the webpage
url = "https://www.coindesk.com/markets/2024/12/02/xrp-replaces-tether-as-3rd-largest-cryptocurrency-while-btc-faces-384-m-sell-wall"

published_at = "1970-01-01 00:00:00"
content = "No content"
try:
    # Make the HTTP request
    response = requests.get(url, timeout=10)

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

        article_header_div = soup.find('div', {'data-module-name': 'article-body'})
        if article_header_div:
            for unwanted in article_header_div.select(".border, .playlistThumb, .article-ad"):
                unwanted.decompose()
            
            content = ' '.join(article_header_div.stripped_strings)


    if published_at == "1970-01-01 00:00:00":
            print(f'Failed to get publish date for {url}')
    if content == "No content":
        print(f'Failed to get content for {url}')

    print(content) 
    print(published_at)
except Exception as e:
    print(f"Error get publish date for URL {url}: {e}")