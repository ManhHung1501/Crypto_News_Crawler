from datetime import datetime
from dateutil import parser
from dateutil.relativedelta import relativedelta
import re
from utils.minio_utils import upload_json_to_minio
import json
from selenium.common.exceptions import NoSuchElementException
from utils.chrome_driver_utils import setup_driver, wait_for_page_load
from crawler import coindesk,cointelegraph,cryptoslate
from selenium.webdriver.common.by import By
import requests
from bs4 import BeautifulSoup
import time

coindesk.full_crawl_articles()
