from datetime import datetime
from dateutil import parser
from dateutil.relativedelta import relativedelta
import re
from utils.minio_utils import upload_json_to_minio
import json
from utils.chrome_driver_utils import setup_driver, wait_for_page_load
from crawler.coindesk import crawl_articles_by_topic
from crawler.cointelegraph import crawl_articles_by_tag
from crawler.cryptoslate import crawl_articles
crawl_articles(20)