from datetime import datetime
from dateutil import parser
from dateutil.relativedelta import relativedelta
import re
from utils.minio_utils import upload_json_to_minio
import json
from utils.common_utils import save_to_json
from utils.chrome_driver_utils import setup_driver, wait_for_page_load
from crawler.coindesk import crawl_articles_by_topic
from crawler.cointelegraph import crawl_tag_articles

crawl_tag_articles('bitcoin', '2024-11-20')