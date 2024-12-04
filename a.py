from crawler_utils.minio_utils import upload_json_to_minio, connect_minio
from crawler_utils.common_utils import generate_url_hash, get_last_crawled, save_last_crawled, get_last_initial_crawled

minio_client = connect_minio()
prefix = f'web_crawler/cointelegraph/bitcoin/cointelegraph_bitcoin_initial_batch_'

a, b  = get_last_initial_crawled(minio_client, 'crypto-news', prefix)

print(a, b)