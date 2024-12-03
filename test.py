from crawler_utils.common_utils import get_last_initial_crawled
from crawler_utils.minio_utils import connect_minio
from crawler_config.storage_config import CRYPTO_NEWS_BUCKET
minio_client = connect_minio()


prefix = f'web_crawler/cryptoslate/cryptoslate_initial_batch_'
a = get_last_initial_crawled(minio_client, bucket=CRYPTO_NEWS_BUCKET, prefix=prefix)
print(a)