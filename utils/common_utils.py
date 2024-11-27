import os
import hashlib
import re
import json
from datetime import datetime, timedelta
from dateutil import parser
from dateutil.relativedelta import relativedelta
from minio_utils import connect_minio
from config.storage_config import CRYPTO_NEWS_BUCKET

def get_project_path():
    current_path = os.path.abspath(__file__)
    current_directory = os.path.dirname(current_path)
    return os.path.dirname(current_directory)
project_dir = get_project_path()


def generate_url_hash(url):
    # Use MD5 to create a 128-bit hash
    hash_object = hashlib.md5(url.encode('utf-8'))
    # Convert the hash to a hexadecimal string
    url_hash = hash_object.hexdigest()
    return url_hash

def get_last_initial_crawled(prefix):
    try:
        minio_client = connect_minio()  # Connect to your MinIO instance
        
        # List objects with the given prefix
        objects = minio_client.list_objects_v2(Bucket=CRYPTO_NEWS_BUCKET, Prefix=prefix)
        files = []

        for obj in objects.get('Contents', []):
            key = obj['Key']
            # Match files with the dynamic prefix and extract the batch number
            match = re.search(rf'{prefix}(\d+)', key)
            if match:
                files.append((key, int(match.group(1))))
        
        if not files:
            print("No matching files found.")
            return None, 100

        # Find the latest file based on the batch number
        latest_file = max(files, key=lambda x: x[1])[0]

        # Retrieve the content of the latest file
        response = minio_client.get_object(Bucket=CRYPTO_NEWS_BUCKET, Key=latest_file)
        file_content = response['Body'].read().decode('utf-8')
        
        # Parse the JSON content
        data = json.loads(file_content)
        return data[-1]['id'], files[-1][1]  # Return last ID and latest batch number

    except Exception as e:
        print(f"Error: {e}")
        return None, 100


def get_last_crawled(STATE_FILE):
    try:
        file_path = os.path.join(project_dir, STATE_FILE)
        with open(file_path, 'r') as f:
            return json.load(f).get("last_crawled", [])
    except FileNotFoundError:
        return []

def save_last_crawled(new_urls, STATE_FILE):
    save_path = os.path.join(project_dir, STATE_FILE)

    # Ensure the directory exists
    directory = os.path.dirname(save_path)
    if not os.path.exists(directory):
        os.makedirs(directory)

    with open(save_path, 'w') as f:
        json.dump({"last_crawled": new_urls}, f)