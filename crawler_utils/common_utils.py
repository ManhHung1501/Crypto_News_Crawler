import os
import hashlib
import re
import json
from datetime import datetime, timedelta
from dateutil import parser
from dateutil.relativedelta import relativedelta

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

def get_last_initial_crawled(minio_client, bucket, prefix):
    try:
        # List objects with the given prefix
        objects = minio_client.list_objects(bucket, prefix=prefix)
        files = []

        for obj in objects:
            key = obj.object_name
            # Match files with the dynamic prefix and extract the batch number
            match = re.search(rf'{prefix}(\d+)', key)
            if match:
                files.append((key, int(match.group(1))))
        
        if not files:
            print("No matching files found.")
            last_id = None
            last_batch = 0

        # Find the latest file based on the batch number
        latest_file = max(files, key=lambda x: x[1])
        
        # Retrieve the content of the latest file
        response = minio_client.get_object(bucket, latest_file[0])
        file_content = response.read().decode('utf-8')
        
        # Parse the JSON content
        data = json.loads(file_content)
        last_id = data[-1]['id']
        last_batch = latest_file[1]
        
    except Exception as e:
        print(f"Error in get last initial crawled: {e}")
        last_id = None
        last_batch = 0
    
    print(f'Last ID: {last_id}, Last Batch: {last_batch}')
    return last_id, last_batch

def save_last_crawled(new_urls, STATE_FILE):
    save_path = os.path.join(project_dir, STATE_FILE)

    # Ensure the directory exists
    directory = os.path.dirname(save_path)
    if not os.path.exists(directory):
        os.makedirs(directory)

    with open(save_path, 'w') as f:
        json.dump({"last_crawled": new_urls}, f)

def get_last_crawled(STATE_FILE, minio_client, bucket, prefix):
    try:
        file_path = os.path.join(project_dir, STATE_FILE)
        with open(file_path, 'r') as f:
            last_crawled = json.load(f).get("last_crawled", [])
    except FileNotFoundError:
        # List objects with the given prefix
        objects = minio_client.list_objects(bucket, prefix=prefix)
        files = []

        for obj in objects:
            key = obj.object_name
            # Match files with the dynamic prefix and extract the batch number
            match = re.search(rf'{prefix}(\d+)', key)
            if match:
                files.append((key, int(match.group(1))))
        
        if not files:
            last_crawled =  []

        # Find the latest file based on the batch number
        latest_file = min(files, key=lambda x: x[1])[0]

        # Retrieve the content of the latest file
        response = minio_client.get_object(bucket, latest_file)
        file_content = response.read().decode('utf-8')
        
        # Parse the JSON content
        data = json.loads(file_content)[:5] 

        last_crawled =  [artc['id'] for artc in data]
        save_last_crawled(last_crawled, file_path)
    print(f'Last crawled id: {last_crawled}')
    return last_crawled