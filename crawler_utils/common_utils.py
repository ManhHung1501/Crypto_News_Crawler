import os
import hashlib
import re
import json
from datetime import datetime

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

def get_last_crawled(STATE_FILE, minio_client, bucket, prefix):
    objects = minio_client.list_objects(bucket, prefix=STATE_FILE)
    files = []
    for obj in objects:
        key = obj.object_name
        # Match files with the dynamic prefix and extract the batch number
        timestamp = key.split('_')[-1].replace('.json','')
        files.append((key, timestamp))
    
    if not files:
        print("No incremental files found.")
        objects = minio_client.list_objects(bucket, prefix=prefix)

        for obj in objects:
            key = obj.object_name
            # Match files with the dynamic prefix and extract the batch number
            match = re.search(rf'{prefix}(\d+)', key)
            if match:
                files.append((key, int(match.group(1))))

        # Find the latest file based on the batch number
        latest_file = min(files, key=lambda x: x[1])
    else:
        latest_file = max(files, key=lambda x: x[1])
    
    print(f'Read Last ID from : {latest_file[0]}')
    # Retrieve the content of the latest file
    response = minio_client.get_object(bucket, latest_file[0])
    file_content = response.read().decode('utf-8')
    
    # Parse the JSON content
    data = json.loads(file_content)[:5] 

    last_crawled =  [artc['id'] for artc in data]
      
    print(f'Last crawled id: {last_crawled}')
    return last_crawled