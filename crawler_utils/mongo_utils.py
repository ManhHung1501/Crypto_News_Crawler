from pymongo import MongoClient
from crawler_config.storage_config import MONGODB_URL, MONGO_DB, MONGO_COLLECTION, CRYPTO_NEWS_BUCKET
from io import BytesIO
import json 
from datetime import datetime

def connect_mongodb():
    return MongoClient(MONGODB_URL)

def load_json_from_minio_to_mongodb(
    minio_client, object_name
):

    # Connect to MongoDB
    mongo_client = connect_mongodb()
    collection = mongo_client[MONGO_DB][MONGO_COLLECTION]

    # Fetch JSON file from MinIO
    response = minio_client.get_object(CRYPTO_NEWS_BUCKET, object_name)
    json_data = json.load(BytesIO(response.read()))  # Convert response to JSON
    
    # Ensure the data is a list
    if not isinstance(json_data, list):
        raise ValueError("JSON file should contain a list of documents")
    for row in json_data:
        try:
            if row['published_at'] == "1970-01-01 00:00:00":
                row['published_at'] = None
            else:
                row['published_at'] = int(datetime.strptime(row['published_at'], "%Y-%m-%d %H:%M:%S").timestamp())
        except Exception as e:
            row['published_at'] = None
    # Insert JSON data into MongoDB
    result = collection.insert_many(json_data)
    
    print(f"Inserted {len(result.inserted_ids)} documents into MongoDB.")

    