
from crawler_utils.minio_utils import connect_minio
from crawler_utils.mongo_utils import connect_mongodb
from io import BytesIO
import json 
from datetime import datetime

def migrate_all_json(
    bucket_name, prefix,
     mongo_db, mongo_collection
):
    """
    Load JSON data from MinIO and insert into MongoDB.

    :param minio_endpoint: MinIO server URL (e.g., "172.18.0.4:9000")
    :param access_key: MinIO access key
    :param secret_key: MinIO secret key
    :param bucket_name: MinIO bucket containing the JSON file
    :param object_name: JSON file name in MinIO
    :param mongo_uri: MongoDB connection string (e.g., "mongodb://localhost:27017/")
    :param mongo_db: MongoDB database name
    :param mongo_collection: MongoDB collection name
    """
    # Connect to MinIO
    minio_client = connect_minio()

    # Connect to MongoDB
    mongo_client = connect_mongodb()
    collection = mongo_client[mongo_db][mongo_collection]

    # List all JSON files under the given prefix
    objects = minio_client.list_objects(bucket_name, prefix=prefix, recursive=True)

    total_inserted = 0

    for obj in objects:
        object_name = obj.object_name
        if object_name.endswith(".json"):  # Process only JSON files
            print(f"Processing file: {object_name}")
            
            # Get JSON file content
            response = minio_client.get_object(bucket_name, object_name)
            json_data = json.load(BytesIO(response.read()))  # Convert response to JSON
            
            # Ensure the data is a list
            if isinstance(json_data, dict):  # If single JSON object, convert to list
                json_data = [json_data]
            elif not isinstance(json_data, list):
                print(f"Skipping {object_name}, as it's not a valid JSON list.")
                continue
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
            inserted_count = len(result.inserted_ids)
            total_inserted += inserted_count
            print(f"Inserted {inserted_count} documents from {object_name}.")

    print(f"Total inserted documents: {total_inserted}")

# Example usage
if __name__ == "__main__":
    migrate_all_json(
        bucket_name="crypto-news",
        prefix="web_crawler/",
        mongo_db="graphscope",
        mongo_collection="crypto_news_crawl"
    )