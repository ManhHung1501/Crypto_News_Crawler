import json
from minio import Minio
from minio.error import S3Error
from io import BytesIO
from crawler_config.storage_config import MINIO_ENDPOINT,ACCESS_KEY,SECRET_KEY,CRYPTO_NEWS_BUCKET

def connect_minio() -> Minio:
    minio_client = Minio(
        MINIO_ENDPOINT,
        access_key=ACCESS_KEY,
        secret_key=SECRET_KEY,
        secure=False 
    )
    return minio_client

def upload_json_to_minio(json_data, object_key: str, bucket_name: str = CRYPTO_NEWS_BUCKET ):
    """
    Upload a JSON list directly to MinIO.

    :param json_data: Python list containing JSON data.
    :param bucket_name: MinIO bucket name.
    :param object_key: Object name in the MinIO bucket.
    """
    try:
        minio_client = connect_minio()
        # Convert the list to a JSON string
        json_string = json.dumps(json_data, indent=4, ensure_ascii=False)
        json_string = json_string.replace('\\"', '"')
        
        # Convert the JSON string to a bytes stream
        json_bytes = BytesIO(json_string.encode("utf-8"))

        # Create the bucket if it doesn't exist
        if not minio_client.bucket_exists(bucket_name):
            minio_client.make_bucket(bucket_name)
            print(f"Created bucket: {bucket_name}")
        json_bytes.seek(0)
        # Upload the JSON string as an object
        minio_client.put_object(
            bucket_name,
            object_key,
            data=json_bytes,
            length=json_bytes.getbuffer().nbytes,
            content_type="application/json"
        )
        print(f"Uploaded JSON data to {bucket_name}/{object_key}")
    except S3Error as e:
        print(f"Error uploading JSON data: {e}")