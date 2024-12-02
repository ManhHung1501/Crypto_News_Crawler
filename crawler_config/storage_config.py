import os
from dotenv import load_dotenv
from crypto_utils.common_utils import project_dir

# Load the .env file
load_dotenv(f"{project_dir}/.env")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
CRYPTO_NEWS_BUCKET = os.getenv("MINIO_BUCKET")