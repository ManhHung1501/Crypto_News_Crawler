import os
from dotenv import load_dotenv
from crypto_utils.common_utils import project_dir

# Load the .env file
load_dotenv(f"{project_dir}/.env")

CHROME_DRIVER_PATH = os.getenv("CHROME_DRIVER_PATH")