from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException
import os

def setup_driver():
    """Set up the Chrome WebDriver with the appropriate service."""
    options = Options()
    options.add_argument("--headless") 
    options.add_argument("--disable-gpu") 
    options.add_argument("--no-sandbox")
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
    options.add_argument("--disable-blink-features=AutomationControlled") 
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--blink-settings=imagesEnabled=false")
    options.add_argument("--disable-javascript")
    options.add_argument("--ignore-certificate-errors")
    options.add_argument("--ignore-ssl-errors")
    options.add_argument("--disable-infobars")
    options.add_argument("--disable-extensions")
    options.add_argument("window-size=1200x600")

    driver_path = ChromeDriverManager().install()
    
    binary_path = os.path.join(os.path.dirname(driver_path), "chromedriver")
    if not os.path.isfile(binary_path):
        raise FileNotFoundError(f"ChromeDriver binary not found at: {binary_path}")
    if not os.access(binary_path, os.X_OK):
        raise PermissionError(f"ChromeDriver binary is not executable: {binary_path}")
    
    service = Service(binary_path)
    driver = webdriver.Chrome(service=service, options=options)
    return driver

# Function to wait for page to load
def wait_for_page_load(driver, css_selector: str, timeout=10):
    """Wait for the articles to load on the page."""
    try:
        # Wait until the elements are visible and present in the DOM
        wait = WebDriverWait(driver, timeout, poll_frequency=0.5)
        wait.until(EC.visibility_of_all_elements_located((By.CSS_SELECTOR, css_selector)))
        print(f"Page loaded successfully with {css_selector} elements.")
    except TimeoutException:
        print(f"Timed out waiting for elements with CSS selector: {css_selector}")