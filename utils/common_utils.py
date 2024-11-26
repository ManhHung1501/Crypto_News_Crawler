import os
import hashlib
import re
from datetime import datetime, timedelta
from dateutil import parser
from dateutil.relativedelta import relativedelta


def get_project_path():
    current_path = os.path.abspath(__file__)
    current_directory = os.path.dirname(current_path)
    return os.path.dirname(current_directory)
project_dir = get_project_path()

def parse_coindesk_date(date_str):
    # Check for relative date like '18 HRS AGO', '53 MINS AGO'
    if 'AGO' in date_str.upper():
        match = re.match(r'(\d+)\s*(HRS|MINS)\s*AGO', date_str.upper())
        if match:
            value = int(match.group(1))
            unit = match.group(2)
            
            now = datetime.now()

            if unit == 'HRS':
                return (now - relativedelta(hours=value)).strftime('%Y-%m-%d')
            elif unit == 'MINS':
                return (now - relativedelta(minutes=value)).strftime('%Y-%m-%d')
    
    # If it's not a relative date, attempt to parse it as a full date
    try:
        parsed_date = parser.parse(date_str)
        return parsed_date.strftime('%Y-%m-%d')
    except ValueError:
        return None

def parse_cryptoslate_date(relative_time):
    # Get the current date and time
    now = datetime.now()

    # Normalize the input to lowercase for easier matching
    relative_time = relative_time.lower()

    # Parse the input and adjust the date accordingly
    if "minute" in relative_time:
        return now.strftime('%Y-%m-%d')
    elif "hour" in relative_time:
        hours = int(relative_time.split()[0])
        return (now - timedelta(hours=hours)).strftime('%Y-%m-%d')
    elif "day" in relative_time:
        days = int(relative_time.split()[0])
        return (now - timedelta(days=days)).strftime('%Y-%m-%d')
    elif "week" in relative_time:
        weeks = int(relative_time.split()[0])
        return (now - timedelta(weeks=weeks)).strftime('%Y-%m-%d')
    elif "month" in relative_time:
        months = int(relative_time.split()[0])
        return (now - timedelta(days=30 * months)).strftime('%Y-%m-%d')
    elif "year" in relative_time:
        years = int(relative_time.split()[0])
        return (now.replace(year=now.year - years)).strftime('%Y-%m-%d')
    else:
        raise ValueError("Unsupported time format")

def generate_url_hash(url):
    # Use MD5 to create a 128-bit hash
    hash_object = hashlib.md5(url.encode('utf-8'))
    # Convert the hash to a hexadecimal string
    url_hash = hash_object.hexdigest()
    return url_hash
