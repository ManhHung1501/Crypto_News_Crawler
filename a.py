import requests

# Test URL to fetch your public IP
test_url = "http://httpbin.org/ip"

try:
    # Make a request to verify the IP address
    response = requests.get(test_url, timeout=10)
    print("Public IP through VPN:", response.json())
except requests.exceptions.RequestException as e:
    print("Error:", e)
