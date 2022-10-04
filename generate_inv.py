import csv
from datetime import datetime, timedelta
import os
import sys
import time
import requests
import urllib
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), ".env.local"))

api_host = os.getenv("API_HOST")
api_user = os.getenv("API_USERNAME")
api_password = os.getenv("API_PASSWORD")

def main():
    try:
        # login
        passwd = urllib.parse.quote(api_password)
        payload = f'username={api_user}&password={passwd}'
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        response = requests.request(
            "POST", f"{api_host}/login", headers=headers, data=payload)
        auth = response.json()
        token = auth["data"]["jwt_token"]
        headers = {
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/x-www-form-urlencoded'
        }
        # generate invoice
        response = requests.request(
            "PATCH", f"{api_host}/order/ent", headers=headers, data={})
        print(f"generate invoice status: {response.status_code}")
        # logout
        response = requests.request(
            "GET", f"{api_host}/auth/logout", headers=headers, data={})
        print(response.text)
    
    except Exception as e:
            print(e)

if __name__ == "__main__":
    main()
    sys.exit(0)
