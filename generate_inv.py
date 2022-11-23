import csv
from datetime import date, datetime, timedelta
import math
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


def create_log(title, description, is_status):
    payload = f'title={title}&description={description}&is_active={str(is_status).lower()}'
    response = requests.request("POST", f"{api_host}/logs", headers={
                                'Content-Type': 'application/x-www-form-urlencoded'}, data=payload)
    print(f"create log {title} status: {response.status_code}")


def main():
    try:
        # login
        today = date.today()
        start_date = today - timedelta(days=today.weekday())
        end_date = start_date  + timedelta(days=12)
        print(f"Generate Order Start: {str(start_date)} End: {end_date}")
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
        # print(f"{api_host}/order/ent?factory=INJ&start_etd={str(start_date)[:10]}&end_date={str(end_date)[:10]}")
        response = requests.request(
            "PATCH", f"{api_host}/order/ent?factory=INJ&start_etd={str(start_date)[:10]}&end_date={str(end_date)[:10]}", headers=headers, data={})
        print(f"generate invoice status: {response.status_code}")
        # print(response.message)
        # logout
        response = requests.request(
            "GET", f"{api_host}/auth/logout", headers=headers, data={})
        print(response.text)
        status = True
        if response.status_code == 500:
            status = False
        create_log("Auto Generate Invoice",
                   f"Generate Invoice {response.status_code}", status)

    except Exception as e:
        print(e)
        create_log("Auto Generate Invoice",
                   f"Generate Invoice is error {str(e)}", False)
        pass


if __name__ == "__main__":
    main()
    sys.exit(0)
