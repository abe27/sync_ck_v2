import csv
import xlsxwriter
from datetime import datetime, timedelta
import os
import shutil
import sys
import time
from uuid import uuid4
import requests
import urllib
import urllib3
import cx_Oracle
# from nanoid import generate
from bs4 import BeautifulSoup
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), ".env.local"))

api_host = os.getenv("API_HOST")
api_user = os.getenv("API_USERNAME")
api_password = os.getenv("API_PASSWORD")

ORA_DNS = f"{os.environ.get('ORAC_DB_HOST')}/{os.environ.get('ORAC_DB_SERVICE')}"
ORA_USERNAME = os.environ.get('ORAC_DB_USERNAME')
ORA_PASSWORD = os.environ.get('ORAC_DB_PASSWORD')

distination_dir = "data/edi"
source_dir = "data/download"

line_inj_dom = ""
line_inj_com = ""
line_aw_com = ""

pool = cx_Oracle.SessionPool(user=ORA_PASSWORD,
                                 password=ORA_USERNAME,
                                 dsn=ORA_DNS,
                                 min=2,
                                 max=100,
                                 increment=1,
                                 encoding="UTF-8")
# Acquire a connection from the pool
Oracon = pool.acquire()
Oracur = Oracon.cursor()


def create_log(title, description, is_status):
    try:
        payload = f'title={title}&description={description}&is_active={str(is_status).lower()}'
        response = requests.request("POST", f"{api_host}/logs", headers={
                                    'Content-Type': 'application/x-www-form-urlencoded'}, data=payload)
        print(f"create log {title} status: {response.status_code}")
    except Exception as ex:
        print(ex)
        pass


def get_line_token(whs, fac="INJ"):
    response = requests.request(
        "GET", f"{api_host}/notify", headers={}, data={})
    obj = response.json()
    for i in obj["data"]:
        if i["whs"]["title"] == whs and i["factory"]["title"] == fac:
            return str(i["token"])


def line_notification(whs, msg):
    try:
        token = get_line_token(whs, fac="INJ")
        url = "https://notify-api.line.me/api/notify"
        payload = f"message={msg}"
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/x-www-form-urlencoded",
        }

        response = requests.request("POST",
                                    url,
                                    headers=headers,
                                    data=payload.encode("utf-8"))
        print(f"line status => {response}")
        # create_log("LineNotify", f"{msg} status: {response.status_code}", True)
        return True
    except Exception as ex:
        print(ex)
        create_log("LineNotify error", f"{msg} err: {str(ex)}", False)
        pass

    return False


def upload_inv():
    try:
        list_dir = os.listdir("data/invoice")
        list_dir.sort()
        # x = 0
        for dir in list_dir:
            filePath = f"data/invoice/{dir}"
            f = open(filePath, 'rb')
            files = [('file', (dir, f, 'application/octet-stream'))]
            response = requests.request("POST", f"{api_host}/upload/invoice/tap", headers={}, data={}, files=files)
            f.close()
            shutil.move(filePath, f"TmpInvoice/{dir}")
            create_log("Upload Receive Excel",
                       f"""{dir} is success {response.status_code}""", True)
            print(f"Upload Receive Excel {dir} status {response.status_code}")
            # if x > 3:
            #     return x

            # x += 1
        return True

    except Exception as e:
        print(e)
        create_log("Upload Receive Excel", f"""Error: {str(e)}""", False)
        pass

    return False

def upload_receive_excel():
    try:
        list_dir = "data/receive"
        for dir in os.listdir(list_dir):
            filePath = f"data/receive/{dir}"
            f = open(filePath, 'rb')
            files = [('file', (dir, f, 'application/octet-stream'))]
            response = requests.request("POST", f"{api_host}/upload/receive", headers={}, data={}, files=files)
            f.close()
            shutil.move(filePath, f"data/excels/{dir}")
            create_log("Upload Receive Excel",
                       f"""{dir} is success {response.status_code}""", True)
    except Exception as e:
        print(e)
        create_log("Upload Receive Excel", f"""Error: {str(e)}""", False)
        pass


if __name__ == "__main__":
    upload_receive_excel()
    upload_inv()
    pool.release(Oracon)
    pool.close()
    sys.exit(0)