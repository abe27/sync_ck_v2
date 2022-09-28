import csv
from datetime import datetime, timedelta
import os
import time
import requests
import urllib
from dotenv import load_dotenv
load_dotenv()

api_host = os.getenv("API_HOST")
api_user = os.getenv("API_USERNAME")
api_password = os.getenv("API_PASSWORD")

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
# print(headers)
# read user
list_dir = os.listdir(os.path.join(os.path.dirname(__file__), 'data/edi'))
list_dir.sort()

rn = 1
for x in list_dir:
    if x != ".DS_Store":
        edi_dir = os.listdir(os.path.join('data/edi', x))
        edi_dir.sort()
        for i in edi_dir:
            if i != ".DS_Store":
                on_dir = os.path.join('data/edi', x)
                filename = os.path.join(on_dir, i)
                batch_id = str(i)[:7]
                batch_name = str(i)[8:]
                batch_date = datetime.strptime(
                    (str(i)[(len(batch_name) - 10):]).replace(".TXT", ""),
                    "%Y%m%d%H%M%S").strftime("%Y-%m-%dT%H:%M:%S%Z") + ".000Z"
                payload = {
                    'mailbox_id': 'Y32V802',
                    'batch_no': batch_id,
                    'creation_on': batch_date,
                    'flags': 'C RT',
                    'format_type': 'A',
                    'originator': 'Y32TPS1',
                    'is_download': 'false',
                    'is_active': 'true'
                }

                # print(payload)
                f = open(filename, 'rb')
                files = [
                    ('file_edi', (batch_name, f, 'application/octet-stream'))]
                response = requests.request("POST",
                                            f'{api_host}/edi/file',
                                            headers={
                                                'Authorization': f'Bearer {token}', },
                                            data=payload,
                                            files=files)
                f.close()
                print(
                    f"{rn} ==> upload file edi: {batch_name} status: {response.status_code}")
                rn += 1

    time.sleep(5)

# logout
response = requests.request(
    "GET", f"{api_host}/auth/logout", headers=headers, data={})
print(response.text)
