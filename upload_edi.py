import csv
from datetime import datetime, timedelta
import os
import shutil
import sys
import time
import requests
import urllib
from dotenv import load_dotenv
load_dotenv()

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

        # get mailbox
        try:
            url = "http://127.0.0.1:4040/api/v1/edi/mailbox"
            payload = {}
            response = requests.request(
                "GET", url, headers=headers, data=payload)
            print(response.text)
        except Exception as ex:
            print(ex)
            pass

        try:
            # print(headers)
            distination_dir = "data/edi"
            source_dir = "data/download"
            list_dir = os.listdir(os.path.join(
                os.path.dirname(__file__), source_dir))
            list_dir.sort()

            rn = 1
            for x in list_dir:
                if x != ".DS_Store":
                    edi_dir = os.listdir(os.path.join(source_dir, x))
                    edi_dir.sort()
                    for i in edi_dir:
                        if i != ".DS_Store":
                            on_dir = os.path.join(source_dir, x)
                            filename = os.path.join(on_dir, i)
                            batch_id = str(i)[:7]
                            batch_name = str(i)[8:]
                            batch_date = datetime.strptime(
                                (str(i)[(len(batch_name) - 10):]
                                 ).replace(".TXT", ""),
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

                            # backup file edi
                            to_dir = os.path.join(distination_dir, x)
                            if os.path.exists(to_dir) != True:
                                os.makedirs(to_dir)

                            shutil.move(filename, os.path.join(to_dir, i))
                            rn += 1

                    os.rmdir(os.path.join(source_dir, x))
                # new dir
                # time.sleep(5)
        except Exception as ex:
            print(ex)
            pass
        # logout
        response = requests.request(
            "GET", f"{api_host}/auth/logout", headers=headers, data={})
        print(response.text)
    except Exception as e:
        print(e)
        pass


if __name__ == "__main__":
    main()
    sys.exit(0)
