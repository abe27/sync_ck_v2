import csv
from datetime import datetime, timedelta
import os
import shutil
import sys
import time
import requests
import urllib
import urllib3
from bs4 import BeautifulSoup
from dotenv import load_dotenv
load_dotenv()

api_host = os.getenv("API_HOST")
api_user = os.getenv("API_USERNAME")
api_password = os.getenv("API_PASSWORD")

distination_dir = "data/edi"
source_dir = "data/download"


def main():
    try:
        # login
        passwd = urllib.parse.quote(api_password)
        payload = f'username={api_user}&password={passwd}'
        response = requests.request(
            "POST", f"{api_host}/login", headers={'Content-Type': 'application/x-www-form-urlencoded'}, data=payload)
        auth = response.json()
        token = auth["data"]["jwt_token"]
        headers = {
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/x-www-form-urlencoded'
        }

        # get mailbox
        try:
            response = requests.request(
                "GET", f"{api_host}/edi/mailbox", headers=headers, data={})
            obj = response.json()
            for data in obj["data"]:
                mailbox = data["mailbox"]
                password = urllib.parse.quote(data["password"])
                host_url = data["host_url"]
                payload = f'operation=LOGON&remote={mailbox}&password={password}'
                urllib3.disable_warnings()
                session = requests.request("POST",
                                           host_url,
                                           headers={
                                               'Content-Type': 'application/x-www-form-urlencoded', },
                                           verify=False,
                                           data=payload,
                                           timeout=3)
                txt = None
                docs = BeautifulSoup(session.text, "html.parser")
                for i in docs.find_all("hr"):
                    txt = (i.previous).replace("\n", "")

                is_status = True
                if txt.find("751") >= 0:
                    is_status = False

                if is_status:
                    ### s.5 download gedi data ###
                    etd = str(
                        (datetime.now() - timedelta(days=1)).strftime("%Y%m%d"))
                    # get cookies after login.
                    if session.status_code == 200:
                        payload = f"operation=DIRECTORY&fromdate={etd}&Submit=Receive"
                        r = requests.request(
                            "POST",
                            host_url,
                            data=payload,
                            headers={
                                "Content-Type": "application/x-www-form-urlencoded"
                            },
                            verify=False,
                            timeout=3,
                            cookies=session.cookies,
                        )
                        # print(type(r))
                        soup = BeautifulSoup(r.text, "html.parser")
                        for tr in soup.find_all("tr"):
                            found = False
                            i = 0
                            docs = []
                            for td in tr.find_all("td"):
                                txt = (td.text).rstrip().lstrip()
                                docs.append(txt)
                                if td.find("a") != None:
                                    found = True

                                if found is True:  # False =debug,True=prod.
                                    if len(docs) >= 9:
                                        # if str(docs[3])[:len("OES.VCBI")] == "OES.VCBI":
                                        l = {
                                            "mailbox": docs[0],
                                            "batch_id": docs[1],
                                            "batch_file": docs[3],
                                            "uploaded_at": datetime.strptime(f"{docs[4]} {docs[5]}", "%b %d, %Y %I:%M %p"),
                                            "mailto": docs[8]
                                        }
                                        url_downloaded = f"{host_url}?operation=DOWNLOAD&mailbox_id={mailbox}&batch_num={docs[1]}&data_format={docs[7]}&batch_id={docs[3]}"
                                        # makedir folder gedi is exits
                                        dirs_dist = f'{source_dir}/{(l["uploaded_at"]).strftime("%Y%m%d")}'
                                        os.makedirs(dirs_dist, exist_ok=True)
                                        # download file
                                        request = requests.get(
                                            url_downloaded,
                                            stream=True,
                                            verify=False,
                                            cookies=session.cookies,
                                            allow_redirects=True,
                                        )
                                        txt = BeautifulSoup(
                                            request.content, "lxml")
                                        # Write data to GEDI File
                                        file_name = f'{dirs_dist}/{l["batch_id"]}.{docs[3]}'
                                        # ตรวจสอบข้อมูลซ้ำ ถ้าเจอลบไฟล์ที่ซ้ำออก
                                        if os.path.exists(file_name):
                                            os.remove(file_name)

                                        f = open(file_name,
                                                 mode="a",
                                                 encoding="ascii",
                                                 newline="\r\n")
                                        for p in txt:
                                            f.write(p.text)
                                        f.close()
                                        print(f"download file: {file_name}")
                                        time.sleep(5)

                                i += 1

        except Exception as ex:
            print(ex)
            pass

        try:
            # print(headers)
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

        ## Sync Receive
        
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
