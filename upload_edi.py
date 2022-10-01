import csv
from datetime import datetime, timedelta
import os
import shutil
import sys
import time
import requests
import urllib
import urllib3
import cx_Oracle
from nanoid import generate
from bs4 import BeautifulSoup
from dotenv import load_dotenv
load_dotenv()

api_host = os.getenv("API_HOST")
api_user = os.getenv("API_USERNAME")
api_password = os.getenv("API_PASSWORD")

ORA_DNS = f"{os.environ.get('ORAC_DB_HOST')}/{os.environ.get('ORAC_DB_SERVICE')}"
ORA_USERNAME = os.environ.get('ORAC_DB_USERNAME')
ORA_PASSWORD = os.environ.get('ORAC_DB_PASSWORD')

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

distination_dir = "data/edi"
source_dir = "data/download"


def line_notification(token, msg):
    try:
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
        if response.status_code == 200:
            return True
    except Exception as ex:
        print(ex)
        pass

    return False


def get_rvn():
    rvm_no = None
    isFound = True
    while isFound:
        rvm_no = (Oracur.execute(
            f"select 'BD' ||to_char(sysdate,'YYMMDD')|| trim(to_char(rckey.nextval,'00000')) genrunno  from dual"
        )).fetchone()[0]
        ischeck = int((Oracur.execute(
            f"SELECT count(RVMANAGINGNO) FROM TXP_RECTRANSBODY WHERE RVMANAGINGNO='{rvm_no}'"
        )).fetchone()[0])
        if ischeck == 0:
            isFound = False

    return rvm_no


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

        # Sync Receive
        try:
            response = requests.request(
                "GET", f"{api_host}/receive/ent", headers=headers, data={})
            data = response.json()
            for x in data["data"]:
                # print(x)
                receive_id = x["id"]
                batch_id = x["file_edi"]["batch_no"]
                transfer_out_no = x["transfer_out_no"]
                item = x["item"]
                receive_date = str(x["receive_date"])[:10]
                factory = x["file_edi"]["factory"]["title"]
                cd = x["file_edi"]["factory"]["cd_code"]
                rsstype = x["receive_type"]["whs"]["value"]
                whs = x["receive_type"]["whs"]["description"]
                whs_name = x["receive_type"]["whs"]["title"]
                ctn = x["plan_ctn"]
                # check receive ent
                sql_ent = Oracur.execute(
                    f"SELECT RECEIVINGKEY FROM TXP_RECTRANSENT WHERE RECEIVINGKEY='{transfer_out_no}'")
                sql_ent_insert = f"""INSERT INTO TXP_RECTRANSENT(RECEIVINGKEY,RECEIVINGMAX,RECEIVINGDTE,VENDOR,RECSTATUS,RECISSTYPE,RECPLNCTN,RECENDCTN,UPDDTE,SYSDTE,GEDI_FILE)VALUES('{transfer_out_no}',{item},to_date('{str(receive_date)}', 'YYYY-MM-DD'),'{factory}',0,'{rsstype}',{ctn},0,current_timestamp,current_timestamp,'{batch_id}')"""
                txt_ent = "INSERT"
                if sql_ent.fetchone() != None:
                    txt_ent = "UPDATE"
                    sql_ent_insert = f"UPDATE TXP_RECTRANSENT SET RECEIVINGMAX={item},RECSTATUS=0,RECPLNCTN={ctn},UPDDTE=current_timestamp WHERE RECEIVINGKEY='{transfer_out_no}'"

                Oracur.execute(sql_ent_insert)
                Oracur.execute(
                    f"DELETE FROM TXP_RECTRANSBODY WHERE RECEIVINGKEY='{transfer_out_no}' AND RECCTN=0")
                print(f"{txt_ent} RECEIVINGKEY={transfer_out_no}")

                # check part master
                seq = 0
                for p in x["receive_detail"]:
                    part_no = p["ledger"]["part"]["title"]
                    part_name = str(p["ledger"]["part"]
                                    ["description"]).replace("'", "''")

                    part_type = p["ledger"]["part_type"]["title"]
                    unit = p["ledger"]["unit"]["title"]
                    plan_qty = p["plan_qty"]
                    plan_ctn = 0
                    try:
                        plan_ctn = p["plan_ctn"]
                    except Exception as e:
                        print(e)
                        pass
                    sql_part = Oracur.execute(
                        f"SELECT PARTNO FROM TXP_PART WHERE PARTNO='{part_no}' AND TAGRP='{whs}'")
                    sql_part_insert = f"insert into txp_part(tagrp,partno,partname,carmaker,CD,TYPE,VENDORCD,UNIT,upddte,sysdte)values('{whs}','{part_no}','{part_name}','{whs}','{cd}','{part_type}','{factory}','{unit}',current_timestamp,current_timestamp)"
                    if sql_part.fetchone() != None:
                        sql_part_insert = f"UPDATE TXP_PART SET partname='{part_name}' WHERE PARTNO='{part_no}' AND TAGRP='{whs}'"

                    Oracur.execute(sql_part_insert)

                    # Check Ledger Master
                    sql_ledger = Oracur.execute(
                        f"SELECT PARTNO FROM TXP_LEDGER WHERE PARTNO='{part_no}' AND TAGRP='{whs}'")
                    sql_ledger_insert = f"""INSERT INTO TXP_LEDGER(PARTNO,TAGRP,MINIMUM,MAXIMUM,WHS,PICSHELFBIN,STKSHELFBIN,OVSSHELFBIN,OUTERPCS,UPDDTE, SYSDTE)VALUES('{part_no}','{whs}',0,0,'{whs_name}','PNON','SNON','ONON',0,current_timestamp,current_timestamp)"""
                    if sql_ledger.fetchone() is None:
                        Oracur.execute(sql_ledger_insert)
                    
                    ### Delete Body
                    rec_body = (Oracur.execute(
                        f"SELECT count(RECEIVINGKEY) FROM TXP_RECTRANSBODY WHERE RECEIVINGKEY='{transfer_out_no}' AND PARTNO='{part_no}'"
                    )).fetchone()[0]

                    sql_receive_body = f"UPDATE TXP_RECTRANSBODY SET PLNQTY='{plan_qty}',PLNCTN='{plan_ctn}' WHERE RECEIVINGKEY='{transfer_out_no}' AND PARTNO='{part_no}'"
                    if rec_body == 0:
                        rvm_no = get_rvn()
                        sql_receive_body = f"""INSERT INTO TXP_RECTRANSBODY(RECEIVINGKEY,RECEIVINGSEQ,PARTNO,PLNQTY,PLNCTN,RECQTY,RECCTN,TAGRP,UNIT,CD,WHS,DESCRI,RVMANAGINGNO,UPDDTE,SYSDTE,CREATEDBY,MODIFIEDBY,OLDERKEY)VALUES('{transfer_out_no}','{(seq + 1)}','{part_no}',{plan_qty},{plan_ctn},0,0,'{whs}','{unit}','{cd}','{whs_name}','{part_no}','{rvm_no}',sysdate, sysdate, 'SKTSYS', 'SKTSYS','{transfer_out_no}')"""

                    Oracur.execute(sql_receive_body)
                    print(f"{seq}. {transfer_out_no} PART: {part_no}")
                    seq += 1

                Oracon.commit()

                ### Update Receive Status
                payload='is_sync=true&is_active=true'
                response = requests.request("PUT", f"{api_host}/receive/ent/{receive_id}", headers=headers, data=payload)
                print(f"UPDATE {transfer_out_no} SET STATUS: {response.status_code}")
                _ctn = f"{int(ctn):,}"
                d = datetime.now()
                msg = f"""เปิดรอบ {whs_name}\nเลขที่: {transfer_out_no}\nจำนวน: {seq} กล่อง: {_ctn}\nวดป.: {d.strftime('%Y-%m-%d %H:%M:%S')}"""
                token = os.getenv("LINE_NOTIFICATION_DOM_TOKEN")
                if whs_name == "DOM":
                    token = os.getenv("LINE_NOTIFICATION_TOKEN")
                
                line_notification(token, msg)

        except Exception as ex:
            print(ex)
            pass

        ### Sync Order
        try:
            print(f"")
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


def merge_receive():
    try:
        d = datetime.now()
        for r in (Oracur.execute(
                f"SELECT e.GEDI_FILE FROM TXP_RECTRANSENT e WHERE e.RECEIVINGKEY LIKE 'TI%' GROUP BY e.GEDI_FILE HAVING COUNT(e.GEDI_FILE) > 1 ORDER BY e.GEDI_FILE"
        )).fetchall():
            batch_id = str(r[0])
            keys = []
            old_key = []
            for k in (Oracur.execute(
                    f"SELECT RECEIVINGKEY FROM TXP_RECTRANSENT WHERE GEDI_FILE='{batch_id}' ORDER BY RECEIVINGKEY"
            )).fetchall():
                keys.append(k[0])
                old_key.append(str(k[0])[len(str(k[0])) - 2:])

            unit = "BOX"
            part_type = "PART"
            cd = "20"
            whs = "C"
            rsstype = "01"
            whs_name = "CK-2"
            whs_type = "INJ"
            factory = "INJ"
            prefix = "SI" + (d.strftime("%Y%m%d"))[2:]
            if str(keys[0])[:3] == "TI1":
                whs_name = "CK-1"
                prefix = "SD" + (d.strftime("%Y%m%d"))[2:]
                whs = "D"
                whs_type = "DOM"
                rsstype = "05"

            q = (Oracur.execute(
                f"SELECT TO_CHAR(COUNT(*)+1, '00') FROM TXP_RECTRANSENT WHERE RECEIVINGKEY LIKE '{prefix}%'"
            )).fetchone()[0]
            merge_no = f"{prefix}{str(q).strip()}"

            receive_date = None
            seq = 0
            ctn = 0
            for b in (Oracur.execute(
                    f"""SELECT SUBSTR(e.RECEIVINGKEY, 1, 3) recname,e.VENDOR,e.RECEIVINGDTE,b.PARTNO,sum(b.PLNQTY) qty,sum(b.PLNCTN)  FROM TXP_RECTRANSENT e INNER JOIN TXP_RECTRANSBODY b ON e.RECEIVINGKEY=b.RECEIVINGKEY AND e.GEDI_FILE='{batch_id}' GROUP BY SUBSTR(e.RECEIVINGKEY, 1, 3),e.VENDOR,e.RECEIVINGDTE,b.PARTNO ORDER BY b.PARTNO"""
            )).fetchall():
                factory = b[1]
                receive_date = str(b[2])[:10]
                partno = b[3]
                plnqty = int(b[4])
                plnctn = int(b[5])

                k = ",".join(old_key)
                rvm_no = get_rvn()
                sql_receive_body = f"""INSERT INTO TXP_RECTRANSBODY(RECEIVINGKEY,RECEIVINGSEQ,PARTNO,PLNQTY,PLNCTN,RECQTY,RECCTN,TAGRP,UNIT,CD,WHS,DESCRI,RVMANAGINGNO,UPDDTE,SYSDTE,CREATEDBY,MODIFIEDBY,OLDERKEY)VALUES('{merge_no}','{seq}','{partno}',{plnqty},{plnctn},0,0,'{whs}','{unit}','{cd}','{whs_type}','{partno}','{rvm_no}',sysdate, sysdate, 'SKTSYS', 'SKTSYS','{k}')"""
                Oracur.execute(sql_receive_body)
                ctn += plnctn
                seq += 1

            # insert ent
            sql_ent_receive = f"""INSERT INTO TXP_RECTRANSENT(RECEIVINGKEY,RECEIVINGMAX,RECEIVINGDTE,VENDOR,RECSTATUS,RECISSTYPE,RECPLNCTN,RECENDCTN,UPDDTE,SYSDTE,GEDI_FILE)VALUES('{merge_no}',{seq},to_date('{str(receive_date)}', 'YYYY-MM-DD'),'{factory}',0,'{rsstype}',{ctn},0,current_timestamp,current_timestamp,'{batch_id}')"""
            Oracur.execute(sql_ent_receive)

            # after insert delete ent
            Oracur.execute(
                f"DELETE TXP_RECTRANSENT WHERE RECEIVINGKEY IN ({str(keys).replace('[', '').replace(']', '')})"
            )
            Oracur.execute(
                f"DELETE TXP_RECTRANSBODY WHERE RECEIVINGKEY IN ({str(keys).replace('[', '').replace(']', '')})"
            )

            receive_key = ",".join(old_key)
            _ctn = f"{ctn:,}"
            msg = f"""รวมรอบ INJ {whs_name}\nรอบ: {merge_no}\nจำนวน: {seq} กล่อง: {_ctn}\nรอบที่รวม: {receive_key}\nวดป.: {d.strftime('%Y-%m-%d %H:%M:%S')}"""
            token = os.getenv("LINE_NOTIFICATION_TOKEN")
            if whs_name == "CK-1":
                token = os.getenv("LINE_NOTIFICATION_DOM_TOKEN")

            line_notification(token, msg)

        Oracon.commit()
    except Exception as e:
        pass


if __name__ == "__main__":
    main()
    merge_receive()
    pool.release(Oracon)
    pool.close()
    sys.exit(0)
