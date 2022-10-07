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


def create_log(title, description, is_status):
    payload = f'title={title}&description={description}&is_active={str(is_status).lower()}'
    response = requests.request("POST", f"{api_host}/logs", headers={
                                'Content-Type': 'application/x-www-form-urlencoded'}, data=payload)
    print(f"create log {title} status: {response.status_code}")


def get_line_token(whs, fac="INJ"):
    response = requests.request(
        "GET", f"{api_host}/notify", headers={}, data={})
    obj = response.json()
    for i in obj["data"]:
        if i["whs"]["title"] == whs and i["factory"]["title"] == fac:
            return str(i["token"])


def line_notification(whs, msg):
    token = get_line_token(whs, fac="INJ")
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
        # create_log("LineNotify", f"{msg} status: {response.status_code}", True)
        return True
    except Exception as ex:
        print(ex)
        create_log("LineNotify error", f"{msg} err: {str(ex)}", False)
        pass

    return False


def get_rvn():
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

    pool.release(Oracon)
    pool.close()
    return rvm_no


def main():
    headers = None
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
        print(f"sign in is: {token}")
        create_log("User Authorization", f"{api_user} is success", True)
    except Exception as e:
        print(e)
        create_log("User Authorization",
                   f"{api_user} is error {str(e)}", False)
        pass

    return headers


def get_mailbox(headers):
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
                                    create_log(
                                        "Download EDI", f"{file_name} download is success", True)
                                    print(f"download file: {file_name}")
                                    time.sleep(5)
                            i += 1
    except Exception as ex:
        print(f"get mail box: {str(ex)}")
        create_log("Download EDI", f"{file_name} download is {str(ex)}", False)
        pass


def upload_edi(header):
    try:
        headers = {'Authorization': header["Authorization"]},
        list_dir = os.listdir(source_dir)
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
                                                    headers=headers[0],
                                                    data=payload,
                                                    files=files)
                        f.close()
                        print(
                            f"{rn} ==> upload file edi: {batch_name} status: {response.status_code}")
                        create_log("Upload EDI To SPL CLOUD",
                                   f"{batch_name} upload is success", True)
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
        print(f"upload_edi error: %s" % ex)
        create_log("Upload EDI To SPL CLOUD",
                   f"{batch_name} upload is error {str(ex)}", False)
        pass


def sync_receive(headers):
    # Sync Receive
    try:
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

                # Delete Body
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
            # Update Receive Status
            payload = 'is_sync=true&is_active=true'
            response = requests.request(
                "PUT", f"{api_host}/receive/ent/{receive_id}", headers=headers, data=payload)
            print(
                f"UPDATE {transfer_out_no} SET STATUS: {response.status_code}")
            _ctn = f"{int(ctn):,}"
            d = datetime.now()
            create_log(
                "Sync Receive", f"""{whs_name} NO: {transfer_out_no} ITEM: {seq} CTN: {_ctn} Date: {d.strftime('%Y-%m-%d %H:%M:%S')}""", True)
            msg = f"""เปิดรอบ {whs_name}\nเลขที่: {transfer_out_no}\nจำนวน: {seq} กล่อง: {_ctn}\nวดป.: {d.strftime('%Y-%m-%d %H:%M:%S')}"""
            line_notification(whs_name, msg)

        pool.release(Oracon)
        pool.close()
    except Exception as ex:
        print(f"Error Sync Receiver: {ex}")
        create_log("Sync Receive",
                   f"""{transfer_out_no} is error {str(ex)}""", False)
        pass


def sync_order(headers):
    # Sync Order
    try:
        response = requests.request(
            "GET", f"{api_host}/order/ent", headers=headers, data={})
        data = response.json()["data"]
        seq_ord = 1
        for ord in data:
            # print(ord)
            id = ord["id"]
            whs = ord["consignee"]["whs"]["title"]
            cmaker = ord["consignee"]["whs"]["description"]
            factory = ord["consignee"]["factory"]["title"]
            inv_prefix = ord["consignee"]["factory"]["inv_prefix"]
            label_prefix = ord["consignee"]["factory"]["label_prefix"]
            shiptype = ord["shipment"]["title"]
            affcode = ord["consignee"]["affcode"]["title"]
            pc = ord["pc"]["title"]
            commercial = ord["commercial"]["title"]
            sampflg = ord["sample_flg"]["title"]
            order_title = ord["order_title"]["title"]
            etdtap = str(ord["etd_date"])[:10]
            bioat = ord["bioat"]
            bishpc = ord["consignee"]["customer"]["title"]
            biivpx = ord["commercial"]["prefix"]  # ord["consignee"]["prefix"]
            bisafn = ord["consignee"]["customer"]["description"]
            ship_form = ord["ship_form"]
            ship_to = ord["ship_to"]
            loading_area = ord["loading_area"]
            privilege = ord["privilege"]
            zone_code = ord["zone_code"]
            running_seq = int(ord["running_seq"])
            if ord["commercial"]["prefix"] == "-":
                biivpx = ord["consignee"]["prefix"]

            inv_no = f"{inv_prefix}{biivpx}{etdtap[3:4]}{running_seq:04d}{shiptype}"
            ref_inv = f"{biivpx}-{str(etdtap).replace('-', '')}-{running_seq:04d}"

            # print(f"factory={factory} inv_prefix={inv_prefix} label_prefix={label_prefix} shiptype={shiptype} affcode={affcode} pc={pc} commercial={commercial} sampflg={sampflg} order_title={order_title} etdtap={etdtap} bioat={bioat} bishpc={bishpc} biivpx={biivpx} bisafn={bisafn} ship_form={ship_form} ship_to={ship_to} loading_area={loading_area} privilege={privilege} zone_code={zone_code} running_seq={running_seq} ")
            print(f"----------------------------------------------------------------")
            print(f"{seq_ord}. {etdtap} INV: {inv_no} REF: {ref_inv} ==> {id}")
            seq = 1
            for b in ord["order_detail"]:
                order_id = b["id"]
                carriercode = b["orderplan"]["carrier_code"]
                ordertype = b["orderplan"]["order_type"]["title"]
                pono = b["pono"]
                partno = b["ledger"]["part"]["title"]
                partname = b["ledger"]["part"]["description"]
                part_type = b["ledger"]["part_type"]["title"]
                unit = b["ledger"]["unit"]["title"]
                ordermonth = b["orderplan"]["ordermonth"]
                orderorgi = b["orderplan"]["orderorgi"]
                orderround = b["orderplan"]["orderround"]
                balqty = b["orderplan"]["balqty"]
                shippedflg = b["orderplan"]["shipped_flg"]
                shippedqty = b["orderplan"]["shipped_qty"]
                upddte = b["orderplan"]["updtime"]
                allocateqty = 0
                bidrfl = b["orderplan"]["bidrfl"]
                deleteflg = b["orderplan"]["delete_flg"]
                reasoncd = b["orderplan"]["reasoncd"]
                firmflg = b["orderplan"]["firm_flg"]
                bicomd = b["orderplan"]["bicomd"]
                bistdp = b["orderplan"]["bistdp"]
                binewt = b["orderplan"]["binewt"]
                bigrwt = b["orderplan"]["bigrwt"]
                bileng = b["orderplan"]["bileng"]
                biwidt = b["orderplan"]["biwidt"]
                bihigh = b["orderplan"]["bihigh"]
                curinv = "-"
                oldinv = ""
                sysdte = b["orderplan"]["updtime"]
                poupdflag = ""
                createdby = "SKTSYS"
                modifiedby = "SKTSYS"
                lotno = b["orderplan"]["lotno"]
                orderstatus = 0
                print(f"{seq}. ORDER DETAIL: {order_id}")

                seq += 1

            # Update Order Status Sync
            payload = 'is_sync=true&is_active=true'
            response = requests.request(
                "PUT", f"{api_host}/order/ent/{id}", headers=headers, data=payload)
            print(f"UPDATE STATUS SYNC: {response.status_code}")
            seq_ord += 1

    except Exception as ex:
        print(ex)
        pass


def sign_out(headers):
    try:
        # logout
        response = requests.request(
            "GET", f"{api_host}/auth/logout", headers=headers, data={})
        print(f"sign out is: {response.status_code}")
        create_log("User Authorization", f"""SignOut is success""", True)
    except Exception as e:
        print(e)
        create_log("User Authorization",
                   f"""SignOut is error {str(e)}""", False)
        pass


def merge_receive():
    try:
        d = datetime.now()
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
            line_notification(whs, msg)
            create_log(
                "Merge Receive", f"""{whs_name} No: {merge_no} Item: {seq} CTN: {_ctn} Merge: {receive_key} Date: {d.strftime('%Y-%m-%d %H:%M:%S')}""", True)

        Oracon.commit()
        pool.release(Oracon)
        pool.close()
    except Exception as e:
        create_log("Merge Receive", str(e), False)
        pass


def move_to_group(whs, tagrp):
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
    mvTagrp = "D"
    if tagrp == "D":
        mvTagrp = "C"

    for i in (Oracur.execute(
            f"SELECT RUNNINGNO FROM TXP_CARTONDETAILS WHERE SHELVE='{whs}' AND TAGRP='{tagrp}'"
    )).fetchall():
        serail_no = str(i[0])
        for x in (Oracur.execute(
                f"SELECT TAGRP,PARTNO,LOTNO,RUNNINGNO,CASEID,CASENO,STOCKQUANTITY,SHELVE,PALLETKEY,(SELECT SYS_CONTEXT('USERENV','IP_ADDRESS') FROM dual),'SYSTEM',invoiceno FROM TXP_CARTONDETAILS WHERE RUNNINGNO='{str(i[0])}'"
        )).fetchall():
            tagrp = str(x[0])
            partno = str(x[1])
            txt = "DOM"
            if tagrp == "C":
                txt = "COM"

            # check master part
            # print(f"MASTER PART PARTNO='{partno}' TAGRP='{mvTagrp}'")
            part = (Oracur.execute(
                f"SELECT count(PARTNO) FROM TXP_PART WHERE PARTNO='{partno}' AND TAGRP='{mvTagrp}'"
            )).fetchone()[0]
            if part == 0:
                try:
                    Oracur.execute(
                        f"insert into txp_part(tagrp,partno,partname,carmaker,CD,TYPE,VENDORCD,UNIT,upddte,sysdte)values('{mvTagrp}','{partno}','{partno}','{txt}','02','PART','{txt}','BOX',current_timestamp,current_timestamp)"
                    )
                except Exception as e:
                    print(e)
                    pass
            # check master ledger
            # print(f"MASTER LEDGER PARTNO='{partno}' TAGRP='{mvTagrp}'")
            ledger = (Oracur.execute(
                f"SELECT count(PARTNO) FROM TXP_LEDGER WHERE PARTNO='{partno}' AND TAGRP='{mvTagrp}'"
            )).fetchone()[0]
            if ledger == 0:
                try:
                    Oracur.execute(
                        f"""INSERT INTO TXP_LEDGER(PARTNO,TAGRP,MINIMUM,MAXIMUM,WHS,PICSHELFBIN,STKSHELFBIN,OVSSHELFBIN,OUTERPCS,UPDDTE, SYSDTE)VALUES('{partno}','{mvTagrp}',0,0,'{txt}','PNON','SNON','ONON',0,current_timestamp,current_timestamp)"""
                    )
                except Exception as e:
                    print(e)
                    pass

            # Oracur.execute(
            #     f"CALL SEND_CARTON_TRIGGER('{tagrp}','{partno}','{lotno}','{runningno}','{caseid}','{caseno}',{qty},'{shelve}','{palletkey}', '{ipaddres}', 'MOVE TO {txt}', '{issueno}')"
            # )
            Oracur.execute(
                f"UPDATE TXP_CARTONDETAILS SET TAGRP='{mvTagrp}' WHERE RUNNINGNO='{serail_no}'"
            )
            create_log(
                f"Move Whs", f"Move Part {serail_no} to {mvTagrp}", True)

    Oracon.commit()
    pool.release(Oracon)
    pool.close()


def move_whs():
    # move ck2 to ck1
    move_to_group('S-CK1', 'C')
    # move ck1 to ck2
    move_to_group('S-CK2', 'D')


if __name__ == "__main__":
    headers = main()
    if headers != None:
        get_mailbox(headers)
        upload_edi(headers)
        sync_receive(headers)
        # sync_order(headers)
        sign_out(headers)

    merge_receive()
    move_whs()
    sys.exit(0)
