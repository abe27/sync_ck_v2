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


def upload_inv(headers):
    try:
        list_dir = os.listdir("data/invoice")
        list_dir.sort()
        # x = 0
        for dir in list_dir:
            filePath = f"data/invoice/{dir}"
            f = open(filePath, 'rb')
            files = [('file', (dir, f, 'application/octet-stream'))]
            response = requests.request("POST", f"{api_host}/upload/invoice/tap", headers={
                                        'Authorization': headers["Authorization"]}, data={}, files=files)
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

def sync_orderplan(headers):
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
            "PATCH", f"{api_host}/order/plan", headers=headers, data={})
        data = response.json()["data"]
        seq = 1
        for ord in data:
            order_id = ord["id"]
            carriercode = ord["carrier_code"]
            ordertype = ord["order_type"]["title"]
            pono = ord["pono"]
            partno = ord["ledger"]["part"]["title"]
            partname = ord["ledger"]["part"]["description"]
            pname = (partname).replace("'", "''")
            part_type = ord["ledger"]["part_type"]["title"]
            unit = ord["ledger"]["unit"]["title"]
            ordermonth = str(ord["ordermonth"])[:10]
            orderorgi = ord["orderorgi"]
            orderround = ord["orderround"]
            balqty = ord["balqty"]
            shippedflg = ord["shipped_flg"]
            shippedqty = ord["shipped_qty"]
            bidrfl = ord["bidrfl"]
            deleteflg = ord["delete_flg"]
            reasoncd = ord["reasoncd"]
            firmflg = ord["firm_flg"]
            bicomd = ord["bicomd"]
            bistdp = ord["bistdp"]
            binewt = ord["binewt"]
            bigrwt = ord["bigrwt"]
            bileng = ord["bileng"]
            biwidt = ord["biwidt"]
            bihigh = ord["bihigh"]
            poupdflag = ""
            lotno = ord["lotno"]
            fac_cd_code = ord["consignee"]["factory"]["cd_code"]
            whs = ord["consignee"]["whs"]["title"]
            cmaker = ord["consignee"]["whs"]["description"]
            if ord["consignee"]["whs"]["title"] == "N" or ord["consignee"]["whs"]["title"] == "I":
                whs = "COM"
                cmaker = "C"

            factory = ord["vendor"]
            etdtap = str(ord["etd_tap"])[:10]
            shiptype = ord["shipment"]["title"]
            affcode = ord["biac"]
            bishpc = ord["bishpc"]
            pc = ord["pc"]["title"]
            commercial = ord["commercial"]["title"]
            bioabt = ord["bioabt"]
            bishpc = ord["bishpc"]
            biivpx = ord["biivpx"]
            bisafn = ord["bisafn"]
            sampflg = ord["sample_flg"]
            # print(f"{seq}. ORDER DETAIL: {order_id}")
            # Create Part Master
            Oracur.execute(
                f"SELECT rowid FROM TXP_PART WHERE PARTNO='{partno}' AND TAGRP='{cmaker}'")
            if Oracur.fetchone() is None:
                try:
                    Oracur.execute(
                        f"insert into txp_part(tagrp,partno,partname,carmaker,CD,TYPE,VENDORCD,UNIT,upddte,sysdte)values('{cmaker}','{partno}','{pname}','{cmaker}','{fac_cd_code}','{part_type}','{whs}','{unit}',current_timestamp,current_timestamp)"
                    )
                except Exception as e:
                    print(e)
                    pass
            # Create Master Ledger
            Oracur.execute(
                f"SELECT rowid FROM TXP_LEDGER WHERE PARTNO='{partno}' AND TAGRP='{cmaker}'")
            if Oracur.fetchone() is None:
                try:
                    Oracur.execute(
                        f"""INSERT INTO TXP_LEDGER(PARTNO,TAGRP,MINIMUM,MAXIMUM,WHS,PICSHELFBIN,STKSHELFBIN,OVSSHELFBIN,OUTERPCS,UPDDTE, SYSDTE)VALUES('{partno}','{cmaker}',0,0,'{whs}','PNON','SNON','ONON',0,current_timestamp,current_timestamp)"""
                    )
                except Exception as e:
                    print(e)
                    pass
            # check Orderplan
            sql_orderplan = f"SELECT rowid FROM TXP_ORDERPLAN WHERE FACTORY='{factory}' AND TO_CHAR(ETDTAP,'YYYY-MM-DD') ='{etdtap}' AND SHIPTYPE='{shiptype}' AND AFFCODE='{affcode}' AND BISHPC='{bishpc}' AND PC='{pc}' AND COMMERCIAL='{commercial}' AND PONO='{pono}' AND PARTNO='{partno}'"
            Oracur.execute(sql_orderplan)
            sql_insert_orderplan = f"""INSERT INTO TXP_ORDERPLAN(FACTORY, SHIPTYPE, AFFCODE, PONO, ETDTAP, PARTNO, PARTNAME, ORDERMONTH, ORDERORGI, ORDERROUND, BALQTY, SHIPPEDFLG, SHIPPEDQTY, PC, COMMERCIAL, SAMPFLG, CARRIERCODE, ORDERTYPE, UPDDTE, ALLOCATEQTY, BIDRFL, DELETEFLG, REASONCD, BIOABT, FIRMFLG, BICOMD, BISTDP, BINEWT, BIGRWT, BISHPC, BIIVPX, BISAFN, BILENG, BIWIDT, BIHIGH, CURINV, OLDINV, SYSDTE, POUPDFLAG, CREATEDBY, MODIFIEDBY, LOTNO, ORDERSTATUS, ORDERID)VALUES('{factory}', '{shiptype}', '{affcode}', '{pono}', TO_DATE('{etdtap}','YYYY-MM-DD'), '{partno}', '{pname}', TO_DATE('{ordermonth}','YYYY-MM-DD'), {orderorgi}, {orderround}, {balqty}, '{shippedflg}', {shippedqty}, '{pc}', '{commercial}', '{sampflg}', '{carriercode}', '{ordertype}', current_timestamp, 0, '{bidrfl}', '{deleteflg}', '{reasoncd}', '{bioabt}', '{firmflg}', '{bicomd}', {bistdp}, {binewt}, {bigrwt}, '{bishpc}', '{biivpx}', '{bisafn}', {bileng}, {biwidt}, {bihigh}, null, null, current_timestamp, '{poupdflag}', 'SKTSYS', 'SKTSYS', '{lotno}', 0, '{order_id}')"""
            rowid = Oracur.fetchone()
            if rowid != None:
                sql_insert_orderplan = f"""UPDATE TXP_ORDERPLAN SET BALQTY='{balqty}',SAMPFLG='{sampflg}', CARRIERCODE='{carriercode}', ORDERTYPE='{ordertype}', UPDDTE=current_timestamp,DELETEFLG='{deleteflg}', REASONCD='{reasoncd}', BINEWT='{binewt}', BIGRWT='{bigrwt}', CURINV=null, OLDINV=null, POUPDFLAG='{poupdflag}', MODIFIEDBY='SKTSYS', LOTNO='{lotno}', ORDERSTATUS=0, ORDERID='{order_id}' WHERE ROWID='{rowid[0]}'"""
            Oracur.execute(sql_insert_orderplan)
            Oracur.execute(sql_orderplan)
            rowid = Oracur.fetchone()[0]
            payload = f'row_id={rowid}&is_sync=true&is_active=true'
            response = requests.request("PUT", f"{api_host}/order/plan/{order_id}", headers=headers, data=payload)
            print(f"UPDATE STATUS SYNC: {response.status_code}")
            seq += 1

        Oracon.commit()
        pool.release(Oracon)
        pool.close()
    except Exception as ex:
        print(str(ex))
        pass


def sync_order(headers):
    # Sync Order
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
            "GET", f"{api_host}/sync/order", headers=headers, data={})
        data = response.json()["data"]
        seq_ord = 1
        d = datetime.now()
        create_log("Start Sync Order",
                   f"Sync {len(data)} Running on: {d.strftime('%Y-%m-%d %H:%M:%S')}", True)
        for ord in data:
            # print(ord)
            id = ord["id"]
            whs = ord["consignee"]["whs"]["title"]
            cmaker = "C"#ord["consignee"]["whs"]["description"]
            factory = ord["consignee"]["factory"]["title"]
            fac_cd_code = ord["consignee"]["factory"]["cd_code"]
            inv_prefix = ord["consignee"]["factory"]["inv_prefix"]
            label_prefix = ord["consignee"]["factory"]["label_prefix"]
            shiptype = ord["shipment"]["title"]
            affcode = ord["consignee"]["affcode"]["title"]
            pc = ord["pc"]["title"]
            commercial = ord["commercial"]["title"]
            sampflg = ord["sample_flg"]["title"]
            order_title = ord["order_title"]["title"]
            etdtap = str(ord["etd_date"])[:10]
            bioabt = ord["bioabt"]
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
            ref_inv = f"S{biivpx}-{str(str(etdtap).replace('-', ''))}-{running_seq:04d}"

            # print(f"factory={factory} inv_prefix={inv_prefix} label_prefix={label_prefix} shiptype={shiptype} affcode={affcode} pc={pc} commercial={commercial} sampflg={sampflg} order_title={order_title} etdtap={etdtap} bioabt={bioabt} bishpc={bishpc} biivpx={biivpx} bisafn={bisafn} ship_form={ship_form} ship_to={ship_to} loading_area={loading_area} privilege={privilege} zone_code={zone_code} running_seq={running_seq} ")
            print(f"----------------------------------------------------------------")
            print(f"🐒{seq_ord}. {etdtap} INV: {inv_no} REF: {ref_inv} ==> {id}🐒")
            seq = 1
            for b in ord["order_detail"]:
                order_id = b["id"]
                carriercode = b["orderplan"]["carrier_code"]
                ordertype = b["orderplan"]["order_type"]["title"]
                pono = b["pono"]
                partno = b["ledger"]["part"]["title"]
                partname = b["ledger"]["part"]["description"]
                pname = (partname).replace("'", "''")
                part_type = b["ledger"]["part_type"]["title"]
                unit = b["ledger"]["unit"]["title"]
                ordermonth = str(b["orderplan"]["ordermonth"])[:10]
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
                # print(f"{seq}. ORDER DETAIL: {order_id}")

                # Create Part Master
                Oracur.execute(
                    f"SELECT rowid FROM TXP_PART WHERE PARTNO='{partno}' AND TAGRP='{cmaker}'")
                if Oracur.fetchone() is None:
                    try:
                        Oracur.execute(
                            f"insert into txp_part(tagrp,partno,partname,carmaker,CD,TYPE,VENDORCD,UNIT,upddte,sysdte)values('{cmaker}','{partno}','{pname}','{cmaker}','{fac_cd_code}','{part_type}','{whs}','{unit}',current_timestamp,current_timestamp)"
                        )
                    except Exception as e:
                        print(e)
                        pass
                # Create Master Ledger
                Oracur.execute(
                    f"SELECT rowid FROM TXP_LEDGER WHERE PARTNO='{partno}' AND TAGRP='{cmaker}'")
                if Oracur.fetchone() is None:
                    try:
                        Oracur.execute(
                            f"""INSERT INTO TXP_LEDGER(PARTNO,TAGRP,MINIMUM,MAXIMUM,WHS,PICSHELFBIN,STKSHELFBIN,OVSSHELFBIN,OUTERPCS,UPDDTE, SYSDTE)VALUES('{partno}','{cmaker}',0,0,'{whs}','PNON','SNON','ONON',0,current_timestamp,current_timestamp)"""
                        )
                    except Exception as e:
                        print(e)
                        pass

                # check Orderplan
                sql_orderplan = f"SELECT rowid FROM TXP_ORDERPLAN WHERE FACTORY='{factory}' AND TO_CHAR(ETDTAP,'YYYY-MM-DD') ='{etdtap}' AND SHIPTYPE='{shiptype}' AND AFFCODE='{affcode}' AND BISHPC='{bishpc}' AND PC='{pc}' AND COMMERCIAL='{commercial}' AND PONO='{pono}' AND PARTNO='{partno}'"
                Oracur.execute(sql_orderplan)
                sql_insert_orderplan = f"""INSERT INTO TXP_ORDERPLAN(FACTORY, SHIPTYPE, AFFCODE, PONO, ETDTAP, PARTNO, PARTNAME, ORDERMONTH, ORDERORGI, ORDERROUND, BALQTY, SHIPPEDFLG, SHIPPEDQTY, PC, COMMERCIAL, SAMPFLG, CARRIERCODE, ORDERTYPE, UPDDTE, ALLOCATEQTY, BIDRFL, DELETEFLG, REASONCD, BIOABT, FIRMFLG, BICOMD, BISTDP, BINEWT, BIGRWT, BISHPC, BIIVPX, BISAFN, BILENG, BIWIDT, BIHIGH, CURINV, OLDINV, SYSDTE, POUPDFLAG, CREATEDBY, MODIFIEDBY, LOTNO, ORDERSTATUS, ORDERID)VALUES('{factory}', '{shiptype}', '{affcode}', '{pono}', TO_DATE('{etdtap}','YYYY-MM-DD'), '{partno}', '{pname}', TO_DATE('{ordermonth}','YYYY-MM-DD'), {orderorgi}, {orderround}, {balqty}, '{shippedflg}', {shippedqty}, '{pc}', '{commercial}', '{sampflg}', '{carriercode}', '{ordertype}', current_timestamp, 0, '{bidrfl}', '{deleteflg}', '{reasoncd}', '{bioabt}', '{firmflg}', '{bicomd}', {bistdp}, {binewt}, {bigrwt}, '{bishpc}', '{biivpx}', '{bisafn}', {bileng}, {biwidt}, {bihigh}, '{ref_inv}', '{inv_no}', current_timestamp, '{poupdflag}', 'SKTSYS', 'SKTSYS', '{lotno}', 0, '{order_id}')"""
                rowid = Oracur.fetchone()
                txt = "INSERT"
                if rowid != None:
                    txt = "UPDATE"
                    sql_insert_orderplan = f"""UPDATE TXP_ORDERPLAN SET BALQTY='{balqty}',SAMPFLG='{sampflg}', CARRIERCODE='{carriercode}', ORDERTYPE='{ordertype}', UPDDTE=current_timestamp,DELETEFLG='{deleteflg}', REASONCD='{reasoncd}', BINEWT='{binewt}', BIGRWT='{bigrwt}', CURINV='{inv_no}', OLDINV='{ref_inv}', POUPDFLAG='{poupdflag}', MODIFIEDBY='SKTSYS', LOTNO='{lotno}', ORDERSTATUS=0, ORDERID='{order_id}' WHERE ROWID='{rowid[0]}'"""

                Oracur.execute(sql_insert_orderplan)
                Oracur.execute(sql_orderplan)
                rowid = Oracur.fetchone()[0]
                # Create Invoice
                Oracur.execute(
                    f"SELECT rowid FROM TXP_ISSTRANSENT WHERE ISSUINGKEY='{ref_inv}'")
                invData = Oracur.fetchone()
                sql_insert_inv = f"""INSERT INTO TXP_ISSTRANSENT(ISSUINGKEY, ETDDTE, FACTORY, AFFCODE, BISHPC, CUSTNAME, COMERCIAL, ZONEID, SHIPTYPE, COMBINV, SHIPDTE, PC, SHIPFROM, ZONECODE, NOTE1, NOTE2, ISSUINGMAX, ISSUINGSTATUS, RECISSTYPE,UPDDTE, SYSDTE, UUID, CREATEDBY, MODIFIEDBY, REFINVOICE)VALUES('{ref_inv}', TO_DATE('{etdtap}','YYYY-MM-DD'), '{factory}', '{affcode}', '{bishpc}', '{bisafn}', '{commercial}', '{bioabt}', '{shiptype}', '{ordertype}', TO_DATE('{etdtap}','YYYY-MM-DD'), '{pc}', '{ship_form}', '{zone_code}', '{loading_area}', '{privilege}',  {seq}, 0, '{fac_cd_code}',current_timestamp, current_timestamp, '{str(uuid4())}', 'SKTSYS', 'SKTSYS','{inv_no}')"""
                if invData != None:
                    sql_insert_inv = f"UPDATE TXP_ISSTRANSENT SET ISSUINGKEY='{ref_inv}', COMERCIAL='{commercial}', ZONEID='{bioabt}', SHIPTYPE='{shiptype}', PC='{pc}', SHIPFROM='{ship_form}', ZONECODE='{zone_code}', ISSUINGMAX='{seq}', ISSUINGSTATUS=0, UPDDTE=current_timestamp, MODIFIEDBY='SKTSYS', REFINVOICE='{inv_no}' WHERE ROWID='{invData[0]}'"

                # print(sql_insert_inv)
                Oracur.execute(sql_insert_inv)

                # Create Invoice Detail
                Oracur.execute(
                    f"SELECT rowid FROM TXP_ISSTRANSBODY WHERE ISSUINGKEY='{ref_inv}' AND PONO='{pono}' AND PARTNO='{partno}'")
                orderDetail = Oracur.fetchone()
                sql_ins_detail = f"""INSERT INTO TXP_ISSTRANSBODY(ISSUINGKEY, ISSUINGSEQ, PONO, TAGRP, PARTNO, STDPACK, ORDERQTY, ISSUINGSTATUS, BWIDE, BLENG, BHIGHT, NEWEIGHT, GTWEIGHT, UPDDTE, SYSDTE, PARTTYPE, PARTNAME, SHIPTYPE, EDTDTE, UUID, CREATEDBY, MODIFIEDBY, ORDERTYPE, LOTNO, ORDERID, REFINV)VALUES('{ref_inv}', {seq}, '{pono}', '{cmaker}', '{partno}', {bistdp}, {balqty}, 0, {biwidt}, {bileng}, {bihigh}, {binewt},{bigrwt}, current_timestamp, current_timestamp, '{whs[:1]}', '{pname}', '{shiptype}', TO_DATE('{etdtap}','YYYY-MM-DD'), '{str(uuid4())}', 'SKTSYS', 'SKTSYS', '{ordertype}', '{lotno}', '{order_id}', '{ref_inv}')"""
                if orderDetail != None:
                    sql_ins_detail = f"""UPDATE TXP_ISSTRANSBODY SET ORDERQTY='{balqty}',NEWEIGHT='{binewt}',GTWEIGHT='{bigrwt}',UPDDTE=current_timestamp,LOTNO='{lotno}' WHERE ROWID='{orderDetail[0]}'"""

                Oracur.execute(sql_ins_detail)
                print(f"👌{seq}::::{rowid}:::{txt} ==> {inv_no} ID: {order_id}👌")
                seq += 1

            # Create PalletNo
            list_pallet = ord["pallet"]
            for pallet in list_pallet:
                pallet_prefix = pallet["pallet_prefix"]
                pallet_seq = pallet["pallet_no"]
                limit_total = len(pallet["pallet_detail"])

                dim_width = pallet["pallet_type"]["pallet_size_width"]
                dim_length = pallet["pallet_type"]["pallet_size_length"]
                dim_hight = pallet["pallet_type"]["pallet_size_hight"]
                if pallet_prefix == "C":
                    dim_width = pallet["pallet_type"]["box_size_width"]
                    dim_length = pallet["pallet_type"]["box_size_length"]
                    dim_hight = pallet["pallet_type"]["box_size_hight"]

                pallet_no = f"{pallet_prefix}{int(pallet_seq):03d}"

                # Create Invoice Pallet
                Oracur.execute(
                    f"SELECT ROWID FROM TXP_ISSPALLET WHERE ISSUINGKEY='{ref_inv}' AND PALLETNO='{pallet_no}'")
                plData = Oracur.fetchone()
                sql_pl_insert = f"""INSERT INTO TXP_ISSPALLET(FACTORY, ISSUINGKEY, PALLETNO, CUSTNAME, PLTYPE, PLOUTSTS, UPDDTE, SYSDTE, PLTOTAL,PLWIDE, PLLENG, PLHIGHT)VALUES('{factory}','{ref_inv}','{pallet_no}','{bisafn}','{pallet_prefix}',0, current_timestamp,current_timestamp,{limit_total},{dim_width},{dim_length},{dim_hight})"""
                if plData != None:
                    sql_pl_insert = f"UPDATE TXP_ISSPALLET SET UPDDTE=current_timestamp,PLTOTAL={limit_total} WHERE ROWID='{plData[0]}'"

                Oracur.execute(sql_pl_insert)
                pallet_detail = pallet["pallet_detail"]
                pl_seq = 1
                for p in pallet_detail:
                    pono = p["order_detail"]["pono"]
                    partno = p["order_detail"]["ledger"]["part"]["title"]
                    fticket_no = (
                        f'{label_prefix}{str(etdtap[3:4])}{int(p["seq_no"]) + 1:08d}')
                    print(f"{pl_seq}. PALLETNO: {pallet_no} FTICK: {fticket_no}")
                    Oracur.execute(
                        f"SELECT ROWID FROM TXP_ISSPACKDETAIL WHERE FTICKETNO='{fticket_no}'")
                    ftData = Oracur.fetchone()
                    sql_fticket = f"""INSERT INTO TXP_ISSPACKDETAIL(ISSUINGKEY, PONO, TAGRP, PARTNO, FTICKETNO, SHIPPLNO,ISSUINGSTATUS, UPDDTE, SYSDTE, UUID, CREATEDBY, MODIFEDBY)VALUES('{ref_inv}', '{pono}', '{cmaker}', '{partno}', '{fticket_no}', '{pallet_no}', 0,current_timestamp, current_timestamp, '{p['id']}', 'SKTSYS', 'SKTSYS')"""
                    if ftData != None:
                        sql_fticket = f"UPDATE TXP_ISSPACKDETAIL SET UPDDTE=current_timestamp WHERE ROWID='{ftData[0]}'"

                    Oracur.execute(sql_fticket)
                    pl_seq += 1

            # Update Order Status Sync
            payload = 'is_sync=true&is_active=true'
            response = requests.request(
                "PUT", f"{api_host}/order/ent/{id}", headers=headers, data=payload)
            print(f"UPDATE STATUS SYNC: {response.status_code}")
            seq_ord += 1
            Oracon.commit()

        d = datetime.now()
        create_log("End Sync Order",
                   f" At: {d.strftime('%Y-%m-%d %H:%M:%S')}", True)
        pool.release(Oracon)
        pool.close()
    except Exception as ex:
        print(ex)
        create_log("Error Sync Order", f"Error with: {str(ex)}", False)
        pass


def upload_receive_excel(headers):
    try:
        list_dir = "data/receive"
        for dir in os.listdir(list_dir):
            filePath = f"data/receive/{dir}"
            f = open(filePath, 'rb')
            files = [('file', (dir, f, 'application/octet-stream'))]
            response = requests.request("POST", f"{api_host}/upload/receive", headers={
                                        'Authorization': headers["Authorization"]}, data={}, files=files)
            f.close()
            shutil.move(filePath, f"data/excels/{dir}")
            create_log("Upload Receive Excel",
                       f"""{dir} is success {response.status_code}""", True)
    except Exception as e:
        print(e)
        create_log("Upload Receive Excel", f"""Error: {str(e)}""", False)
        pass


def check_receive_carton():
    response = requests.request(
        "GET", f"{api_host}/receive/notscan", headers={}, data={})
    if response.status_code == 200:
        obj = response.json()["data"]
        for i in obj:
            url = f"http://127.0.0.1:4000/carton/search?serial_no={i['serial_no']}"
            payload = {}
            headers = {}
            response = requests.request(
                "GET", url, headers=headers, data=payload)
            if response.status_code == 302:
                # update status
                payload = f"transfer_out_no={i['transfer_out_no']}&part_no={i['part_no']}&is_sync=true"
                headers = {
                    'Content-Type': 'application/x-www-form-urlencoded'
                }
                response = requests.request(
                    "PUT", f"{api_host}/receive/notscan/{i['id']}", headers=headers, data=payload)
                print(f"update id: {i['id']} is :{response.status_code}")


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


def update_reset_stock():
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
    Oracur.execute(
        "UPDATE TXP_CARTONDETAILS SET SIDTE=NULL,SINO=NULL,SIID=NULL WHERE SHELVE='SNON' AND SIDTE IS NOT NULL")
    Oracon.commit()
    pool.release(Oracon)
    pool.close()


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
            whs_code = "COM"
            rsstype = "01"
            whs_name = "CK-2"
            whs_type = "INJ"
            factory = "INJ"
            prefix = "SI" + (d.strftime("%Y%m%d"))[2:]
            if str(keys[0])[:3] == "TI1":
                whs_name = "CK-1"
                prefix = "SD" + (d.strftime("%Y%m%d"))[2:]
                whs = "D"
                whs_code = "DOM"
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
            line_notification(whs_code, msg)
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


def export_check_inv(headers):
    try:
        dt = (datetime.now() + timedelta(days=7))
        start = dt - timedelta(days=dt.weekday())
        end = start + timedelta(days=6)
        print(
            f"start: {start.strftime('%Y-%m-%d')} to: {end.strftime('%Y-%m-%d')}")
        response = requests.request(
            "GET", f"{api_host}/order/ent?start_etd={start.strftime('%Y-%m-%d')}&to_etd={end.strftime('%Y-%m-%d')}", headers=headers, data={})
        data = response.json()["data"]

        file_name = f"{start.strftime('%Y%m%d')}-{end.strftime('%Y%m%d')}.xlsx"
        source_dir_path = f"export/invoice"
        if (os.path.exists(source_dir_path)) == False:
            os.makedirs(source_dir_path)
        print(f"create file {file_name} on {source_dir_path}")
        workbook = xlsxwriter.Workbook(f"{source_dir_path}/{file_name}")
        worksheet = workbook.add_worksheet()
        worksheet.write('A1', 'ID')
        worksheet.write('B1', 'WHS')
        worksheet.write('C1', 'INVOICE')
        worksheet.write('D1', 'ETD')
        worksheet.write('E1', 'SHIP')
        worksheet.write('F1', 'AFFCODE')
        worksheet.write('G1', 'BISHPC')
        worksheet.write('H1', 'BISAFN')
        worksheet.write('I1', 'PONO')
        worksheet.write('J1', 'PARTNO')
        worksheet.write('K1', 'PARTNAME')
        worksheet.write('L1', 'BALQTY')
        worksheet.write('M1', 'STDPACK')
        worksheet.write('N1', 'CTN')
        worksheet.write('O1', 'MATCHED')
        worksheet.write('P1', 'REVISE')
        worksheet.write('Q1', 'REMARK')

        seq_ord = 1
        rnd = 2
        for i in data:
            print(f"export id: {i['id']}")
            response = requests.request(
                "GET", f"{api_host}/order/ent/{i['id']}", headers=headers, data={})
            ord = response.json()["data"]
            id = ord["id"]
            whs = ord["consignee"]["whs"]["title"]
            cmaker = ord["consignee"]["whs"]["description"]
            factory = ord["consignee"]["factory"]["title"]
            fac_cd_code = ord["consignee"]["factory"]["cd_code"]
            inv_prefix = ord["consignee"]["factory"]["inv_prefix"]
            label_prefix = ord["consignee"]["factory"]["label_prefix"]
            shiptype = ord["shipment"]["title"]
            affcode = ord["consignee"]["affcode"]["title"]
            pc = ord["pc"]["title"]
            commercial = ord["commercial"]["title"]
            sampflg = ord["sample_flg"]["title"]
            order_title = ord["order_title"]["title"]
            etdtap = str(ord["etd_date"])[:10]
            bioabt = ord["bioabt"]
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
            ref_inv = f"S{biivpx}-{str(str(etdtap).replace('-', ''))}-{running_seq:04d}"
            print(f"----------------------------------------------------------------")
            print(f"🐒{seq_ord}. {etdtap} INV: {inv_no} REF: {ref_inv} ==> {id}🐒")
            print(f"Folder: {str(etdtap).replace('-','')} CUSTNAME: {bisafn}")
            orderDetail = ord["order_detail"]
            for x in orderDetail:
                is_matched = ''
                if str(x['is_matched']) == "True":
                    is_matched = 'OK'

                reason_title = ""
                if x["orderplan"]["revise_order"]["description"] != "-":
                    reason_title = x["orderplan"]["revise_order"]["description"]

                print(f"{(rnd - 1)} ==> ID: {i['id']} IS: {is_matched}")
                whs_name = "SPL"
                worksheet.write(f'A{rnd}', (rnd - 1))
                worksheet.write(f'B{rnd}', whs_name)
                worksheet.write(f'C{rnd}', inv_no)
                worksheet.write(f'D{rnd}', etdtap)
                worksheet.write(f'E{rnd}', shiptype)
                worksheet.write(f'F{rnd}', affcode)
                worksheet.write(f'G{rnd}', bishpc)
                worksheet.write(f'H{rnd}', bisafn)
                worksheet.write(f'I{rnd}', x["pono"])
                worksheet.write(f'J{rnd}', x["orderplan"]["part_no"])
                worksheet.write(f'K{rnd}', x["orderplan"]["part_name"])
                worksheet.write(f'L{rnd}', x["orderplan"]["balqty"])
                worksheet.write(f'M{rnd}', x["orderplan"]["bistdp"])
                worksheet.write(f'N{rnd}', x["order_ctn"])
                worksheet.write(f'O{rnd}', is_matched)
                worksheet.write(f'P{rnd}', x["orderplan"]["reasoncd"])
                worksheet.write(f'Q{rnd}', reason_title)
                rnd += 1

            seq_ord += 1
        # Finally, close the Excel file
        # via the close() method.
        workbook.close()
        #     create_log("Upload Receive Excel",
        #                f"""{dir} is success {response.status_code}""", True)
    except Exception as e:
        print(e)
        create_log("Upload Receive Excel", f"""Error: {str(e)}""", False)
        pass

def patch_invoice(headers):
    response = requests.request("PATCH", f"{api_host}/upload/invoice/tap", headers=headers)
    print(response.text)

if __name__ == "__main__":
    update_reset_stock()
    headers = main()
    if headers != None:
        get_mailbox(headers)
        upload_edi(headers)
        sync_receive(headers)
        merge_receive()
        sync_orderplan(headers)
        sync_order(headers)
        upload_receive_excel(headers)
        if upload_inv(headers) is False:
            print("upload")
            # export_check_inv(headers)
        move_whs()
        check_receive_carton()
        patch_invoice(headers)
        sign_out(headers)
    sys.exit(0)
