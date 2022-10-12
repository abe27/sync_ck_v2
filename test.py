import csv
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


def sync_order(headers):
    # Sync Order
    try:
        response = requests.request(
            "GET", f"{api_host}/order/ent", headers=headers, data={})
        data = response.json()["data"]
        seq_ord = 1
        d = datetime.now()
        create_log("Start Sync Order",
                   f"Sync {len(data)} Running on: {d.strftime('%Y-%m-%d %H:%M:%S')}", True)
        for ord in data:
            # print(ord)
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
            ref_inv = f"S{biivpx}-{str(str(etdtap).replace('-', ''))}-{running_seq:04d}"

            # print(f"factory={factory} inv_prefix={inv_prefix} label_prefix={label_prefix} shiptype={shiptype} affcode={affcode} pc={pc} commercial={commercial} sampflg={sampflg} order_title={order_title} etdtap={etdtap} bioat={bioat} bishpc={bishpc} biivpx={biivpx} bisafn={bisafn} ship_form={ship_form} ship_to={ship_to} loading_area={loading_area} privilege={privilege} zone_code={zone_code} running_seq={running_seq} ")
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
                print(f"👌{seq}::::{id}:::TEST ==> {inv_no} ID: {order_id}👌")
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

                pallet_detail = pallet["pallet_detail"]
                pl_seq = 1
                for p in pallet_detail:
                    fticket_no = (
                        f'{label_prefix}{str(etdtap[3:4])}{int(p["seq_no"]) + 1:08d}')
                    print(f"{pl_seq}. PALLETNO: {pallet_no} FTICK: {fticket_no}")
                    pl_seq += 1

            # Update Order Status Sync
            payload = 'is_sync=true&is_active=true'
            response = requests.request(
                "PUT", f"{api_host}/order/ent/{id}", headers=headers, data=payload)
            print(f"UPDATE STATUS SYNC: {response.status_code}")
            seq_ord += 1
        d = datetime.now()
        create_log("End Sync Order",
                   f" At: {d.strftime('%Y-%m-%d %H:%M:%S')}", True)
    except Exception as ex:
        print(ex)
        create_log("Error Sync Order", f"Error with: {str(ex)}", False)
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


if __name__ == "__main__":
    headers = main()
    if headers != None:
        sync_order(headers)
        sign_out(headers)
    sys.exit(0)
