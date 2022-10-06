import csv
from datetime import datetime
from ipaddress import ip_address
import os
import shelve
import time
import requests
import urllib
import cx_Oracle
from psycopg2.pool import SimpleConnectionPool
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), ".env.local"))

api_host = os.getenv("API_HOST")
api_user = os.getenv("API_USERNAME")
api_password = os.getenv("API_PASSWORD")

DB_HOSTNAME = os.environ.get('DATABASE_URL')
DB_PORT = os.environ.get('DATABASE_PORT')
DB_NAME = os.environ.get('DATABASE_NAME')
DB_USERNAME = os.environ.get('DATABASE_USERNAME')
DB_PASSWORD = os.environ.get('DATABASE_PASSWORD')

ORA_DNS = f"{os.environ.get('ORAC_DB_HOST')}/{os.environ.get('ORAC_DB_SERVICE')}"
ORA_USERNAME = os.environ.get('ORAC_DB_USERNAME')
ORA_PASSWORD = os.environ.get('ORAC_DB_PASSWORD')


def create_log(title, description, is_status):
    payload = f'title={title}&description={description}&is_active={str(is_status).lower()}'
    response = requests.request("POST", f"{api_host}/logs", headers={
                                'Content-Type': 'application/x-www-form-urlencoded'}, data=payload)
    print(f"create log {title} status: {response.status_code}")


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
# # print(headers)
# # read user
# file = open(os.path.join(os.path.dirname(__file__), 'data/master/user.csv'))
# csvreader = csv.reader(file)
# for r in csvreader:
#     username = r[0]
#     password = r[1]
#     email = r[2]
#     first_name = r[3]
#     last_name = r[4]
#     payload = f'username={username}&email={urllib.parse.quote(email)}&password={urllib.parse.quote(password)}&firstname={first_name}&lastname={last_name}'
#     response = requests.request(
#         "POST", f"{api_host}/register", headers=headers, data=payload)
#     print(f"Create User {username} Status: {response.status_code}")
#     time.sleep(0.1)

# print(f"----------------------------------------------------------------")

# # read consignee
# file = open(os.path.join(os.path.dirname(__file__), 'data/master/consignee.csv'))
# csvreader = csv.reader(file)
# for r in csvreader:
#     whs = r[0]
#     factory = r[1]
#     affcode = r[2]
#     customer = r[3]
#     address = r[4]
#     prefix = r[5]
#     payload = f'whs_id={whs}&factory_id={factory}&affcode_id={affcode}&customer_id={customer}&customer_address_id={address}&prefix={prefix}&is_active=true'
#     response = requests.request(
#         "POST", f"{api_host}/consignee", headers=headers, data=payload)
#     print(f"Create Consignee {customer} Status: {response.status_code}")

# # read group
# file = open(os.path.join(os.path.dirname(__file__), 'data/master/group.csv'))
# csvreader = csv.reader(file)
# for r in csvreader:
#     username = r[0]
#     whs = r[1]
#     factory = r[2]
#     affcode = r[3]
#     customer = r[4]
#     order_group = r[5]
#     sub_order = r[6]
#     payload = f'user_id={username}&whs_id={whs}&factory_id={factory}&affcode_id={affcode}&custcode_id={customer}&order_group_type_id={order_group}&sub_order={sub_order}&description=-&is_active=true'
#     response = requests.request(
#         "POST", f"{api_host}/order/consignee", headers=headers, data=payload)
#     print(
#         f"create consignee {username} ==> {affcode} customer:{customer} group is: {response.status_code}")

# # read loading area
# file = open(os.path.join(os.path.dirname(__file__), 'data/master/loading_area.csv'))
# csvreader = csv.reader(file)
# for r in csvreader:
#     bioat = r[0]
#     factory = r[1]
#     prefix = r[2]
#     loading_area = r[3]
#     privilege = r[4]
#     payload = f'bioat={bioat}&factory={factory}&prefix={prefix}&loading_area={loading_area}&privilege={privilege}&is_active=true'
#     response = requests.request(
#         "POST", f"{api_host}/order/loading", headers=headers, data=payload)
#     print(f"create loading Area {loading_area} is: {response.status_code}")
#     # time.sleep(0.1)

# # read loading area
# for a in range(97, 123):
#     location = f"S-{str(chr(a)).upper()}"
#     for i in range(1, 5):
#         for e in range(1, 16):
#             for j in range(1, 6):
#                 title = f"{location}{i:02d}-{e:02d}-{j:02d}"
#                 payload = f'title={title}&description={title}&is_active=true'
#                 response = requests.request(
#                     "POST", f"{api_host}/location", headers=headers, data=payload)
#                 print(f"create location {title} is: {response.status_code}")
#                 time.sleep(0.1)

# l = ["S-P58", "S-P59", "S-CK1", "S-CK2", "S-OVER-Y01", "S-OVER-Y02", "S-OVER-Y03", "S-OVER-Y04", "S-OVER-Y05",
#      "S-OVER-Y06", "S-OVER1", "S-OVER2", "S-OVER3", "S-HOLD", "S-REPALLET", "S-RECHECK", "SNON", "S-XXX", "S-PLOUT", "-"]
# for title in l:
#     payload = f'title={title}&description={title}&is_active=true'
#     response = requests.request(
#         "POST", f"{api_host}/location", headers=headers, data=payload)
#     print(f"create location {title} is: {response.status_code}")
#     # time.sleep(0.1)


# try:
#     # # update stock
#     pool = cx_Oracle.SessionPool(user=ORA_PASSWORD,
#                                  password=ORA_USERNAME,
#                                  dsn=ORA_DNS,
#                                  min=2,
#                                  max=100,
#                                  increment=1,
#                                  encoding="UTF-8")
#     # Acquire a connection from the pool
#     Oracon = pool.acquire()
#     Oracur = Oracon.cursor()
#     file = open(os.path.join(os.path.dirname(
#         __file__), 'data/master/stock_10.csv'))
#     csvreader = csv.reader(file)
#     n = 1
#     for r in csvreader:
#         tagrp = r[0]
#         serial_no = r[1]
#         Oracur.execute(
#             f"SELECT rowid,TAGRP,PARTNO,LOTNO,RUNNINGNO,CASEID,CASENO,STOCKQUANTITY,SHELVE,(SELECT SYS_CONTEXT('USERENV','IP_ADDRESS') FROM dual),SIID,PALLETKEY,INVOICENO,SINO FROM TXP_CARTONDETAILS WHERE RUNNINGNO='{serial_no}'")
#         obj = Oracur.fetchone()
#         if obj != None:
#             Oracur.execute(f"UPDATE TXP_CARTONDETAILS SET IS_CHECK=1 WHERE RUNNINGNO='{serial_no}'")
#             Oracon.commit()
#             time.sleep(0.5)
#             print(f"{n}. update {tagrp} stock {serial_no} id: {obj[0]}")
#         n += 1

#     ### Sync Carton
#     Oracur.execute(f"SELECT RUNNINGNO FROM TXP_CARTONDETAILS ORDER BY PARTNO,LOTNO,RUNNINGNO")
#     n = 1
#     for r in Oracur.fetchall():
#         Oracur.execute(f"UPDATE TXP_CARTONDETAILS SET IS_CHECK=1 WHERE RUNNINGNO='{r[0]}'")
#         Oracon.commit()
#         time.sleep(0.5)
#         print(f"{n}. update stock {r[0]}")
#         n += 1
#     # Oracon.commit()
#     pool.release(Oracon)
#     pool.close()
# except Exception as e:
#     print(e)
#     pass


# Get Stock DB
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

    # Initail PostgreSQL Server Pool 10 Live
    pgdb_conn = SimpleConnectionPool(1,
                                     20,
                                     host=DB_HOSTNAME,
                                     port=DB_PORT,
                                     user=DB_USERNAME,
                                     password=DB_PASSWORD,
                                     database=DB_NAME)
    pgdb = pgdb_conn.getconn()
    pg_cursor = pgdb.cursor()

    # Fetch STKDB from the database
    pg_cursor.execute(
        f"select partno,serial_no from tbt_check_stocks where is_sync=false order by whs,partno,serial_no limit 200")
    n = 1
    data = pg_cursor.fetchall()
    if data is not None:
        create_log("Start Sync carton stock", f"running at {datetime.now().strftime('%Y%m%d %H:%M:%S')}", True)
        for r in data:
            part_no = str(r[0])
            serial_no = str(r[1])
            Oracur.execute(
                f"SELECT rowid,TAGRP,PARTNO,LOTNO,RUNNINGNO,CASEID,CASENO,STOCKQUANTITY,SHELVE,(SELECT SYS_CONTEXT('USERENV','IP_ADDRESS') FROM dual),SIID,PALLETKEY,INVOICENO,SINO FROM TXP_CARTONDETAILS WHERE PARTNO='{part_no}' AND RUNNINGNO='{serial_no}'")
            obj = Oracur.fetchone()
            if obj is not None:
                rowid = obj[0]
                whs = obj[1]
                # part_no = obj[2]
                lot_no = obj[3]
                # serial_no = obj[4]
                line_no = obj[5]
                revision_no = obj[6]
                if revision_no is None:
                    revision_no = "-"
                qty = obj[7]
                shelve = obj[8]
                if shelve is None:
                    shelve = "-"

                ip_address = obj[9]
                emp_id = obj[10]
                if emp_id is None:
                    emp_id = "-"

                pallet_no = obj[11]
                if pallet_no is None:
                    pallet_no = "-"

                invoice_no = obj[12]
                description = obj[13]
                if description is None:
                    description = "-"

                payload = f'row_id={rowid}&whs={whs}&part_no={part_no}&lot_no={lot_no}&serial_no={serial_no}&die_no={line_no}&rev_no={revision_no}&qty={qty}&shelve={shelve}&ip_address={ip_address}&emp_id={emp_id}&ref_no={pallet_no}&receive_no={invoice_no}&description={description}'
                headers = {'Content-Type': 'application/x-www-form-urlencoded'}
                response = requests.request(
                    "POST", f"{api_host}/carton/history", headers=headers, data=payload)
                print("%d . %s ==> %s %s serial_no = %s status code = %s" %
                    (n, rowid, whs, part_no, serial_no, response.status_code))

                # after create carton history
                if response.status_code == 201:
                    pg_cursor.execute(
                        f"update tbt_check_stocks set is_sync=true where serial_no='{serial_no}'")
                    pgdb.commit()

                time.sleep(0.2)

            n += 1

        create_log("End Sync carton stock", f"running at {datetime.now().strftime('%Y%m%d %H:%M:%S')}", True)

    pool.release(Oracon)
    pool.close()
except Exception as e:
    print(e)
    create_log("Error Sync carton stock", str(e), False)
    pass


# logout
response = requests.request(
    "GET", f"{api_host}/auth/logout", headers=headers, data={})
print(response.text)
