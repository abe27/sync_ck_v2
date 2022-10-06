import csv
from datetime import datetime
from ipaddress import ip_address
import os
import shelve
import sys
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


def fetch_carton():
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

        # Fetch CartonDB
        Oracur.execute(
            f"SELECT TAGRP,PARTNO,SHELVE,LOTNO,RUNNINGNO,STOCKQUANTITY,'INJ' factory,SYSDTE FROM TXP_CARTONDETAILS ORDER BY SYSDTE,PARTNO,LOTNO,RUNNINGNO")
        data = Oracur.fetchall()
        pool.release(Oracon)
        pool.close()

        n = 1
        for i in data:
            # print(i)
            whs = i[0]
            partno = i[1]
            zone = i[2]
            lotno = i[3]
            serial_no = i[4]
            qty = i[5]
            factory = i[6]
            pg_cursor.execute(
                f"select serial_no,is_sync from tbt_check_stocks where serial_no='{serial_no}'")
            stk = pg_cursor.fetchone()
            if stk is None:
                pg_cursor.execute(
                    f"insert into tbt_check_stocks(whs, partno, zone, lotno, serial_no, qty, factory, is_out, is_found, is_matched, is_sync, on_date)values('{whs}', '{partno}', '{zone}', '{lotno}', '{serial_no}', {qty}, '{factory}', false, false, false, false, CURRENT_TIMESTAMP)")
                pgdb.commit()
            print(f"{n} . insert stock check serial no {serial_no}")
            n += 1
        # create_log("Sync carton stock", ("running at %s",
        #                                  datetime.now().strftime("%Y%m%d %H:%M:%S")), True)

    except Exception as e:
        print(e)
        pass


if __name__ == "__main__":
    fetch_carton()
    sys.exit(0)
