import csv
import os
import sys
import cx_Oracle
from psycopg2.pool import SimpleConnectionPool
import pandas as pd
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), ".env.local"))

DB_HOSTNAME = os.environ.get('DATABASE_URL')
DB_PORT = os.environ.get('DATABASE_PORT')
DB_NAME = os.environ.get('DATABASE_NAME')
DB_USERNAME = os.environ.get('DATABASE_USERNAME')
DB_PASSWORD = os.environ.get('DATABASE_PASSWORD')

ORA_DNS = f"{os.environ.get('ORAC_DB_HOST')}/{os.environ.get('ORAC_DB_SERVICE')}"
ORA_USERNAME = os.environ.get('ORAC_DB_USERNAME')
ORA_PASSWORD = os.environ.get('ORAC_DB_PASSWORD')

# Initail Data

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


def main():
    file_excel = ["data/stocks/ck1.xls", "data/stocks/ck2.xls"]
    for filename in file_excel:
        df = pd.read_excel(filename, index_col=None)
        data = df.to_dict('records')
        r = 1
        for i in data:
            whs = i["stock"]
            partno = i["partno"]
            zone = i["zone"]
            lotno = str(i["lotno"])
            serial_no = i["serial_no"]
            qty = 0
            factory = "TAP"
            is_out = 'false'
            try:
                qty = int(i["qtypcs"])
            except Exception as e:
                pass

            serial = Oracur.execute(
                f"SELECT RUNNINGNO,STOCKQUANTITY FROM TXP_CARTONDETAILS WHERE RUNNINGNO='{serial_no}'")
            is_found = 'false'
            factory = "TAP"
            is_out = 'false'
            data = serial.fetchone()
            if data != None:
                is_found = 'true'
                factory = "-"
                is_out = 'true'
                if int(data[1]) > 0:
                    is_out = 'false'

            pg_cursor.execute(
                f"insert into tbt_check_stocks(whs,partno,zone,lotno,serial_no,qty,factory,is_out,is_found,on_date)values('{whs}','{partno}','{zone}','{lotno}','{serial_no}','{qty}','{factory}','{is_out}',{is_found},current_timestamp)")
            print(f"{r}. TAP SERIALNO: {serial_no}")
            r += 1

        pgdb.commit()


def check_by_spl():
    sql = f"""SELECT t.TAGRP whs,t.partno,c.SHELVE zone,c.lotno,t.RUNNINGNO serial_no,c.STOCKQUANTITY qty,'INJ' factory
        FROM TXP_STKTAKECARTON t
        LEFT JOIN TXP_CARTONDETAILS c ON t.RUNNINGNO=c.RUNNINGNO
        ORDER BY t.PARTNO,c.LOTNO,t.RUNNINGNO"""
    obj = (Oracur.execute(sql)).fetchall()
    r = 1
    for i in obj:
        whs = i[0]
        partno = i[1]
        zone = i[2]
        lotno = i[3]
        serial_no = i[4]
        qty = int(i[5])
        is_out = 'false'
        if qty > 0:
            is_out = 'true'

        factory = i[6]
        pg_cursor.execute(
            f"select serial_no from tbt_check_stocks where serial_no='{serial_no}'")
        sql_pg = f"""update tbt_check_stocks set factory='-',is_found=true,is_matched=true where serial_no='{serial_no}'"""
        if pg_cursor.fetchone() is None:
            sql_pg = f"""insert into tbt_check_stocks(whs,partno,zone,lotno,serial_no,qty,factory,is_out,is_found,on_date)values('{whs}','{partno}','{zone}','{lotno}','{serial_no}','{qty}','{factory}','{is_out}',false,current_timestamp)"""
        pg_cursor.execute(sql_pg)
        print(f"{r}. SPL SERIALNO: {serial_no}")
        r += 1
    
    pgdb.commit()

if __name__ == '__main__':
    main()
    check_by_spl()
    pool.release(Oracon)
    pool.close()
    sys.exit(0)
