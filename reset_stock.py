from datetime import datetime, timedelta
import os
import sys
import cx_Oracle
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), ".env.local"))

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

def main():
    print("Connecting to Oracle...")
    obj =  Oracur.execute("SELECT RUNNINGNO FROM TXP_CARTONDETAILS WHERE SHELVE IS NULL").fetchall()
    for i in obj:
        sh = Oracur.execute(f"SELECT SHELVE FROM TXP_STKTAKECARTON WHERE RUNNINGNO='{i[0]}'").fetchone()
        if sh:
            shelve = sh[0]
            print(f"{i[0]} is SHELVE: {shelve}")
            if shelve != None:
                Oracur.execute(f"UPDATE TXP_CARTONDETAILS SET SHELVE='{shelve}' WHERE RUNNINGNO='{i[0]}'")

    Oracon.commit()
    print("Connected")

if __name__ == '__main__':
    main()
    pool.release(Oracon)
    pool.close()
    sys.exit(0)