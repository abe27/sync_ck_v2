import csv
from datetime import datetime
from ipaddress import ip_address
from multiprocessing.util import is_abstract_socket_namespace
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


# print(headers)
# read user
file = open(os.path.join(os.path.dirname(__file__), 'data/master/user.csv'))
csvreader = csv.reader(file)
for r in csvreader:
    username = f"{int(r[0]):05d}"
    password = r[1]
    try:
        password = f"{int(r[1]):05d}"
    except Exception as e:
        pass
    email = r[2]
    first_name = r[3]
    last_name = r[4]
    position_id = r[5]
    department_id = r[6]
    area_id = r[7]
    whs_id = r[8]
    factory_id = r[9]
    is_admin = "false"
    if int(r[10]) > 0:
        is_admin = "true"

    # login
    passwd = urllib.parse.quote(password)
    payload = f'username={username}&password={passwd}'
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    response = requests.request(
        "POST", f"{api_host}/login", headers=headers, data=payload)
    auth = response.json()
    token = auth["data"]["jwt_token"]
    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/x-www-form-urlencoded'
    }

    # Update Profile
    payload = f'prefix_name_id=-&first_name={first_name}&last_name={last_name}&position_id={position_id}&department_id={department_id}&area_id={area_id}&whs_id={whs_id}&factory_id={factory_id}&is_administrator={is_admin}&is_active=true'
    response = requests.request(
        "PUT", f"{api_host}/auth/me", headers=headers, data=payload)

    print(f"update profile : {response.status_code}")

    # logout
    response = requests.request(
        "GET", f"{api_host}/auth/logout", headers=headers, data={})
    print(response.text)
