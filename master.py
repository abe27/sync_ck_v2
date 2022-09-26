import csv
import os
import time
import requests
import urllib
from dotenv import load_dotenv
load_dotenv()

api_host = os.getenv("API_HOST")
api_user = os.getenv("API_USERNAME")
api_password = os.getenv("API_PASSWORD")

# login
passwd = urllib.parse.quote(api_password)
payload = f'username={api_user}&password={passwd}'
headers = {'Content-Type': 'application/x-www-form-urlencoded'}
response = requests.request("POST", f"{api_host}/login", headers=headers, data=payload)
auth = response.json()
token = auth["data"]["jwt_token"]
headers = {
    'Authorization': f'Bearer {token}',
    'Content-Type': 'application/x-www-form-urlencoded'
}
# print(headers)
### read user
file = open('data/master/user.csv')
csvreader = csv.reader(file)
for r in csvreader:
    username = r[0]
    password = r[1]
    email = r[2]
    first_name = r[3]
    last_name = r[4]
    payload=f'username={username}&email={urllib.parse.quote(email)}&password={urllib.parse.quote(password)}&firstname={first_name}&lastname={last_name}'
    response = requests.request("POST", f"{api_host}/register", headers=headers, data=payload)
    print(f"Create User {username} Status: {response.status_code}")
    time.sleep(0.1)

print(f"----------------------------------------------------------------")

### read consignee
file = open('data/master/consignee.csv')
csvreader = csv.reader(file)
for r in csvreader:
    whs = r[0]
    factory = r[1]
    affcode = r[2]
    customer = r[3]
    address = r[4]
    prefix = r[5]
    payload=f'whs_id={whs}&factory_id={factory}&affcode_id={affcode}&customer_id={customer}&customer_address_id={address}&prefix={prefix}&is_active=true'
    response = requests.request("POST", f"{api_host}/consignee", headers=headers, data=payload)
    print(f"Create Consignee {customer} Status: {response.status_code}")
    time.sleep(0.1)

### read group
file = open('data/master/group.csv')
csvreader = csv.reader(file)
for r in csvreader:
    user = r[0]##"00534",
    affcode = r[0]##"32Y1",
    custcode = r[0]##"32Y1",
    custname = r[0]##YAS,
    order_group = r[0]##N,
    sub_order = r[0]##-
    # payload=f'whs_id={whs}&factory_id={factory}&affcode_id={affcode}&customer_id={customer}&customer_address_id={address}&prefix={prefix}&is_active=true'
    # response = requests.request("POST", f"{api_host}/consignee", headers=headers, data=payload)
    # print(f"Create Consignee {customer} Status: {response.status_code}")
    # time.sleep(0.1)


### logout
response = requests.request("GET", f"{api_host}/auth/logout", headers=headers, data={})
print(response.text)

