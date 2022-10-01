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
    username = r[0]
    whs = r[1]
    factory = r[2]
    affcode = r[3]
    customer = r[4]
    order_group = r[5]
    sub_order = r[6]
    payload=f'user_id={username}&whs_id={whs}&factory_id={factory}&affcode_id={affcode}&custcode_id={customer}&order_group_type_id={order_group}&sub_order={sub_order}&description=-&is_active=true'
    response = requests.request("POST", f"{api_host}/order/consignee", headers=headers, data=payload)
    print(f"create consignee {username} ==> {affcode} customer:{customer} group is: {response.status_code}")
    time.sleep(0.1)

### read loading area
file = open('data/master/loading_area.csv')
csvreader = csv.reader(file)
for r in csvreader:
    bioat = r[0]
    factory = r[1]
    prefix = r[2]
    loading_area = r[3]
    privilege = r[4]
    payload=f'bioat={bioat}&factory={factory}&prefix={prefix}&loading_area={loading_area}&privilege={privilege}&is_active=true'
    response = requests.request("POST", f"{api_host}/order/loading", headers=headers, data=payload)
    print(f"create loading Area {loading_area} is: {response.status_code}")
    time.sleep(0.1)

### read loading area
file = open('data/master/location.csv')
csvreader = csv.reader(file)
for r in csvreader:
    title = r[0]
    payload = f'title={title}&description={title}&is_active=true'
    response = requests.request("POST", f"{api_host}/location", headers=headers, data=payload)
    print(f"create location {title} is: {response.status_code}")
    time.sleep(0.1)


### logout
response = requests.request("GET", f"{api_host}/auth/logout", headers=headers, data={})
print(response.text)

