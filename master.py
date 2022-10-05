import csv
import os
import time
import requests
import urllib
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), ".env.local"))

api_host = os.getenv("API_HOST")
api_user = os.getenv("API_USERNAME")
api_password = os.getenv("API_PASSWORD")

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

# read loading area
for a in range(97, 123):
    location = f"S-{str(chr(a)).upper()}"
    for i in range(1, 5):
        for e in range(1, 21):
            for j in range(1, 6):
                title = f"{location}{i:02d}-{e:02d}-{j:02d}"
                payload = f'title={title}&description={title}&is_active=true'
                response = requests.request(
                    "POST", f"{api_host}/location", headers=headers, data=payload)
                print(f"create location {title} is: {response.status_code}")
                time.sleep(0.1)

l = ["S-P58", "S-P59", "S-CK1", "S-CK2", "S-OVER-Y01", "S-OVER-Y02", "S-OVER-Y03", "S-OVER-Y04", "S-OVER-Y05",
     "S-OVER-Y06", "S-OVER1", "S-OVER2", "S-OVER3", "S-HOLD", "S-REPALLET", "S-RECHECK", "SNON", "S-XXX", "S-PLOUT", "-"]
for title in l:
    payload = f'title={title}&description={title}&is_active=true'
    response = requests.request(
        "POST", f"{api_host}/location", headers=headers, data=payload)
    print(f"create location {title} is: {response.status_code}")
    # time.sleep(0.1)


# # update stock
file = open(os.path.join(os.path.dirname(__file__), 'data/master/stock_10.csv'))
csvreader = csv.reader(file)
for r in csvreader:
    if r[0] == 'name':
        tagrp = r[0]
        serial_no = r[1]
        print(f"update {tagrp} stock {serial_no}")

# logout
response = requests.request(
    "GET", f"{api_host}/auth/logout", headers=headers, data={})
print(response.text)
