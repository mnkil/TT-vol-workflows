from discordwebhook import Discord
import yaml
import requests
import json
import platform
import pandas as pd
from datetime import datetime
import sqlite3

ps = platform.system()

if ps == "Darwin":
    file = "creds.yaml"
if ps == "Linux":
    file = "/home/ec2-user/tt/creds.yaml"

conn = sqlite3.connect('nav.db')  # This will create the database file if it doesn't exist

try:
    nav_df = pd.read_sql('SELECT * FROM nav', conn)
except:
    nav_df = pd.DataFrame(columns=['timestamp', 'NAV'])

### INIT SESSION
with open(file, "r") as file:
    data = yaml.safe_load(file)
    discord_url = data.get("discord_url")[0]
    pw = data.get("pw")[0]
    user = data.get("user")[0]

discord = Discord(url=discord_url)

url = 'https://api.tastyworks.com/sessions'
data = {
    "login": user,
    "password": pw,
    "remember-me": True
}
headers = {
    'Content-Type': 'application/json'
}

response = requests.post(url, data=json.dumps(data), headers=headers)
print(response.status_code)  # Prints the status code of the response
print(response.text)

### GET AUTH
json_string = response.text

data = json.loads(json_string)
session_token = data['data']['session-token']
print("Session Token:", session_token)

### GET SNAPSHOT
url = 'https://api.tastyworks.com/accounts/5WY49300/balances'

headers = {
    'Authorization': session_token,
    'Content-Type': 'application/json'
}

response = requests.get(url, headers=headers)
print('Status Code:', response.status_code)

json_string = response.text
data = json.loads(json_string)
nav = data['data']['net-liquidating-value']
nav = float(nav)
current_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
new_data = pd.DataFrame({'timestamp': [current_timestamp], 'NAV': [nav]})
if nav_df.empty:
    nav_df = new_data
else:
    nav_df = pd.concat([nav_df, new_data], ignore_index=True)
nav_df.to_sql('nav', conn, if_exists='replace', index=False)

discord.post(content=f'NAV ${nav:,.2f}')

### END SESSION
url = 'https://api.tastyworks.com/sessions'
headers = {
    'Authorization': session_token,
    'Content-Type': 'application/json'
}

# Sending the DELETE request
response = requests.delete(url, headers=headers)

print('Status Code:', response.status_code)
print('Response:', response.text)

data['data']['net-liquidating-value']
