# vol inventory

from discordwebhook import Discord
import yaml
import requests
import json
import platform
import pandas as pd
from datetime import datetime
import sqlite3

def position_feedr():
    ps, file = pform()
    print(f'platform: {ps}, file: {file}')
    creds = get_creds(file)
    discord_url_logs = creds["discord_url_logs"]
    pw = creds["pw"]
    user = creds['user']
    discord = Discord(url=discord_url_logs)
    data = {
        "login": user,
        "password": pw,
        "remember-me": True
    }
    response = get_auth(data)
    session_token = get_token(response)
    data = get_positions(session_token)
    positions_df = pd.DataFrame(data['data']['items'])
    end_session(session_token)
    df_fxinventory = data_dump(positions_df)
    discord.post(content="Inventory job done")
    return df_fxinventory

def data_dump(df):
    ps, _ = pform()
    if ps == "Linux":
        inventory_db_path = "/home/ec2-user/tt/inventory.db"
        inventory_fx_db_path = "/home/ec2-user/tt/inventory-fx.db"
    else:
        inventory_db_path = "inventory.db"
        inventory_fx_db_path = "inventory-fx.db"

    inventory_conn = sqlite3.connect(inventory_db_path)
    df.to_sql('positions', inventory_conn, if_exists='replace', index=False)
    inventory_conn.close()

    df_fxinventory = df[df['symbol'].str[2] == '6']

    inventory_fx_conn = sqlite3.connect(inventory_fx_db_path)
    df_fxinventory.to_sql('fx_positions', inventory_fx_conn, if_exists='replace', index=False)
    # df_fxinventory.to_clipboard()   
    inventory_fx_conn.close()
    # print(df['account-number'].head(1))
    return df_fxinventory

def end_session(session_token):
    url = 'https://api.tastyworks.com/sessions'
    headers = {
        'include-marks': "True",  # Set it
        'Authorization': session_token,
        'Content-Type': 'application/json'
    }
    response = requests.delete(url, headers=headers)
    print('Status Code:', response.status_code)
    print('Response:', response.text)

def get_positions(session_token):
    url = 'https://api.tastyworks.com/accounts/5WY49300/positions'
    headers = {
        'Authorization': session_token,
        'Content-Type': 'application/json'
    }
    response = requests.get(url, headers=headers)
    print('Status Code:', response.status_code)
    json_string = response.text
    data = json.loads(json_string)
    return data

def get_token(response):
    json_string = response.text
    data = json.loads(json_string)
    session_token = data['data']['session-token']
    print("Session Token:", session_token)
    return session_token

def get_auth(data):
    url = 'https://api.tastyworks.com/sessions'
    headers = {
        'Content-Type': 'application/json'
    }
    response = requests.post(url, data=json.dumps(data), headers=headers)
    return response
    print(response.status_code)  # Prints the status code of the response
    print(response.text)

def get_creds(file):
    with open(file, "r") as file:
        data = yaml.safe_load(file)
        discord_url_logs = data.get("discord_url_logs")[0]
        pw = data.get("pw")[0]
        user = data.get("user")[0]
        return {"discord_url_logs": discord_url_logs, "pw": pw, "user": user} 

def pform():
    ps = platform.system()
    if ps == "Darwin":
        file = "creds.yaml"
    if ps == "Linux":
        file = "/home/ec2-user/tt/creds.yaml"
    return ps, file

if __name__ == '__main__':
    df_fxinventory = position_feedr()