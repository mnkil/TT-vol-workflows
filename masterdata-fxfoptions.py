#!/usr/bin/env python3

import os
import yaml
import requests
import json
import platform
import pandas as pd
from datetime import datetime
import sqlite3
from discordwebhook import Discord

class optionchain:
    def __init__(self, creds_file):
        self.creds_file = creds_file
        self.session_token = None
        self.load_credentials()
        self.discord = Discord(url=self.discord_url_logs)

    def load_credentials(self):
        with open(self.creds_file, "r") as file:
            data = yaml.safe_load(file)
            self.discord_url = data.get("discord_url")[0]
            self.pw = data.get("pw")[0]
            self.user = data.get("user")[0]
            self.discord_url_logs = data.get("discord_url_logs")[0]

    def init_session(self):
        url = 'https://api.tastyworks.com/sessions'
        data = {
            "login": self.user,
            "password": self.pw,
            "remember-me": True
        }
        headers = {
            'Content-Type': 'application/json'
        }
        response = requests.post(url, data=json.dumps(data), headers=headers)
        if response.status_code == 201:
            self.session_token = response.json()['data']['session-token']
        else:
            raise Exception("Failed to initialize session")

    def get_snapshot(self, endpoint):
        url = f'https://api.tastyworks.com/{endpoint}'
        headers = {
            'Authorization': self.session_token,
            'Content-Type': 'application/json'
        }
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception("Failed to get snapshot")

#    def save_positions_to_clipboard(self, data):
#        positions_df = pd.DataFrame(data['data']['items'])
#        positions_df.to_clipboard()

    def end_session(self):
        url = 'https://api.tastyworks.com/sessions'
        headers = {
            'Authorization': self.session_token,
            'Content-Type': 'application/json'
        }
        response = requests.delete(url, headers=headers)
        if response.status_code != 204:
            raise Exception("Failed to end session")

    def post_to_discord(self, content):
        self.discord.post(content=content)

def main():
    ps = platform.system()
    file = "creds.yaml" if ps == "Darwin" else "/home/ec2-user/tt/creds.yaml"

    api = optionchain(file)
    api.init_session()
    print("Session Token:", api.session_token)

    # data = api.get_snapshot('futures-option-chains/6E/')
    symbols = ['6A', '6B', '6C', '6E', '6J']
    snapshots = {}

    for symbol in symbols:
        endpoint = f'futures-option-chains/{symbol}/'
        snapshots[symbol] = api.get_snapshot(endpoint)

    # Parse snapshots into a pandas DataFrame
    snapshots_list = []
    for symbol, snapshot in snapshots.items():
        for item in snapshot['data']['items']:
            # item['symbol'] = symbol
            snapshots_list.append(item)

    df_snapshots = pd.DataFrame(snapshots_list)
    # df_snapshots.to_clipboard()
    
    # Convert unsupported types to strings
    for col in df_snapshots.columns:
        if df_snapshots[col].dtype == 'object' or col == 'symbol':
            df_snapshots[col] = df_snapshots[col].astype(str)

    # Save/replace the DataFrame into an SQLite3 database
    db_path = os.path.expanduser('~/tt/masterdata-fxoptchain.db')
    conn = sqlite3.connect(db_path)
    df_snapshots.to_sql('fxoptchain', conn, if_exists='replace', index=False)
    conn.close()

    # Post to Discord
    api.post_to_discord("Masterdata FX Futures Options job done")
    
    api.end_session()
    return df_snapshots

if __name__ == "__main__":
    datax = main()
    # datax