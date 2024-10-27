#!/usr/bin/env python3

# Download Futures Masterdata
import os
import yaml
import requests
import json
import platform
import pandas as pd
import sqlite3
from datetime import datetime
from discordwebhook import Discord

class TastyworksAPI:
    def __init__(self, creds_file):
        self.creds_file = creds_file
        self.session_token = None
        self.discord = None
        self.load_creds()
        self.init_discord()

    def load_creds(self):
        with open(self.creds_file, "r") as file:
            data = yaml.safe_load(file)
            self.discord_url = data.get("discord_url")[0]
            self.pw = data.get("pw")[0]
            self.user = data.get("user")[0]
            self.discord_url_logs = data.get("discord_url_logs")[0]

    def init_discord(self):
        self.discord = Discord(url=self.discord_url_logs)

    def start_session(self):
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
            raise Exception(f"Failed to start session: {response.status_code}")

    def end_session(self):
        url = 'https://api.tastyworks.com/sessions'
        headers = {
            'Authorization': self.session_token,
            'Content-Type': 'application/json'
        }
        response = requests.delete(url, headers=headers)
        if response.status_code != 204:
            raise Exception(f"Failed to end session: {response.status_code}")

    def get_instruments(self):
        url = 'https://api.tastyworks.com/instruments/futures'
        headers = {
            'Authorization': self.session_token,
            'Content-Type': 'application/json'
        }
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            return response.json()['data']['items']
        else:
            raise Exception(f"Failed to get instruments: {response.status_code}")

    def post_to_discord(self, content):
        self.discord.post(content=content)


class DatabaseManager:
    def __init__(self, db_file):
        db_file = os.path.expanduser(db_file)  # Expand the tilde to the full path
        self.conn = sqlite3.connect(db_file)

    def read_table(self, table_name):
        try:
            return pd.read_sql(f'SELECT * FROM {table_name}', self.conn)
        except:
            return pd.DataFrame()

    def write_table(self, df, table_name):
        df.to_sql(table_name, self.conn, if_exists='replace', index=False)

    def close(self):
        self.conn.close()


def main():
    ps = platform.system()
    creds_file = "creds.yaml" if ps == "Darwin" else "/home/ec2-user/tt/creds.yaml"

    # Initialize API and Database Manager
    api = TastyworksAPI(creds_file)
    db_manager = DatabaseManager('~/tt/masterdata-futures.db')

    # Start session
    api.start_session()

    instruments = api.get_instruments()

    # Save positions to database
    dfmasterfutures = pd.DataFrame(instruments)
    # dfmasterfutures.to_clipboard()    
    dfmasterfutures = dfmasterfutures.astype({col: 'str' for col in dfmasterfutures.select_dtypes(include=['object']).columns})
    db_manager.write_table(dfmasterfutures, 'masterdatafutures')
    # return dfmasterfutures

    # Post to Discord
    api.post_to_discord("Masterdata Futures job done")

    # End session
    api.end_session()

    # Close database connection
    db_manager.close()

if __name__ == '__main__':
    # master = main()
    main()
