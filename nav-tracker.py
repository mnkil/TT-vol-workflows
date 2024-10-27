import yaml
import requests
import json
import platform
import pandas as pd
from datetime import datetime
import sqlite3
from discordwebhook import Discord

class NAVTracker:
    def __init__(self):
        self.config = self.load_config()
        self.discord = Discord(url=self.config['discord_url'])
        self.conn = sqlite3.connect('nav.db')
        self.nav_df = self.load_nav_data()
        self.session_token = None

    def load_config(self):
        ps = platform.system()
        file_path = "creds.yaml" if ps == "Darwin" else "/home/ec2-user/tt/creds.yaml"
        with open(file_path, "r") as file:
            data = yaml.safe_load(file)
        return {
            'discord_url': data.get("discord_url")[0],
            'user': data.get("user")[0],
            'password': data.get("pw")[0]
        }

    def load_nav_data(self):
        try:
            return pd.read_sql('SELECT * FROM nav', self.conn)
        except:
            return pd.DataFrame(columns=['timestamp', 'NAV'])

    def authenticate(self):
        url = 'https://api.tastyworks.com/sessions'
        payload = {
            "login": self.config['user'],
            "password": self.config['password'],
            "remember-me": True
        }
        headers = {'Content-Type': 'application/json'}
        response = requests.post(url, data=json.dumps(payload), headers=headers)
        response.raise_for_status()
        self.session_token = response.json()['data']['session-token']

    def fetch_nav(self):
        url = 'https://api.tastyworks.com/accounts/5WY49300/balances'
        headers = {
            'Authorization': self.session_token,
            'Content-Type': 'application/json'
        }
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        nav = float(response.json()['data']['net-liquidating-value'])
        return nav

    def update_nav_data(self, nav_value):
        current_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        new_data = pd.DataFrame({'timestamp': [current_timestamp], 'NAV': [nav_value]})
        self.nav_df = pd.concat([self.nav_df, new_data], ignore_index=True)
        self.nav_df.to_sql('nav', self.conn, if_exists='replace', index=False)

    def post_to_discord(self, message):
        self.discord.post(content=message)

    def end_session(self):
        url = 'https://api.tastyworks.com/sessions'
        headers = {
            'Authorization': self.session_token,
            'Content-Type': 'application/json'
        }
        response = requests.delete(url, headers=headers)
        response.raise_for_status()

    def run(self):
        try:
            self.authenticate()
            nav_value = self.fetch_nav()
            self.update_nav_data(nav_value)
            self.post_to_discord(f'NAV ${nav_value:,.2f}')
        finally:
            self.end_session()

if __name__ == "__main__":
    tracker = NAVTracker()
    tracker.run()
