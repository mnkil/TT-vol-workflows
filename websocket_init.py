#!/usr/bin/env python3

import platform
import yaml
import requests
import json
from discordwebhook import Discord
from datetime import datetime
import pandas as pd
import sqlite3

# Constants
API_BASE_URL = "https://api.tastyworks.com"
SESSION_URL = f"{API_BASE_URL}/sessions"
QUOTE_TOKEN_URL = f"{API_BASE_URL}/api-quote-tokens"

class TastyworksSession:
    def __init__(self):
        """Initializes the session, loads configuration, and sets up Discord webhook."""
        config = self._get_config()
        self.user = config["user"][0]
        self.password = config["pw"][0]
        self.discord = self._init_discord(config)
        self.session_token = None

    def _get_config(self):
        """
        Determines the correct path to the configuration file based on the OS
        and loads credentials from a YAML file.
        """
        ps = platform.system()
        file = "creds.yaml" if ps == "Darwin" else "/home/ec2-user/tt/creds.yaml"
        
        with open(file, "r") as f:
            data = yaml.safe_load(f)
        return data

    def _init_discord(self, data):
        """
        Initializes the Discord webhook client with the specified URL.
        """
        discord_url = data.get("discord_url_logs")[0]
        return Discord(url=discord_url)

    def authenticate(self):
        """
        Initiates a session with Tastyworks by sending a POST request with user credentials.
        Sets the session token if successful.
        """
        payload = {
            "login": self.user,
            "password": self.password,
            "remember-me": True
        }
        headers = {'Content-Type': 'application/json'}

        response = requests.post(SESSION_URL, data=json.dumps(payload), headers=headers)
        if response.status_code in {200, 201}:  # Accept 200 and 201 as successful responses
            session_data = response.json()
            self.session_token = session_data['data'].get('session-token')
            print("Session Token:", self.session_token)
        else:
            raise ConnectionError(f"Failed to authenticate: {response.status_code} - {response.text}")

    def get_quote_token(self):
        """
        Retrieves the quote token using the session token for authorization.
        """
        headers = {
            'Authorization': self.session_token,
            'Content-Type': 'application/json'
        }
        response = requests.get(QUOTE_TOKEN_URL, headers=headers)
        if response.status_code == 200:
            quote_data = response.json()
            print("Quote Token Data:", quote_data)
            return quote_data
        else:
            raise ConnectionError(f"Failed to get quote token: {response.status_code} - {response.text}")

    def close_session(self):
        """
        Closes the Tastyworks session by sending a DELETE request with the session token.
        """
        headers = {
            'Authorization': self.session_token,
            'Content-Type': 'application/json'
        }
        response = requests.delete(SESSION_URL, headers=headers)
        if response.status_code in {200, 204}:  # Accept 200 and 204 as successful responses
            print("Session closed successfully.")
        else:
            print(f"Failed to close session: {response.status_code} - {response.text}")
        return response.status_code, response.text

    def send_discord_message(self, message):
        """
        Sends a message to the Discord webhook.
        """
        self.discord.post(content=message)

    def run(self):
        """
        Runs the full workflow: authenticate, get quote token, close session, and send notification.
        """
        # Authenticate and get session token
        self.authenticate()

        # Get quote token
        quote_data = self.get_quote_token()

        # Close session
        status_code, response_text = self.close_session()

        # Send message to Discord (Example usage)
        self.send_discord_message(f"Session closed with status: {status_code}")
        
        return quote_data


# Run the main workflow
if __name__ == "__main__":
    session = TastyworksSession()
    streamer_token = session.run()
