#!/usr/bin/env python3

import platform
import sqlite3
import pandas as pd
import yaml
import requests
import json
from datetime import datetime
from discordwebhook import Discord

# --------- Tastyworks API ---------

class TastyworksAPI:
    def __init__(self, creds_path):
        self.creds = self._load_creds(creds_path)
        self.session_token = None

    def _load_creds(self, file_path):
        """Retrieve credentials from a YAML file."""
        with open(file_path, "r") as file:
            data = yaml.safe_load(file)
            return {
                "discord_url_logs": data.get("discord_url_logs")[0],
                "pw": data.get("pw")[0],
                "user": data.get("user")[0]
            }

    def authenticate(self):
        """Authenticate with Tastyworks API and return session token."""
        url = 'https://api.tastyworks.com/sessions'
        headers = {'Content-Type': 'application/json'}
        data = {"login": self.creds["user"], "password": self.creds["pw"], "remember-me": True}
        response = requests.post(url, data=json.dumps(data), headers=headers)
        response_data = response.json()
        self.session_token = response_data['data']['session-token']

    def end_session(self):
        """End Tastyworks session."""
        url = 'https://api.tastyworks.com/sessions'
        headers = {'Authorization': self.session_token, 'Content-Type': 'application/json'}
        requests.delete(url, headers=headers)
        print('Session ended.')

    def get_positions(self):
        """Retrieve positions data from Tastyworks API."""
        url = 'https://api.tastyworks.com/accounts/5WY49300/positions'
        headers = {'Authorization': self.session_token, 'Content-Type': 'application/json'}
        response = requests.get(url, headers=headers)
        return response.json()

# --------- Database Handler ---------

class DatabaseHandler:
    def __init__(self, db_path):
        self.db_path = db_path

    def connect(self):
        """Establish a connection to the SQLite database at the specified path."""
        return sqlite3.connect(self.db_path)

    def save_dataframe_to_table(self, df, table_name):
        """Save a pandas DataFrame to a specified table in the database."""
        with self.connect() as conn:
            df.to_sql(table_name, conn, if_exists='replace', index=False)

    def fetch_table_as_dataframe(self, table_name):
        """Fetch a specified table from the database as a pandas DataFrame."""
        with self.connect() as conn:
            return pd.read_sql(f'SELECT * FROM {table_name}', conn)

    @staticmethod
    def get_db_paths():
        """Determine the platform and set the database paths accordingly."""
        ps = platform.system()
        if ps == "Linux":
            return "/home/ec2-user/tt/inventory-fx.db", "/home/ec2-user/tt/masterdata-futures.db", "/home/ec2-user/tt/inventory.db"
        return "inventory-fx.db", "masterdata-futures.db", "inventory.db"

# --------- FX Inventory Processor ---------

class FXInventoryProcessor:
    def __init__(self, api, inventory_db, inventory_fx_db, futures_db, fopchain_db_path):
        self.api = api
        self.inventory_db = DatabaseHandler(inventory_db)  # Handles full inventory
        self.inventory_fx_db = DatabaseHandler(inventory_fx_db)  # Handles FX-only inventory
        self.futures_db = DatabaseHandler(futures_db)
        self.fopchain_db_path = fopchain_db_path

    def filter_fx_positions(self, df):
        """Filter FX positions from the positions DataFrame."""
        return df[df['symbol'].str[2] == '6']

    def map_symbols_to_streamers(self, df_fx, df_futures):
        """Map underlying symbols from FX inventory to streamer symbols."""
        symbol_to_streamer = dict(zip(df_futures['symbol'], df_futures['streamer-symbol']))
        df_fx['streamer-symbol'] = df_fx['underlying-symbol'].map(symbol_to_streamer)
        return df_fx

    def run_position_feed(self):
        """Retrieve positions, store them in both databases, and filter FX inventory."""
        # Authenticate, retrieve positions, and close session
        self.api.authenticate()
        positions_data = self.api.get_positions()
        self.api.end_session()

        # Store all positions in inventory.db
        positions_df = pd.DataFrame(positions_data['data']['items'])
        if 'underlying-symbol' in positions_df.columns:
            positions_df = positions_df.sort_values(by='underlying-symbol')
        self.inventory_db.save_dataframe_to_table(positions_df, 'positions')

        # Filter and store FX positions in inventory-fx.db
        df_fxinventory = self.filter_fx_positions(positions_df)
        if 'underlying-symbol' in df_fxinventory.columns:
            df_fxinventory = df_fxinventory.sort_values(by='underlying-symbol')
        self.inventory_fx_db.save_dataframe_to_table(df_fxinventory, 'fx_positions')
        return df_fxinventory

    def update_inventory(self):
        """Update the FX inventory with additional streamer symbols and save."""
        # Load FX inventory and futures data
        df_fxinventory = self.inventory_fx_db.fetch_table_as_dataframe('fx_positions')
        df_masterfuturesdata = self.futures_db.fetch_table_as_dataframe('masterdatafutures')

        # Update FX inventory with streamer symbols
        df_fxinventory = self.map_symbols_to_streamers(df_fxinventory, df_masterfuturesdata)

        # Load the fopchain table from the masterdata-fxoptchain.db database
        fopchain_db = DatabaseHandler(self.fopchain_db_path)
        df_fopchain = fopchain_db.fetch_table_as_dataframe('fxoptchain')

        # Merge with FX inventory to get the option-streamer-symbol
        df_fxinventory = df_fxinventory.merge(
            df_fopchain[['symbol', 'streamer-symbol', 'strike-price']],
            on='symbol', how='left', suffixes=('', '_option')
        )

        # Update streamer-symbol for minifuture symbols
        minifuture_symbols = df_fxinventory[df_fxinventory['symbol'].str.startswith('/M')]['symbol'].unique()
        minifuture_streamers = df_masterfuturesdata[df_masterfuturesdata['symbol'].isin(minifuture_symbols)][['symbol', 'streamer-symbol', 'contract-size']]
        df_fxinventory = df_fxinventory.merge(minifuture_streamers, on='symbol', how='left', suffixes=('', '_minifuture'))
        df_fxinventory['streamer-symbol'] = df_fxinventory['streamer-symbol'].combine_first(df_fxinventory['streamer-symbol_minifuture'])
        df_fxinventory.drop(columns=['streamer-symbol_minifuture'], inplace=True)

        # Rename columns and fill contract-size where needed
        df_fxinventory.rename(columns={'streamer-symbol_option': 'option-streamer-symbol'}, inplace=True)
        df_fxinventory['contract-size'] = df_fxinventory.apply(
            lambda row: df_masterfuturesdata[df_masterfuturesdata['symbol'] == row['underlying-symbol']]['contract-size'].values[0]
            if pd.isna(row['contract-size']) else row['contract-size'], axis=1
        )

        # Save enriched FX inventory data
        if 'underlying-symbol' in df_fxinventory.columns:
            df_fxinventory = df_fxinventory.sort_values(by='underlying-symbol')
        self.inventory_fx_db.save_dataframe_to_table(df_fxinventory, 'fx_positions')
        print("Unique underlying symbols:", df_fxinventory['underlying-symbol'].unique().tolist())



# --------- Helper Function for Credentials Path ---------

def get_creds_path():
    """Determine the path for the credentials YAML file based on the platform."""
    ps = platform.system()
    return "creds.yaml" if ps == "Darwin" else "/home/ec2-user/tt/creds.yaml"

# --------- Main Program Execution ---------

def main():
    # Initialize paths and credentials
    creds_path = get_creds_path()  # For YAML credentials
    inventory_fx_db_path, masterdata_futures_db_path, inventory_db_path = DatabaseHandler.get_db_paths()  # For database files
    fopchain_db_path = "/home/ec2-user/tt/masterdata-fxoptchain.db" if platform.system() == "Linux" else "masterdata-fxoptchain.db"

    # Initialize API and Processor with the correct creds path
    api = TastyworksAPI(creds_path)
    processor = FXInventoryProcessor(api, inventory_db_path, inventory_fx_db_path, masterdata_futures_db_path, fopchain_db_path)

    # Run position feed and inventory update
    processor.run_position_feed()
    processor.update_inventory()

if __name__ == '__main__':
    main()
