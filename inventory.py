# eq and fop inventory

import platform
import sqlite3
import pandas as pd
import yaml
import requests
import json
from datetime import datetime
from discordwebhook import Discord

# --------- Authentication and Platform Functions ---------

def get_creds(file_path):
    """Retrieve credentials from a YAML file."""
    with open(file_path, "r") as file:
        data = yaml.safe_load(file)
        return {
            "discord_url_logs": data.get("discord_url_logs")[0],
            "pw": data.get("pw")[0],
            "user": data.get("user")[0]
        }

def pform():
    """Determine the platform and set the credentials file path accordingly."""
    ps = platform.system()
    file_path = "creds.yaml" if ps == "Darwin" else "/home/ec2-user/tt/creds.yaml"
    return ps, file_path

# --------- Discord Notifications ---------

def post_discord_message(url, message):
    """Send a message to a Discord webhook."""
    discord = Discord(url=url)
    discord.post(content=message)

# --------- API Session and Data Retrieval ---------

def get_auth_token(user, password):
    """Authenticate with Tastyworks API and return session token."""
    url = 'https://api.tastyworks.com/sessions'
    headers = {'Content-Type': 'application/json'}
    data = {"login": user, "password": password, "remember-me": True}
    response = requests.post(url, data=json.dumps(data), headers=headers)
    response_data = response.json()
    return response_data['data']['session-token']

def end_session(session_token):
    """End Tastyworks session."""
    url = 'https://api.tastyworks.com/sessions'
    headers = {'Authorization': session_token, 'Content-Type': 'application/json'}
    response = requests.delete(url, headers=headers)
    print('Session ended:', response.status_code)

def get_positions(session_token):
    """Retrieve positions data from Tastyworks API."""
    url = 'https://api.tastyworks.com/accounts/5WY49300/positions'
    headers = {'Authorization': session_token, 'Content-Type': 'application/json'}
    response = requests.get(url, headers=headers)
    return response.json()

# --------- Database Handling ---------

def connect_to_database(db_path):
    """Establish a connection to the SQLite database at the specified path."""
    return sqlite3.connect(db_path)

def save_dataframe_to_table(df, conn, table_name):
    """Save a pandas DataFrame to a specified table in the database."""
    df.to_sql(table_name, conn, if_exists='replace', index=False)

def fetch_table_as_dataframe(conn, table_name):
    """Fetch a specified table from the database as a pandas DataFrame."""
    return pd.read_sql(f'SELECT * FROM {table_name}', conn)

def get_db_paths():
    """Determine the platform and set the database paths accordingly."""
    ps = platform.system()
    if ps == "Linux":
        return "/home/ec2-user/tt/inventory-fx.db", "/home/ec2-user/tt/masterdata-futures.db"
    return "inventory-fx.db", "masterdata-futures.db"

# --------- Data Processing ---------

def filter_fx_positions(df):
    """Filter FX positions from the positions DataFrame."""
    return df[df['symbol'].str[2] == '6']

def map_symbols_to_streamers(df_fx, df_futures):
    """Map underlying symbols from FX inventory to streamer symbols."""
    symbol_to_streamer = dict(zip(df_futures['symbol'], df_futures['streamer-symbol']))
    df_fx['streamer-symbol'] = df_fx['underlying-symbol'].map(symbol_to_streamer)
    return df_fx

# --------- Main Position Feed and Database Update ---------

def position_feed():
    """Retrieve positions, store them in database, and send notification."""
    ps, file_path = pform()
    creds = get_creds(file_path)
    discord_url_logs = creds["discord_url_logs"]
    
    session_token = get_auth_token(creds["user"], creds["pw"])
    positions_data = get_positions(session_token)
    end_session(session_token)
    
    positions_df = pd.DataFrame(positions_data['data']['items'])
    inventory_db_path, _ = get_db_paths()
    
    # Store all positions
    with connect_to_database(inventory_db_path) as inventory_conn:
        save_dataframe_to_table(positions_df, inventory_conn, 'positions')
    
    # Filter FX positions and store them separately
    df_fxinventory = filter_fx_positions(positions_df)
    with connect_to_database(inventory_db_path) as inventory_fx_conn:
        save_dataframe_to_table(df_fxinventory, inventory_fx_conn, 'fx_positions')

    post_discord_message(discord_url_logs, "Inventory job done")
    return df_fxinventory

def main():
    """Main workflow to update FX inventory with additional streamer symbols and save."""
    inventory_fx_db_path, masterdata_futures_db_path = get_db_paths()

    with connect_to_database(inventory_fx_db_path) as inventory_fx_conn, \
         connect_to_database(masterdata_futures_db_path) as futures_conn:
        
        # Load FX inventory and futures data
        df_fxinventory = fetch_table_as_dataframe(inventory_fx_conn, 'fx_positions')
        df_masterfuturesdata = fetch_table_as_dataframe(futures_conn, 'masterdatafutures')
        
        # Update FX inventory with streamer symbols
        df_fxinventory = map_symbols_to_streamers(df_fxinventory, df_masterfuturesdata)

    # --------- Additional Processing: Merging Option Data ---------
    
    # Load the fopchain table from the masterdata-fxoptchain.db database
    ps = platform.system()
    fopchain_db_path = "masterdata-fxoptchain.db" if ps != "Linux" else "/home/ec2-user/tt/masterdata-fxoptchain.db"
    with connect_to_database(fopchain_db_path) as conn_fopchain:
        df_fopchain = fetch_table_as_dataframe(conn_fopchain, 'fxoptchain')
    
    # df_fopchain.to_clipboard()
    # Merge with the FX inventory to get the option-streamer-symbol
    df_fxinventory = df_fxinventory.merge(
        df_fopchain[['symbol', 'streamer-symbol', 'strike-price']],
        on='symbol',
        how='left',
        suffixes=('', '_option')
    )

    # Update the streamer-symbol for minifuture symbols from masterdatafutures
    minifuture_symbols = df_fxinventory[df_fxinventory['symbol'].str.startswith('/M')]['symbol'].unique()
    # df_masterfuturesdata.to_clipboard()
    minifuture_streamers = df_masterfuturesdata[df_masterfuturesdata['symbol'].isin(minifuture_symbols)][['symbol', 'streamer-symbol', 'contract-size']]

    # Merge to get the streamer-symbol for minifuture symbols
    # minifuture_streamers.to_clipboard()
    df_fxinventory = df_fxinventory.merge(minifuture_streamers, on='symbol', how='left', suffixes=('', '_minifuture'))

    # Update the streamer-symbol column where it is NaN
    df_fxinventory['streamer-symbol'] = df_fxinventory['streamer-symbol'].combine_first(df_fxinventory['streamer-symbol_minifuture'])

    # Drop the temporary minifuture streamer-symbol column
    df_fxinventory.drop(columns=['streamer-symbol_minifuture'], inplace=True)
    
    # Rename the merged streamer-symbol column to option-streamer-symbol
    df_fxinventory.rename(columns={'streamer-symbol_option': 'option-streamer-symbol'}, inplace=True)
    df_fxinventory = df_fxinventory.sort_values(by='underlying-symbol')
    # Fill empty contract-size in df_fxinventory using values from df_masterfuturesdata
    df_fxinventory['contract-size'] = df_fxinventory.apply(
        lambda row: df_masterfuturesdata[df_masterfuturesdata['symbol'] == row['underlying-symbol']]['contract-size'].values[0]
        if pd.isna(row['contract-size']) else row['contract-size'], axis=1
    )

    # --------- Save the enriched FX inventory data ---------
    with connect_to_database(inventory_fx_db_path) as inventory_fx_conn:
        save_dataframe_to_table(df_fxinventory, inventory_fx_conn, 'fx_positions')
        
    # Output updated results
    print("Unique underlying symbols:", df_fxinventory['underlying-symbol'].unique().tolist())

if __name__ == '__main__':
    # Run the position feed job
    position_feed()
    
    # Update the FX inventory with additional data
    main()
