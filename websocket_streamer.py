#!/usr/bin/env python3

import websocket
import ssl
import json
import pandas as pd
import sqlite3
import os
from typing import List, Tuple, Any
import platform
from websocket_init import TastyworksSession

class MarketDataProcessor:
    def __init__(self, token: str):
        self.db_path, self.master_db_path = self.get_db_paths()
        self.ws_url = 'wss://tasty-openapi-ws.dxfeed.com/realtime'
        self.channel_number = 3
        self.token = token
        self.ws_client = MarketDataWebSocket(self.ws_url, self.token, self.channel_number)
        self.columns_to_check = [
            'bidPrice', 'askPrice', 'bidSize', 'askSize', 
            'bidPrice_future', 'askPrice_future', 'bidSize_future', 'askSize_future', 
            'streamer-symbol-option', 'mid_option', 'mid_future'
        ]
        self.numeric_columns = ['bidPrice', 'askPrice', 'bidPrice_future', 'askPrice_future']
        
    def get_db_paths(self) -> Tuple[str, str]:
        """Determine platform and set database paths accordingly."""
        ps = platform.system()
        if ps == "Linux":
            return "/home/ec2-user/tt/inventory-fx.db", "/home/ec2-user/tt/masterdata-futures.db"
        return "inventory-fx.db", "masterdata-futures.db"

    def get_streamer_symbols(self) -> List[str]:
        """Retrieve symbols from the database to track."""
        conn = sqlite3.connect(self.db_path)
        df = pd.read_sql('SELECT * FROM fx_positions', conn)
        conn.close()
        streamer_symbols = df['streamer-symbol'].unique().tolist()
        option_streamer_symbols = df['option-streamer-symbol'].unique().tolist()
        print(symbol for symbol in streamer_symbols + option_streamer_symbols if symbol and symbol != 'None')
        return [symbol for symbol in streamer_symbols + option_streamer_symbols if symbol and symbol != 'None']

    def parse_market_data(self, received_data: List[Tuple[str, List[Any]]]) -> pd.DataFrame:
        """Parse received WebSocket market data into a DataFrame."""
        parsed_data = []
        for item in received_data:
            feed_type, market_data = item
            for i in range(0, len(market_data), 6):
                if i + 5 < len(market_data):
                    parsed_data.append([
                        feed_type,
                        market_data[i],
                        market_data[i+1],
                        market_data[i+2],
                        market_data[i+3],
                        market_data[i+4],
                        market_data[i+5]
                    ])
        df_parsed = pd.DataFrame(parsed_data, columns=[
            'eventType', 'eventType2', 'streamer-symbol', 'bidPrice', 'askPrice', 'bidSize', 'askSize'
        ])
        return df_parsed.drop_duplicates(subset='streamer-symbol', keep='last')

    def read_inventory_from_db(self) -> pd.DataFrame:
        """Read the inventory data from the database."""
        conn = sqlite3.connect(self.db_path)
        df = pd.read_sql('SELECT * FROM fx_positions', conn)
        conn.close()
        return df

    def drop_existing_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Drop specified columns if they exist in the DataFrame."""
        return df.drop(columns=[col for col in self.columns_to_check if col in df.columns], inplace=False)

    def merge_data(self, df_inventory: pd.DataFrame, df_parsed: pd.DataFrame) -> pd.DataFrame:
        """Merge parsed market data into the inventory DataFrame."""
        df_inventory = df_inventory.merge(
            df_parsed[['streamer-symbol', 'bidPrice', 'askPrice', 'bidSize', 'askSize']],
            left_on='option-streamer-symbol',
            right_on='streamer-symbol',
            how='left',
            suffixes=('', '-option')
        )
        return df_inventory.merge(
            df_parsed[['streamer-symbol', 'bidPrice', 'askPrice', 'bidSize', 'askSize']],
            left_on='streamer-symbol',
            right_on='streamer-symbol',
            how='left',
            suffixes=('', '_future')
        )

    def convert_columns_to_numeric(self, df: pd.DataFrame) -> pd.DataFrame:
        """Convert specified columns to numeric, coercing errors."""
        for column in self.numeric_columns:
            df[column] = pd.to_numeric(df[column], errors='coerce')
        return df

    def calculate_mid_prices(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate mid prices for options and futures."""
        df['mid_option'] = (df['bidPrice'] + df['askPrice']) / 2
        df['mid_future'] = (df['bidPrice_future'] + df['askPrice_future']) / 2
        return df

    def save_inventory_to_db(self, df: pd.DataFrame):
        """Save the updated inventory DataFrame back to the database."""
        conn = sqlite3.connect(self.db_path)
        df = df.astype({col: 'str' for col in df.columns if df[col].dtype == 'object'})
        df.to_sql('fx_positions', conn, if_exists='replace', index=False)
        conn.close()

    def reorder_columns(self, df: pd.DataFrame, new_columns: list) -> pd.DataFrame:
        """Reorder the DataFrame columns to place new columns at the end."""
        existing_columns = [col for col in df.columns if col not in new_columns]
        return df[existing_columns + new_columns]

    def process_market_data(self) -> pd.DataFrame:
        """Main function to process WebSocket market data and update inventory."""
        symbols = self.get_streamer_symbols()
        self.ws_client.set_symbols_to_track(symbols)
        self.ws_client.connect()

        df_parsed = self.parse_market_data(self.ws_client.received_data)
        df_inventory = self.read_inventory_from_db()
        df_inventory = self.drop_existing_columns(df_inventory)
        df_inventory = self.merge_data(df_inventory, df_parsed)
        df_inventory = self.convert_columns_to_numeric(df_inventory)
        df_inventory = self.calculate_mid_prices(df_inventory)
        self.save_inventory_to_db(df_inventory)

        return self.reorder_columns(df_inventory, ['mid_option', 'mid_future'])

class MarketDataWebSocket:
    def __init__(self, ws_url: str, token: str, channel_number: int):
        self.ws_url = ws_url
        self.token = token
        self.channel_number = channel_number
        self.symbols_to_track = {}
        self.received_data = []

    def connect(self):
        headers = {'Authorization': f'Bearer {self.token}'}
        websocket.enableTrace(False)
        self.ws = websocket.WebSocketApp(
            self.ws_url,
            header=headers,
            on_open=self.on_open,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close
        )
        self.ws.run_forever(sslopt={"cert_reqs": ssl.CERT_NONE})

    def on_open(self, ws):
        print("### connection opened ###")
        setup_message = {
            "type": "SETUP",
            "channel": 0,
            "version": "0.1-DXF-JS/0.3.0",
            "keepaliveTimeout": 15,
            "acceptKeepaliveTimeout": 20
        }
        ws.send(json.dumps(setup_message))

    def on_message(self, ws, message):
        data = json.loads(message)
        # print(f"Received message: {data}")  # Debugging statement

        # Handle authentication if needed
        if data.get('type') == 'AUTH_STATE' and data.get('state') == 'UNAUTHORIZED':
            ws.send(json.dumps({"type": "AUTH", "channel": 0, "token": self.token}))
        elif data.get('type') == 'AUTH_STATE' and data.get('state') == 'AUTHORIZED':
            ws.send(json.dumps({
                "type": "CHANNEL_REQUEST",
                "channel": self.channel_number,
                "service": "FEED",
                "parameters": {"contract": "AUTO"}
            }))
        
        # Setup the feed once the channel is opened
        elif data.get('type') == 'CHANNEL_OPENED' and data.get('channel') == self.channel_number:
            ws.send(json.dumps({
                "type": "FEED_SETUP",
                "channel": self.channel_number,
                "acceptAggregationPeriod": 0.1,
                "acceptDataFormat": "COMPACT",
                "acceptEventFields": {
                    "Quote": ["eventType", "eventSymbol", "bidPrice", "askPrice", "bidSize", "askSize"]
                }
            }))
        
        # Subscribe to symbols
        elif data.get('type') == 'FEED_CONFIG' and data.get('channel') == self.channel_number:
            ws.send(json.dumps({
                "type": "FEED_SUBSCRIPTION",
                "channel": self.channel_number,
                "reset": True,
                "add": [{"type": "Quote", "symbol": symbol} for symbol in self.symbols_to_track]
            }))
        
        # Process received data and track symbols
        elif data.get('type') == 'FEED_DATA' and data.get('channel') == self.channel_number:
            feed_data = data['data']
            self.received_data.append(feed_data)  # Ensure data is appended correctly
            # print(f"Appended to received_data: {feed_data}")  # Debugging statement

            feed_type, market_data = feed_data

            if feed_type == "Quote":
                for i in range(1, len(market_data), 6):
                    symbol = market_data[i]
                    if symbol in self.symbols_to_track:
                        self.symbols_to_track[symbol] = True
                        # print(f"Tracking data received for symbol: {symbol}")  # Debugging statement

            # Check if data has been received for all symbols and close if so
            if self.check_all_data_received():
                print("All market data received. Closing the WebSocket.")
                ws.close()

    def on_error(self, ws, error):
        print(f"Error: {error}")

    def on_close(self, ws, close_status_code, close_msg):
        print(f"### connection closed ### with status code: {close_status_code} and message: {close_msg}")

    def check_all_data_received(self):
        """Check if data has been received for all tracked symbols."""
        all_received = all(self.symbols_to_track.values())
        print(f"Check all data received: {all_received}")  # Debugging statement
        return all_received

    def set_symbols_to_track(self, symbols: List[str]):
        """Initialize the symbols to track, marking each as not yet received."""
        self.symbols_to_track = {symbol: False for symbol in symbols}
        print(f"Symbols to track initialized: {self.symbols_to_track}")  # Debugging statement


if __name__ == "__main__":
    session = TastyworksSession()
    streamer_token = session.run()
    print("Streamer Token Data:", streamer_token['data']['token'])
    token = streamer_token['data']['token']
    processor = MarketDataProcessor(token)
    df_fxinventory = processor.process_market_data()
    # df_fxinventory.to_clipboard()
    

