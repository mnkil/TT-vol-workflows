from datetime import datetime, timezone, timedelta
import numpy as np
import pandas as pd
from scipy.optimize import brentq
from scipy.stats import norm
from websocket_init import TastyworksSession
from websocket_streamer import MarketDataProcessor
from inventory_handler import TastyworksAPI, DatabaseHandler, FXInventoryProcessor
import platform
from discordwebhook import Discord
import yaml

# Constants
CENTRAL_TIMEZONE = timezone(timedelta(hours=-6))
RISK_FREE_RATE = 0  # Assuming risk-free rate r = 0
PRECISION_VOLATILITY = 1  # Implied volatility displayed to 1 decimal place
PRECISION_DELTA = 2  # Delta displayed to 2 decimal places
YEAR = 365

class BSM:
    """ """
    @staticmethod
    def price(S, K, T, r, sigma, option_type='put'):
        """BSM price"""
        d1 = (np.log(S / K) + (r + 0.5 * sigma**2) * T) / (sigma * np.sqrt(T))
        d2 = d1 - sigma * np.sqrt(T)
        if option_type == 'call':
            return S * norm.cdf(d1) - K * np.exp(-r * T) * norm.cdf(d2)
        elif option_type == 'put':
            return K * np.exp(-r * T) * norm.cdf(-d2) - S * norm.cdf(-d1)
        else:
            raise ValueError("Invalid option type. Use 'call' or 'put'.")

    @staticmethod
    def implied_volatility(option_price, S, K, T, r, option_type='put'):
        """Calculates the implied volatility for an option."""
        def objective_function(sigma):
            return BSM.price(S, K, T, r, sigma, option_type) - option_price
        try:
            return brentq(objective_function, 1e-6, 1, xtol=1e-6)
        except ValueError:
            return np.nan  # Return NaN if brentq fails

    @staticmethod
    def delta(S, K, T, r, sigma, option_type='put'):
        """Calculates the BSM delta for an option."""
        d1 = (np.log(S / K) + (r + 0.5 * sigma**2) * T) / (sigma * np.sqrt(T))
        if option_type == 'call':
            return norm.cdf(d1)
        elif option_type == 'put':
            return norm.cdf(d1) - 1
        else:
            raise ValueError("Invalid option type. Use 'call' or 'put'.")

class OptionDataProcessor:
    """Processes options data and applies BSM."""

    def __init__(self, df, creds_path):
        self.df = df.copy()  # Original DataFrame copy for merging
        self.local_tz = datetime.now().astimezone().tzinfo
        self.now_central = datetime.now(timezone.utc).astimezone(CENTRAL_TIMEZONE)
        self.creds = self._load_creds(creds_path)
        self.discord_url_logs = self.creds["discord_url_logs"]

    def _load_creds(self, file_path):
        """Retrieve credentials from a YAML file."""
        with open(file_path, "r") as file:
            data = yaml.safe_load(file)
            return {
                "discord_url_logs": data.get("discord_url_logs")[0],
            }
        
    def post_discord_message(self, url, message):
        """Send a message to a Discord webhook."""
        discord = Discord(url=url)
        discord.post(content=message)

    def filter_instrument_type(self, instrument_type='Future Option'):
        """Filters the DataFrame by a specified instrument type."""
        self.df = self.df[self.df['instrument-type'] == instrument_type].copy()

    def convert_to_datetime(self, column_name):
        """Converts a specified column to datetime in the DataFrame."""
        self.df[column_name] = pd.to_datetime(self.df[column_name])

    def calculate_time_to_expiry(self):
        """Calculates time to expiry in years and adds it as a new column 'T'."""
        self.df['T'] = (self.df['expires-at'] - self.now_central).dt.total_seconds() / (YEAR * 24 * 60 * 60)

    def calculate_implied_volatility(self):
        """Applies the implied volatility calculation for each option in the DataFrame."""
        self.df['implied_volatility'] = self.df.apply(
            lambda row: BSM.implied_volatility(
                float(row['mid_option']),
                float(row['mid_future']),
                float(row['strike-price']),
                row['T'],
                RISK_FREE_RATE,
                'call' if 'C' in row['symbol'][-10:] else 'put'
            ), 
            axis=1
        )
        self.df['implied_volatility'] = (self.df['implied_volatility'] * 100).round(PRECISION_VOLATILITY)  # Convert to percentage

    def calculate_delta(self):
        """Applies the delta calculation for each option in the DataFrame."""
        self.df['delta'] = self.df.apply(
            lambda row: BSM.delta(
                float(row['mid_future']),
                float(row['strike-price']),
                row['T'],
                RISK_FREE_RATE,
                row['implied_volatility'] / 100,  # Convert back to decimal
                'call' if 'C' in row['symbol'][-10:] else 'put'
            ), 
            axis=1
        )
        self.df['delta'] = self.df['delta'].round(PRECISION_DELTA)
    
    def calculate_pos_delta(self):
        """Calculates position delta based on delta, quantity direction, and instrument type."""
        self.df['pos_delta'] = self.df.apply(
            lambda row: (
                row['delta'] if row['instrument-type'] == 'Future Option' and row['quantity-direction'] == 'Long'
                else -row['delta'] if row['instrument-type'] == 'Future Option' and row['quantity-direction'] == 'Short'
                else 0
            ),
            axis=1
        )
        
    def assign_pos_delta_for_futures(self, df):
        """Assigns pos_delta values for rows with instrument-type 'Future'."""
        df.loc[df['instrument-type'] == 'Future', 'pos_delta'] = df.apply(
            lambda row: 1 if row['quantity-direction'] == 'Long' else -1,
            axis=1
        )
        return df
    
    def calculate_bc_delta(self):
        """Calculates bc_delta as pos_delta * quantity * contract-size."""
        # Ensure 'quantity' and 'contract-size' are numeric, coerce errors to NaN
        self.df['quantity'] = pd.to_numeric(self.df['quantity'], errors='coerce')
        self.df['contract-size'] = pd.to_numeric(self.df['contract-size'], errors='coerce')
    
        # Calculate bc_delta only where pos_delta, quantity, and contract-size are all numeric
        self.df['bc_delta'] = self.df['pos_delta'] * self.df['quantity'] * self.df['contract-size']

    def assign_ccy(self):
        """Creates a CCY column from the underlying-symbol column by extracting two characters after '6'."""
        self.df['CCY'] = self.df['underlying-symbol'].str.extract(r'(6\w{1})')[0]

    def summarize_bc_delta_by_ccy(self):
        """Summarizes bc_delta by CCY and returns a new DataFrame."""
        summary_df = self.df.groupby('CCY')['bc_delta'].sum().reset_index()
        summary_df.rename(columns={'bc_delta': 'total_bc_delta'}, inplace=True)
        return summary_df

    def merge_with_original(self, original_df):
        """Merges the processed 'Future Option' rows with the original DataFrame and removes duplicates."""
        return pd.concat([self.df, original_df], ignore_index=True).drop_duplicates(subset=['symbol'])

    def process(self):
        """Main function to execute the processing workflow."""
        # Keep a copy of the original DataFrame
        original_df = self.df.copy()

        # Step 1: Filter, Convert, and Calculate Time to Expiry
        self.filter_instrument_type()
        self.convert_to_datetime('expires-at')
        self.calculate_time_to_expiry()

        # Step 2: Calculate Implied Volatility and Delta
        self.calculate_implied_volatility()
        self.calculate_delta()
        self.calculate_pos_delta()

        # Step 3: Merge Processed Data with Original DataFrame
        processed_df = self.merge_with_original(original_df)
        
        # Step 4: Assign pos_delta for 'Future' type rows in the merged DataFrame
        processed_df = self.assign_pos_delta_for_futures(processed_df)

        # Step 5: Calculate bc_delta as pos_delta * quantity * contract-size
        self.df = processed_df  # Update self.df to the merged DataFrame for bc_delta calculation
        self.calculate_bc_delta()

        # Step 6: Create the CCY column
        self.assign_ccy()

        # Step 7: Generate bc_delta summary by CCY
        summary_df = self.summarize_bc_delta_by_ccy()
        self.post_discord_message(self.discord_url_logs, summary_df.to_string(index=False))

        return processed_df, summary_df


def main():
    creds_path = "creds.yaml" if platform.system() == "Darwin" else "/home/ec2-user/tt/creds.yaml"
    inventory_fx_db_path, masterdata_futures_db_path, inventory_db_path = DatabaseHandler.get_db_paths()
    fopchain_db_path = "/home/ec2-user/tt/masterdata-fxoptchain.db" if platform.system() == "Linux" else "masterdata-fxoptchain.db"
    api = TastyworksAPI(creds_path)
    inv_processor = FXInventoryProcessor(api, inventory_db_path, inventory_fx_db_path, masterdata_futures_db_path, fopchain_db_path)
    inv_processor.run_position_feed()
    inv_processor.update_inventory()
    
    session = TastyworksSession()
    streamer_token = session.run()
    processor = MarketDataProcessor(streamer_token['data']['token'])
    df_fxinventory = processor.process_market_data()
    processor = OptionDataProcessor(df_fxinventory, creds_path)
    df_fxinventory = processor.process()
    return df_fxinventory

if __name__ == "__main__":
    df_fxinventory, df_exp = main()
    print(df_exp)
    df_exp
    
    
