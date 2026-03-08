import os
import pandas as pd
from datetime import datetime
from exception.exception_handler import handle_exceptions



class FuturesMasterLoader:
    """
    Handles:
        - Creating futures master CSV (first run)
        - Loading futures master from CSV (subsequent runs)
        - Selecting nearest expiry futures
    """

    def __init__(self, zerodha_client, cache_path="data_cache/futures_master.csv"):
        self.client = zerodha_client
        self.cache_path = cache_path

        # Ensure directory exists
        os.makedirs(os.path.dirname(self.cache_path), exist_ok=True)

    # ---------------------------------------------------------
    # PUBLIC METHOD
    # ---------------------------------------------------------

    @handle_exceptions
    def load(self):
        """
        Main entry method.
        Creates CSV if not exists.
        Otherwise loads from cache.
        """

        if not os.path.exists(self.cache_path) or os.path.getsize(self.cache_path) == 0:
            print("⚡ Futures master not found. Creating new one...")
            self._create_master()

        print("✅ Loading futures master from cache")
        return self._load_from_csv()

    # ---------------------------------------------------------
    # CREATE MASTER FILE
    # ---------------------------------------------------------

    @handle_exceptions
    def _create_master(self):
        print("Creating futures master...")
        instruments = self.client.load_nfo_futures_instruments()

        if instruments is None or instruments.empty:
            raise Exception("Failed to fetch instruments from Zerodha")

        # Select and rename only the columns we need
        df = instruments[["name", "tradingsymbol", "instrument_token", "expiry", "lot_size"]].copy()
        df.columns = [
            "symbol",
            "futures_symbol",
            "instrument_token",
            "expiry",
            "lot_size"
        ]

        df.to_csv(self.cache_path, index=False)

        print(f"📁 Futures master created at {self.cache_path}")

    # ---------------------------------------------------------
    # LOAD CSV INTO MEMORY
    # ---------------------------------------------------------

    @handle_exceptions
    def _load_from_csv(self):
        df = pd.read_csv(self.cache_path)
        # Ensure instrument_token is int
        df["instrument_token"] = df["instrument_token"].astype(int)
        return df