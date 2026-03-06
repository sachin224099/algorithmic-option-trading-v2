from kiteconnect import KiteConnect
from data.futures_master_loader import FuturesMasterLoader
from exception.exception_handler import handle_exceptions
import pandas as pd
from core.config_loader import Config

class ZerodhaClient:
    """
    Centralized Zerodha API client.
    Reads credentials from config object.
    """

    def __init__(self, config: Config):
        self.config = config
        api_key, access_token = config.get_api_credentials()

        self.kite = KiteConnect(api_key=api_key)
        self.kite.set_access_token(access_token)

        print("✅ Zerodha session initialized")

        # -------------------------------------------------
        # 2️⃣ Load Futures Master Using Loader Class
        # -------------------------------------------------
        self.futures_master_loader = FuturesMasterLoader(self)
        self.futures_master = self.futures_master_loader.load()

        print("✅ Futures master loaded into ZerodhaClient")

    @handle_exceptions
    def get_kite(self):
        return self.kite

    @handle_exceptions
    def load_all_instruments(self):
        return pd.DataFrame(self.kite.instruments())

    @handle_exceptions
    def load_nfo_instruments(self, exchange="NFO"):
        return pd.DataFrame(self.kite.instruments(exchange))

    @handle_exceptions
    def load_nfo_futures_instruments(self):
        instruments = self.load_nfo_instruments("NFO")
        if instruments is None or instruments.empty:
            return None
        instruments = instruments[(instruments["segment"] == "NFO-FUT")  & (instruments["expiry"] == self.config.get_expiry_date())]
        return instruments

    def get_stock_spot_prices(self, symbols, exchange="NSE"):
        """
        Fetch LTP for multiple symbols in single API call.

        Parameters:
            symbols (list): ["RELIANCE", "HDFCBANK"]
            exchange (str): NSE / NFO

        Returns:
            dict:
            {
                "RELIANCE": 2825.2,
                "HDFCBANK": 1520.5
            }
        """

        if not symbols:
            return {}

        instruments = [f"{exchange}:{symbol}" for symbol in symbols]

        try:
            response = self.kite.ltp(instruments)

            #print(f"Response: {response}")

            ltp_data = {}

            for key, value in response.items():
                # key format: NSE:RELIANCE
                symbol = key.split(":")[1]
                ltp_data[symbol] = value["last_price"]

            return ltp_data

        except Exception as e:
            print("❌ LTP fetch failed:", e)
            return {}
