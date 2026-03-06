from data.zerodha_client import ZerodhaClient
import pandas as pd
from data.historic_data import get_minutes_data
from datetime import datetime, timedelta

class OptionsMetrics:

    def __init__(self, zerodhaClient: ZerodhaClient) -> None:
        self.zerodhaClient = zerodhaClient


    def populate_options_metrics(self, options) -> pd.Series:
        """
        Populate options metrics for a single option (Series) or DataFrame.
        
        Parameters:
            options: pandas Series (single option) or DataFrame (multiple options)
        
        Returns:
            pandas Series with metrics populated
        """
        
        # Handle Series (single option) - most common case
        if isinstance(options, pd.Series):
            #print(f"Populating options metrics for {options['tradingsymbol']}")
            df = self.get_historic_data(options["instrument_token"])
            if df is not None and len(df) > 0:
                closed_df = df.iloc[:-1].copy()
                options["volume_spike"] = self.calculate_volume_spike(closed_df)
            else:
                options["volume_spike"] = 0
            return options
        
        # Handle DataFrame (multiple options)
        if isinstance(options, pd.DataFrame):
            for index, row in options.iterrows():
                #print(f"Populating options metrics for {row['tradingsymbol']}")
                df = self.get_historic_data(row["instrument_token"])
                if df is not None and len(df) > 0:
                    closed_df = df.iloc[:-1].copy()
                    volume_spike = self.calculate_volume_spike(closed_df)
                    options.at[index, "volume_spike"] = volume_spike
                else:
                    options.at[index, "volume_spike"] = 0
            return options
        
        return options       


    def get_historic_data(self, instrumentsToken: str):
        to_date = datetime.now()
        from_date = to_date - timedelta(days=3)
        return get_minutes_data(self.zerodhaClient.kite, instrumentsToken, from_date, to_date, 15, oi=True)

    def calculate_volume_spike(self, df: pd.DataFrame) -> float:
        """
        Calculate volume spike ratio using last candle volume
        compared with average of previous 5 candles.
        """

        if len(df) < 6:
            return 0

        avg_volume = df.iloc[-6:-1]["volume"].mean()
        current_volume = df.iloc[-1]["volume"]
        print(f"Average volume: {avg_volume}, Current volume: {current_volume}")

        if avg_volume == 0:
            return 0

        return round(current_volume / avg_volume, 2)