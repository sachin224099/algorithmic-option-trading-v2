import pandas as pd
from datetime import datetime
from data.historic_data import get_minutes_data
from indicators.indicator_metrics_calculator import calculate_oi_change_pct


class EntryMonitor:
    """
    Monitor shortlisted symbols and confirm entry using 5-minute candles.
    """

    def __init__(self, zerodha_client, config):
        self.client = zerodha_client
        self.config = config

        self.volume_multiplier = config.get("entry", "volume_multiplier")
        self.lookback = config.get("entry", "lookback_candles")

    # -------------------------------------------------------
    # PUBLIC METHOD
    # -------------------------------------------------------

    def monitor(self, trade_candidates):
        """
        Monitor all shortlisted trades.

        Parameters:
            trade_candidates (list): symbols selected from 15-min scan

        Returns:
            list of confirmed entry signals
        """

        confirmed_entries = []

        for trade in trade_candidates:

            symbol = trade["symbol"]
            instrument_token = trade["instrument_token"]
            direction = trade["signal"]
            option_symbol = trade["option_symbol"]
            option_instrument_token = trade["option_instrument_token"]

            df = self._get_5min_data(symbol, instrument_token)

            if df is None:
                continue

            #df = df.iloc[-1:].copy()
            #option_df = self._get_5min_data(option_symbol, option_instrument_token)
            entry = self._check_entry_conditions(
                symbol,
                direction,
                option_symbol,
                df
            )

            if entry:
                confirmed_entries.append(entry)

        return confirmed_entries

    # -------------------------------------------------------
    # FETCH 5 MIN DATA
    # -------------------------------------------------------

    def _get_5min_data(self, symbol, instrument_token):

        try:

            to_date = datetime.now()
            from_date = to_date.replace(hour=9, minute=15)

            df = get_minutes_data(self.client.kite, instrument_token, from_date, to_date, 5, oi=True)
            return df

        except Exception as e:
            print(f"❌ Failed to fetch 5m data for {symbol}: {e}")
            return None

    # -------------------------------------------------------
    # ENTRY LOGIC
    # -------------------------------------------------------

    def _check_entry_conditions(self, symbol, direction, option_symbol, df):

        df = df.sort_values("date")

        latest = df.iloc[-1]
        prev = df.iloc[-2]

        # -----------------------------------------------
        # MOMENTUM BREAKOUT
        # -----------------------------------------------

        bullish_break = latest["close"] > prev["high"]
        bearish_break = latest["close"] < prev["low"]

        #print(symbol, bullish_break, bearish_break)
        # -----------------------------------------------
        # VOLUME CONFIRMATION
        # -----------------------------------------------

        avg_volume = df.iloc[-self.lookback:-1]["volume"].mean()

        volume_spike = latest["volume"] > avg_volume * self.volume_multiplier

        # -----------------------------------------------
        # ENTRY DECISION
        # -----------------------------------------------


        oi_percentage_change = calculate_oi_change_pct(df)
        #print(symbol, oi_percentage_change)
        #print(symbol, self._get_option_ltp(option_symbol))
        if direction == "CE":

            if bullish_break and volume_spike and oi_percentage_change > 0.3:

                return {
                    "symbol": symbol,
                    "option_symbol": option_symbol,
                    "direction": direction,
                    "oi_percentage_change_5min": oi_percentage_change,
                    "entry_price": self._get_option_ltp(option_symbol),
                    "timestamp": datetime.now()
                }

        elif direction == "PE":

            if bearish_break and volume_spike and oi_percentage_change > 0.3:

                return {
                    "symbol": symbol,
                    "option_symbol": option_symbol,
                    "direction": direction,
                    "oi_percentage_change_5min": oi_percentage_change,
                    "entry_price": self._get_option_ltp(option_symbol),
                    "timestamp": datetime.now()
                }

        return None

    # -------------------------------------------------------
    # OPTION LTP
    # -------------------------------------------------------

    def _get_option_ltp(self, option_symbol):

        try:

            response = self.client.get_option_ltp(option_symbol)
            
            # Response key format is "NFO:option_symbol"
            instrument_key = f"NFO:{option_symbol}"
            
            if instrument_key in response:
                return response[instrument_key].get("last_price")
            
            return None

        except Exception as e:

            print(f"❌ Failed to fetch option LTP {option_symbol}: {e}")

            return None