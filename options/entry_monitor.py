import pandas as pd
from datetime import datetime


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
            direction = trade["signal"]
            option_symbol = trade["option_symbol"]

            df = self._get_5min_data(symbol)

            if df is None:
                continue

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

    def _get_5min_data(self, symbol):

        try:

            to_date = datetime.now()
            from_date = to_date.replace(hour=9, minute=15)

            df = self.client.get_futures_historical(
                stock_symbol=symbol,
                from_date=from_date,
                to_date=to_date,
                interval="5minute"
            )

            if df is None or len(df) < 5:
                return None

            df = pd.DataFrame(df)

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

        # -----------------------------------------------
        # VOLUME CONFIRMATION
        # -----------------------------------------------

        avg_volume = df.iloc[-self.lookback:-1]["volume"].mean()

        volume_spike = latest["volume"] > avg_volume * self.volume_multiplier

        # -----------------------------------------------
        # ENTRY DECISION
        # -----------------------------------------------

        if direction == "BUY_CE":

            if bullish_break and volume_spike:

                return {
                    "symbol": symbol,
                    "option_symbol": option_symbol,
                    "direction": direction,
                    "entry_price": self._get_option_ltp(option_symbol),
                    "timestamp": datetime.now()
                }

        elif direction == "BUY_PE":

            if bearish_break and volume_spike:

                return {
                    "symbol": symbol,
                    "option_symbol": option_symbol,
                    "direction": direction,
                    "entry_price": self._get_option_ltp(option_symbol),
                    "timestamp": datetime.now()
                }

        return None

    # -------------------------------------------------------
    # OPTION LTP
    # -------------------------------------------------------

    def _get_option_ltp(self, option_symbol):

        try:

            response = self.client.get_ltp_multiple(
                [option_symbol],
                exchange="NFO"
            )

            return response.get(option_symbol)

        except Exception as e:

            print(f"❌ Failed to fetch option LTP {option_symbol}: {e}")

            return None