from kiteconnect import KiteConnect
import pandas as pd
from datetime import datetime, timedelta
from exception.exception_handler import handle_exceptions

@handle_exceptions
def fetch_intraday_data(kite: KiteConnect, instrument_token: int):
    today = datetime.now().date()

    data = kite.historical_data(
        instrument_token=instrument_token,
        from_date=datetime(today.year, today.month, today.day, 9, 15),
        to_date=datetime.now(),
        interval="minute"
    )

    df = pd.DataFrame(data)
    df["date"] = pd.to_datetime(df["date"])

    # Ensure only today data
    df = df[df["date"].dt.date == today]

    return df


@handle_exceptions
def get_minutes_data(kite, instrument_token, from_date, to_date, TIMEFRAME_MINUTES=15, oi=False):

    return pd.DataFrame(
        kite.historical_data(
            instrument_token,
            from_date,
            to_date,
            f"{TIMEFRAME_MINUTES}minute",
            oi=oi
        )
    )    