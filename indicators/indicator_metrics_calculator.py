import pandas as pd
import numpy as np
from exception.exception_handler import handle_exceptions


def calculate_volume_spike_ratio(df, lookback=12):
    """
    Calculate volume spike ratio for latest completed candle.

    Parameters:
        df (pd.DataFrame): Must contain 'volume' column
        lookback (int): Number of previous candles to calculate average

    Returns:
        float: volume_spike_ratio
        None: if insufficient data
    """

    if df is None or len(df) < lookback + 1:
        return None

    # Ensure sorted by time
    df = df.sort_index()

    # Latest completed candle
    latest_volume = df.iloc[-1]["volume"]

    # Previous candles (exclude latest)
    previous_volumes = df.iloc[-(lookback + 1):-1]["volume"]

    avg_volume = previous_volumes.mean()

    if avg_volume == 0:
        return None

    volume_spike_ratio = latest_volume / avg_volume

    return round(volume_spike_ratio, 2)


def calculate_price_change_pct(df):
    """
    Calculate percentage price change between last two completed candles.

    Required column:
        'close'

    Returns:
        float: price_change_pct
        None: if insufficient data
    """

    if df is None or len(df) < 2:
        return None

    df = df.sort_index()

    prev_close = df.iloc[-2]["close"]
    curr_close = df.iloc[-1]["close"]

    if prev_close == 0:
        return None

    price_change_pct = ((curr_close - prev_close) / prev_close) * 100

    return round(price_change_pct, 2)


def calculate_oi_change_pct(df):
    """
    Calculate percentage OI change between last two completed candles.

    Required column:
        'oi'

    Returns:
        float: oi_change_pct
        None: if insufficient data
    """

    if df is None or len(df) < 2:
        return None

    if "oi" not in df.columns:
        return None

    df = df.sort_index()

    prev_oi = df.iloc[-2]["oi"]
    curr_oi = df.iloc[-1]["oi"]

    if prev_oi == 0:
        return None

    oi_change_pct = ((curr_oi - prev_oi) / prev_oi) * 100

    return round(oi_change_pct, 2)

def classify_oi_structure(price_change_pct, oi_change_pct, min_threshold=0.5):
    """
    Classify OI structure based on price and OI percentage change.

    Parameters:
        price_change_pct (float): % change in price
        oi_change_pct (float): % change in OI
        min_threshold (float): minimum change to consider meaningful

    Returns:
        str: One of
            - LONG_BUILDUP
            - SHORT_BUILDUP
            - SHORT_COVERING
            - LONG_UNWINDING
            - NEUTRAL
    """

    if price_change_pct is None or oi_change_pct is None:
        return "NEUTRAL"

    # Ignore very small changes (noise filter)
    if abs(price_change_pct) < min_threshold and abs(oi_change_pct) < min_threshold:
        return "NEUTRAL"

    # LONG BUILDUP → Price ↑ + OI ↑
    if price_change_pct > 0 and oi_change_pct > 0:
        return "LONG_BUILDUP"

    # SHORT BUILDUP → Price ↓ + OI ↑
    if price_change_pct < 0 and oi_change_pct > 0:
        return "SHORT_BUILDUP"

    # SHORT COVERING → Price ↑ + OI ↓
    if price_change_pct > 0 and oi_change_pct < 0:
        return "SHORT_COVERING"

    # LONG UNWINDING → Price ↓ + OI ↓
    if price_change_pct < 0 and oi_change_pct < 0:
        return "LONG_UNWINDING"

    return "NEUTRAL"


def identify_oi_trend(df):
    """
    Identify OI trend using last 3 completed candles.

    Required column:
        'oi'

    Returns:
        str:
            - OI_RISING
            - OI_FALLING
            - OI_FLAT
            - None (if insufficient data)
    """

    if df is None or len(df) < 3:
        return None

    if "oi" not in df.columns:
        return None

    df = df.sort_index()

    oi_3 = df.iloc[-3]["oi"]
    oi_2 = df.iloc[-2]["oi"]
    oi_1 = df.iloc[-1]["oi"]

    # Strict rising
    if oi_3 < oi_2 < oi_1:
        return "OI_RISING"

    # Strict falling
    if oi_3 > oi_2 > oi_1:
        return "OI_FALLING"

    return "OI_FLAT"


def calculate_atr(df, period=14):
    """
    Calculate ATR (Average True Range)

    Required columns:
        'high', 'low', 'close'

    Parameters:
        df (pd.DataFrame)
        period (int): ATR period (default 14)

    Returns:
        float: Latest ATR value
        None: If insufficient data
    """

    if df is None or len(df) < period + 1:
        return None

    df = df.sort_index().copy()

    # Previous close
    df["prev_close"] = df["close"].shift(1)

    # True Range components
    df["tr1"] = df["high"] - df["low"]
    df["tr2"] = (df["high"] - df["prev_close"]).abs()
    df["tr3"] = (df["low"] - df["prev_close"]).abs()

    # True Range
    df["true_range"] = df[["tr1", "tr2", "tr3"]].max(axis=1)

    # Wilder's ATR (EMA style smoothing)
    df["atr"] = df["true_range"].ewm(alpha=1/period, adjust=False).mean()

    # Return latest ATR value
    return round(df["atr"].iloc[-1], 4)


def calculate_atr_percentage_from_value(atr_value, latest_close):
    """
    Calculate ATR percentage using precomputed ATR value.

    Parameters:
        atr_value (float): Already calculated ATR
        latest_close (float): Latest closing price

    Returns:
        float: ATR percentage
        None: If invalid input
    """

    if atr_value is None or latest_close is None:
        return None

    if latest_close == 0:
        return None

    atr_percentage = (atr_value / latest_close) * 100

    return round(atr_percentage, 3)



def calculate_atr_series(df, period=14):
    """
    Calculate full ATR series (Wilder's smoothing).

    Required columns:
        'high', 'low', 'close'

    Returns:
        pd.Series of ATR values
        None if insufficient data
    """

    if df is None or len(df) < period + 1:
        return None

    df = df.sort_index().copy()

    # Previous close
    df["prev_close"] = df["close"].shift(1)

    # True Range components
    df["tr1"] = df["high"] - df["low"]
    df["tr2"] = (df["high"] - df["prev_close"]).abs()
    df["tr3"] = (df["low"] - df["prev_close"]).abs()

    # True Range
    df["true_range"] = df[["tr1", "tr2", "tr3"]].max(axis=1)

    # Wilder smoothing
    atr_series = df["true_range"].ewm(alpha=1/period, adjust=False).mean()

    return atr_series 


def is_atr_expanding_2_candles(atr_series):
    """
    Check if ATR is strictly increasing for last 2 candles:
    ATR[-2] < ATR[-1]

    Returns:
        True / False
    """

    if atr_series is None or len(atr_series) < 2:
        return False

    atr_2 = atr_series.iloc[-2]
    atr_1 = atr_series.iloc[-1]

    return atr_2 < atr_1     


def is_atr_expanding_3_candles(atr_series):
    """
    Check if ATR is strictly increasing for last 3 candles:
    ATR[-3] < ATR[-2] < ATR[-1]

    Returns:
        True / False
    """

    if atr_series is None or len(atr_series) < 3:
        return False

    atr_3 = atr_series.iloc[-3]
    atr_2 = atr_series.iloc[-2]
    atr_1 = atr_series.iloc[-1]

    return atr_3 < atr_2 < atr_1      


def calculate_vwap_series(df):
    """
    Calculate intraday VWAP with daily reset.

    Required columns:
        'high', 'low', 'close', 'volume'
    """

    if df is None or df.empty:
        return None

    df = df.copy()
    
    # Handle date extraction: check if index is DatetimeIndex, otherwise use date column
    if isinstance(df.index, pd.DatetimeIndex):
        df["date"] = df.index.date
    elif "date" in df.columns:
        # Convert date column to datetime if not already
        if not pd.api.types.is_datetime64_any_dtype(df["date"]):
            df["date"] = pd.to_datetime(df["date"])
        df["date"] = df["date"].dt.date
    else:
        # Try to set index to date column if it exists
        raise ValueError("DataFrame must have either a DatetimeIndex or a 'date' column")
    
    df = df.sort_index().copy()

    # Typical price
    df["typical_price"] = (df["high"] + df["low"] + df["close"]) / 3

    df["tp_volume"] = df["typical_price"] * df["volume"]

    df["cum_tp_vol"] = df.groupby("date")["tp_volume"].cumsum()
    df["cum_vol"] = df.groupby("date")["volume"].cumsum()

    df["vwap"] = df["cum_tp_vol"] / df["cum_vol"]

    return df["vwap"]

def calculate_vwap(df, vwap_series):
    return vwap_series.iloc[-1] if vwap_series is not None else None


def is_vwap_rising(vwap_series):
    if vwap_series is None or len(vwap_series) < 2:
        return False
    return vwap_series.iloc[-1] > vwap_series.iloc[-2]


def calculate_vwap_distance_pct(df, vwap_series):
    latest_close = df.iloc[-1]["close"]
    latest_vwap = vwap_series.iloc[-1]

    return round(((latest_close - latest_vwap) / latest_vwap) * 100, 2)

def calculate_above_vwap_duration(df, vwap_series):
    df["above_vwap"] = df["close"] > vwap_series
    df["above_vwap_duration"] = df["above_vwap"].astype(int).groupby((df["above_vwap"] != df["above_vwap"].shift()).cumsum()).cumsum()
    return df["above_vwap_duration"].iloc[-1] if len(df) > 0 else None

def extract_vwap_context(df):
    vwap_series = calculate_vwap_series(df)
    return {
        "vwap": calculate_vwap(df, vwap_series),
        "is_vwap_slope_rising": is_vwap_rising(vwap_series),
        "above_vwap_duration_min": calculate_above_vwap_duration(df, vwap_series)
    }
