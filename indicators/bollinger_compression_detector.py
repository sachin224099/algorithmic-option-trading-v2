import pandas as pd
import numpy as np


class BollingerCompressionDetector:
    """
    Detect Bollinger Band compression (volatility squeeze).

    Parameters
    ----------
    period : int
        Bollinger moving average period.
    std_multiplier : float
        Standard deviation multiplier for bands.
    width_lookback : int
        Lookback window to evaluate compression percentile.
    compression_percentile : float
        Percentile threshold for squeeze detection.
    """

    def __init__(
        self,
        period: int = 20,
        std_multiplier: float = 2.0,
        width_lookback: int = 120,
        compression_percentile: float = 0.15,
    ):
        self.period = period
        self.std_multiplier = std_multiplier
        self.width_lookback = width_lookback
        self.compression_percentile = compression_percentile


    def calculate_bollinger_bands(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Compute Bollinger Bands and BB width.
        """

        if "close" not in df.columns:
            raise ValueError("DataFrame must contain 'close' column")

        df = df.copy()

        # Moving average
        df["bb_mid"] = df["close"].rolling(self.period).mean()

        # Standard deviation
        rolling_std = df["close"].rolling(self.period).std(ddof=0)

        df["bb_upper"] = df["bb_mid"] + self.std_multiplier * rolling_std
        df["bb_lower"] = df["bb_mid"] - self.std_multiplier * rolling_std

        # Normalized width (scale invariant)
        df["bb_width"] = (df["bb_upper"] - df["bb_lower"]) / df["bb_mid"]

        return df


    def detect_compression(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Detect Bollinger Band compression based on width percentile.
        """

        df = self.calculate_bollinger_bands(df)

        # Rolling percentile threshold
        rolling_width = df["bb_width"].rolling(self.width_lookback)

        df["bb_width_threshold"] = rolling_width.quantile(self.compression_percentile)

        # Compression flag
        df["bb_compression"] = df["bb_width"] < df["bb_width_threshold"]

        return df


    def latest_signal(self, df: pd.DataFrame) -> bool:
        """
        Return compression state of the latest candle.
        """

        df = self.detect_compression(df)

        if len(df) == 0:
            return False

        return bool(df["bb_compression"].iloc[-1])