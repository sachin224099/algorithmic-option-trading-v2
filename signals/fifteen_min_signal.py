from indicators.breakout import breakout_signal
import pandas as pd
from datetime import datetime, timedelta
from data.historic_data import get_minutes_data
from data.zerodha_client import ZerodhaClient
from core.config_loader import Config
from indicators.indicator_metrics_calculator import calculate_price_change_pct, calculate_oi_change_pct, calculate_volume_spike_ratio, classify_oi_structure, identify_oi_trend, calculate_atr, calculate_atr_percentage_from_value, calculate_atr_series, is_atr_expanding_3_candles, is_atr_expanding_2_candles, extract_vwap_context    

def get_fifteen_min_signals(futures_master: pd.DataFrame):
    kite = ZerodhaClient(Config()).get_kite()
    signals = []
    for index, row in futures_master.iterrows():
        instrument_token = row["instrument_token"]
        to_date = datetime.now()
        from_date = to_date - timedelta(days=15)
        df = get_minutes_data(kite, instrument_token, from_date, to_date, 15, oi=True)
        df_closed = df.iloc[:-1].copy()
        signal = breakout_signal(df_closed)
        if signal is not None:
            #print(f"Signal: {signal} for {row['futures_symbol']}")
            row["signal"] = signal
            row["close"] = df_closed.iloc[-1]["close"]
            row["volume_spike_ratio"] = calculate_volume_spike_ratio(df_closed)
            row["price_change_pct"] = calculate_price_change_pct(df_closed)
            row["oi_change_pct"] = calculate_oi_change_pct(df_closed)
            row["oi_structure"] = classify_oi_structure(row["price_change_pct"], row["oi_change_pct"], 0.5)
            row["oi_trend"] = identify_oi_trend(df_closed)
            row["current_atr"] = calculate_atr(df_closed)
            atr_series = calculate_atr_series(df_closed)
            row["atr_percentage"] = calculate_atr_percentage_from_value(row["current_atr"], row["close"])
            row["candle_range"] = df_closed.iloc[-1]["high"] - df_closed.iloc[-1]["low"]
            row["atr_expanding_3_candles"] = is_atr_expanding_3_candles(atr_series)
            row["atr_expanding_2_candles"] = is_atr_expanding_2_candles(atr_series)
            vwap_context = extract_vwap_context(df_closed)
            row["vwap"] = vwap_context["vwap"]
            row["is_vwap_slope_rising"] = vwap_context["is_vwap_slope_rising"]
            row["above_vwap_duration_min"] = vwap_context["above_vwap_duration_min"]

            signals.append(row)

    #print(signals)           
    return signals  

                        

                        