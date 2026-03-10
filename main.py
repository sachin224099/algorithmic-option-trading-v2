import pandas as pd
import time
import json
import os
from datetime import datetime, timedelta
from core.config_loader import Config
from data.zerodha_client import ZerodhaClient
from exception.exception_handler import handle_exceptions
from signals.fifteen_min_signal import get_fifteen_min_signals
from options.strike_selector import filter_strikes_near_spot
from utils.file_operations import save_signals_with_timestamp
from target_stoploss.target_stop_loss_calculator import calculate_target_stop_loss_futures
from target_stoploss.target_stop_loss_calculator import calculate_target_stop_loss_options
from data.options_data import populate_options_ltp
from utils.signal_serializer import serialize_signal
from scoring.signal_scorer import SignalScorer
from options.options_metrics import OptionsMetrics
import orjson


def filter_and_save_final_signals(signals, timestamp=None, final_signals_dir="data_cache/final_signals"):
    """
    Filter signals based on specific conditions and save to a timestamped JSON file.
    
    Conditions:
    - score >= 75
    - oi_trend = "OI_RISING"
    - oi_structure: PE = "SHORT_BUILDUP", CE = "LONG_BUILDUP"
    - atr_expanding_3_candles = True
    
    Parameters:
        signals: List of signal dictionaries
        timestamp: datetime object for timestamp (defaults to current time)
        final_signals_dir: Directory to save final signals file (default: "data_cache/final_signals")
    
    Returns:
        str: Path to the saved file, or None if no signals matched
    """
    if not signals:
        return None
    
    if timestamp is None:
        timestamp = datetime.now()
    
    # Filter signals based on conditions
    filtered_signals = []
    for signal in signals:
        try:
            # Condition 1: score >= 75
            score = signal.get("score", 0)
            if score < 60:
                continue
            
            # Condition 2: oi_trend = "OI_RISING"
            oi_trend = signal.get("oi_trend")
            if oi_trend != "OI_RISING":
                continue
            
            # Condition 3: oi_structure based on signal type
            signal_type = signal.get("signal")
            oi_structure = signal.get("oi_structure")
            
            if signal_type == "PE" and oi_structure != "SHORT_BUILDUP":
                continue
            if signal_type == "CE" and oi_structure != "LONG_BUILDUP":
                continue
            
            # Condition 4: atr_expanding_3_candles = True
            atr_expanding = signal.get("atr_expanding_3_candles", False)
            if not atr_expanding:
                continue
            
            # All conditions met, add to filtered list
            filtered_signals.append(signal)
        except Exception as e:
            symbol = signal.get("symbol", "UNKNOWN")
            print(f"⚠️ Error filtering signal {symbol}: {e}")
            continue
    
    # Save filtered signals if any match
    if not filtered_signals:
        print("ℹ️ No signals matched the final signal criteria")
        return None
    
    timestamp_str = timestamp.strftime("%Y%m%d_%H%M%S")
    final_signals_filepath = os.path.join(final_signals_dir, f"final_signals_{timestamp_str}.json")
    os.makedirs(final_signals_dir, exist_ok=True)
    
    # Prepare data structure similar to regular signals file
    data = {
        "timestamp": timestamp.isoformat(),
        "count": len(filtered_signals),
        "signals": filtered_signals
    }
    
    # Save to JSON file
    with open(final_signals_filepath, 'wb') as f:
        f.write(orjson.dumps(data, option=orjson.OPT_INDENT_2 | orjson.OPT_NON_STR_KEYS))
    
    print(f"✅ Filtered and saved {len(filtered_signals)} final signals to {final_signals_filepath}")
    return final_signals_filepath


@handle_exceptions
def main():
    """Main scheduler that runs scan every 15 minutes."""
    config = Config()
    timeframe_minutes = config.get_timeframe_minutes()
    start_time = datetime.now()
    print(f"Starting scheduled scans. First run at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    
    iteration = 0
    
    while True:
        iteration += 1
        run_start = datetime.now()
        print(f"\n{'='*60}")
        print(f"Iteration {iteration} - Starting at: {run_start.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'='*60}")
        
        try:
            zerodha_client = ZerodhaClient(config)
            futures_master = zerodha_client.futures_master
            signals = get_fifteen_min_signals(futures_master)
            if len(signals) == 0:
                print("No signals found")
            nfo_instruments = zerodha_client.load_nfo_instruments()
            symbols = [signal["symbol"] for signal in signals]
            stock_spot_prices = zerodha_client.get_stock_spot_prices(symbols)
            #print(stock_spot_prices)
            
            # Create a copy of signals for JSON serialization without affecting original structure
            signals_for_json = []
            for signal in signals:
                try:
                    symbol = signal["symbol"]
                    # Check if symbol exists in stock_spot_prices
                    if symbol not in stock_spot_prices:
                        print(f"⚠️ Skipping {symbol}: spot price not available")
                        continue
                    
                    options = filter_strikes_near_spot(nfo_instruments, symbol, signal["signal"], stock_spot_prices[symbol], signal["current_atr"])
                    options = OptionsMetrics(zerodha_client).populate_options_metrics(options)
                    if options is None:
                        print(f"⚠️ Skipping {symbol}: no suitable option strike found")
                        continue
                    
                    options = populate_options_ltp(options, zerodha_client)
                    if options is None:
                        print(f"⚠️ Skipping {symbol}: failed to populate option LTP")
                        continue
                    
                    # Create a copy of signal for JSON without modifying original
                    # Convert pandas Series to dict first to avoid string conversion of dict values
                    signal_copy = signal.to_dict() if isinstance(signal, pd.Series) else signal.copy()
                    futures_target_stop_loss = calculate_target_stop_loss_futures(signal["close"], signal["current_atr"], signal["signal"], config)
                    signal_copy["futures_target_sl_context"] = futures_target_stop_loss
                    
                    options_target_stop_loss = calculate_target_stop_loss_options(futures_target_stop_loss, options["last_price"], signal["close"], config)

                    options["options_target_sl_context"] = options_target_stop_loss

                    signal_copy["options"] = options

                    signals_for_json.append(signal_copy)
                except Exception as e:
                    symbol = signal.get("symbol", "UNKNOWN")
                    print(f"⚠️ Error processing {symbol}: {e}")
                    continue

            # Save signals to JSON file (using copies, original signals remain unchanged)
            if signals_for_json:
                scorer = SignalScorer(config)
                signals_for_json = scorer.rank_signals(signals_for_json)
                signals_filepath = save_signals_with_timestamp(signals_for_json, run_start)
                print(f"✅ Processed {len(signals_for_json)} signals and saved to {signals_filepath}")
                
                # Filter and save final signals based on criteria
                filter_and_save_final_signals(signals_for_json, run_start)
            else:
                print("⚠️ No signals to save after processing")    
        except Exception as e:
            print(f"Error during scan: {e}")
        
        run_end = datetime.now()
        execution_time = (run_end - run_start).total_seconds()
        print(f"Scan completed in {execution_time:.2f} seconds")
        
        # Calculate next run time: start_time + (iteration * 15 minutes)
        next_run_time = start_time + timedelta(minutes=iteration * timeframe_minutes)
        
        # If execution took longer than expected and we've passed the next run time,
        # move to the following interval
        while next_run_time <= run_end:
            iteration += 1
            next_run_time = start_time + timedelta(minutes=iteration * timeframe_minutes)
        
        sleep_seconds = (next_run_time - run_end).total_seconds()
        
        if sleep_seconds > 0:
            print(f"Next run scheduled at: {next_run_time.strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"Sleeping for {sleep_seconds:.2f} seconds ({sleep_seconds/60:.2f} minutes)")
            time.sleep(sleep_seconds)
        else:
            # Should not happen due to while loop above, but just in case
            print("Proceeding to next run immediately.")



if __name__ == "__main__":
    main()
