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
from options.entry_monitor import EntryMonitor

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
            entry_monitor = EntryMonitor(zerodha_client, config)
            #print(stock_spot_prices)
            for signal in signals:
                print(signal)
                options = filter_strikes_near_spot(nfo_instruments, signal["symbol"], signal["signal"], stock_spot_prices[signal["symbol"]], signal["current_atr"])
                #print(options)
            

            print(signals)    
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
