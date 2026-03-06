import time
import os
import glob
from datetime import datetime
from core.config_loader import Config
from data.zerodha_client import ZerodhaClient
from exception.exception_handler import handle_exceptions
from options.entry_monitor import EntryMonitor
from utils.signal_serializer import load_signals_from_file
from utils.file_operations import save_confirmed_entries, remove_confirmed_from_signals


@handle_exceptions
def monitor_signals():
    """
    Main method for entry monitor that runs in a separate thread.
    Monitors signals from JSON file and checks for entry conditions.
    """
    config = Config()
    signals_dir = "data_cache/signals"
    data_cache_dir = "data_cache"
    
    print("🔍 Entry Monitor started. Monitoring all signals files...")
    
    # Initialize Zerodha client and EntryMonitor
    zerodha_client = ZerodhaClient(config)
    entry_monitor = EntryMonitor(zerodha_client, config)
    
    def get_all_signals_files():
        """Find all signals files with timestamp."""
        pattern = os.path.join(signals_dir, "signals_*.json")
        files = glob.glob(pattern)
        return sorted(files) if files else []
    
    while True:
        try:
            # Get all signal files in the directory
            signals_files = get_all_signals_files()
            
            if signals_files:
                # Load and combine signals from all files
                all_signals = []
                for signals_filepath in signals_files:
                    signals = load_signals_from_file(signals_filepath)
                    if signals:
                        all_signals.extend(signals)
                
                if all_signals:
                    print(f"\n{'='*60}")
                    print(f"📊 Monitoring {len(all_signals)} signals from {len(signals_files)} file(s) at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                    print(f"{'='*60}")
                    
                    # Convert signals to format expected by entry_monitor
                    trade_candidates = []
                    for signal in all_signals:
                        # Extract tradingsymbol from options object
                        options = signal.get("options")
                        if options and isinstance(options, dict) and "tradingsymbol" in options:
                            trade_candidates.append({
                                "symbol": signal.get("symbol"),
                                "signal": signal.get("signal"),
                                "instrument_token": signal.get("instrument_token"),
                                "option_symbol": options["tradingsymbol"],
                                "option_instrument_token": options["instrument_token"]
                            })
                        else:
                            print(f"⚠️ Skipping signal for {signal.get('symbol')} - missing options or tradingsymbol")
                    
                    if trade_candidates:
                        # Monitor for entry conditions
                        confirmed_entries = entry_monitor.monitor(trade_candidates)
                        
                        if confirmed_entries:
                            print(f"\n✅ {len(confirmed_entries)} confirmed entry signals:")
                            for entry in confirmed_entries:
                                print(f"  - {entry['symbol']} | {entry['direction']} | Option: {entry['option_symbol']} | Entry: {entry['entry_price']}")
                            
                            # Save confirmed entries to JSON file with timestamp in subdirectory
                            confirmed_dir = os.path.join(data_cache_dir, "confirmed_signals")
                            save_confirmed_entries(confirmed_entries, confirmed_dir)
                            
                            # Remove confirmed entries from signals JSON files
                            remove_confirmed_from_signals(confirmed_entries, signals_files, signals_dir)
                        else:
                            print("⏳ No entry confirmations yet. Continuing to monitor...")
                    else:
                        print("⚠️ No valid trade candidates found in signals")
                else:
                    print("📭 No signals found in any file")
            else:
                print(f"⏳ Waiting for signals files in {signals_dir}...")
            
            # Sleep for 5 seconds before next check
            time.sleep(300)
            
        except Exception as e:
            print(f"❌ Error in entry monitor: {e}")
            time.sleep(300)


if __name__ == "__main__":
    monitor_signals()
