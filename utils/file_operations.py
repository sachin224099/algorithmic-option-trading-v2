import os
import json
from datetime import datetime, date
from utils.signal_serializer import save_signals_to_file


class CustomJSONEncoder(json.JSONEncoder):
    """Custom JSON encoder that handles datetime.date and datetime.datetime."""
    def default(self, obj):
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        return super().default(obj)


def _make_json_serializable(obj):
    """
    Recursively convert objects to JSON-serializable types.
    Handles datetime.date, datetime.datetime, and preserves dict/list structures.
    """
    if obj is None:
        return None
    elif isinstance(obj, (str, int, float, bool)):
        # If it's a string that looks like a dict representation, try to parse it
        if isinstance(obj, str) and obj.strip().startswith('{') and obj.strip().endswith('}'):
            try:
                import ast
                import re
                # Check if it contains datetime.date
                if 'datetime.date' in obj:
                    def replace_date(match):
                        args = match.group(1)
                        nums = re.findall(r'\d+', args)
                        if len(nums) >= 3:
                            return f"'{nums[0]}-{nums[1].zfill(2)}-{nums[2].zfill(2)}'"
                        return match.group(0)
                    obj = re.sub(r"datetime\.date\(([^)]+)\)", replace_date, obj)
                parsed = ast.literal_eval(obj)
                # Recursively process the parsed object
                return _make_json_serializable(parsed)
            except (ValueError, SyntaxError):
                # If parsing fails, return as string
                return obj
        return obj
    elif isinstance(obj, datetime):
        return obj.isoformat()
    elif isinstance(obj, date):
        return obj.isoformat()
    elif isinstance(obj, dict):
        # Preserve dict structure - recursively process values
        result = {}
        for key, value in obj.items():
            # Never convert dicts to strings - always preserve structure
            serialized_value = _make_json_serializable(value)
            result[key] = serialized_value
        return result
    elif isinstance(obj, (list, tuple)):
        return [_make_json_serializable(item) for item in obj]
    else:
        # Fallback: convert to string for unknown types
        # But log a warning for debugging
        print(f"⚠️ Converting unknown type {type(obj)} to string: {str(obj)[:100]}")
        return str(obj)


def save_signals_with_timestamp(signals, timestamp=None, signals_dir="data_cache/signals"):
    """
    Save signals to a JSON file with timestamp in the filename.
    
    Parameters:
        signals: List of signals to save (can be dicts or pandas Series)
        timestamp: datetime object for timestamp (defaults to current time)
        signals_dir: Directory to save signals file (default: "data_cache/signals")
    
    Returns:
        str: Path to the saved file
    """
    print(f"🔍 save_signals_with_timestamp called with {len(signals) if signals else 0} signals")
    if timestamp is None:
        timestamp = datetime.now()
    
    timestamp_str = timestamp.strftime("%Y%m%d_%H%M%S")
    signals_filepath = os.path.join(signals_dir, f"signals_{timestamp_str}.json")
    os.makedirs(signals_dir, exist_ok=True)
    
    # Check if signals are already dictionaries (from main.py)
    # If so, serialize properly to handle datetime.date and other types
    if signals and isinstance(signals[0], dict):
        print(f"🔍 Signals are dicts, first signal keys: {list(signals[0].keys())[:5]}")
        # Recursively make all data JSON-serializable while preserving structure
        serialized_signals = []
        import ast
        import re
        
        for signal in signals:
            # Debug: Check what we're receiving
            symbol = signal.get('symbol', 'UNKNOWN')
            futures_ctx = signal.get('futures_target_sl_context')
            options_ctx = signal.get('options')
            if isinstance(futures_ctx, str):
                print(f"🔍 DEBUG: futures_target_sl_context is STRING for {symbol} (length: {len(futures_ctx)})")
            elif isinstance(futures_ctx, dict):
                print(f"✅ DEBUG: futures_target_sl_context is DICT for {symbol}")
            else:
                print(f"⚠️ DEBUG: futures_target_sl_context is {type(futures_ctx)} for {symbol}")
            
            if isinstance(options_ctx, str):
                print(f"🔍 DEBUG: options is STRING for {symbol} (length: {len(options_ctx)})")
            elif isinstance(options_ctx, dict):
                print(f"✅ DEBUG: options is DICT for {symbol}")
            else:
                print(f"⚠️ DEBUG: options is {type(options_ctx)} for {symbol}")
            # Deep copy to avoid modifying original
            signal_copy = {}
            for key, value in signal.items():
                # Handle futures_target_sl_context - ensure it's a dict
                if key == 'futures_target_sl_context':
                    if isinstance(value, str):
                        try:
                            signal_copy[key] = ast.literal_eval(value)
                            print(f"🔧 Fixed futures_target_sl_context (was string) for {signal.get('symbol', 'UNKNOWN')}")
                        except (ValueError, SyntaxError) as e:
                            print(f"⚠️ Warning: Could not parse futures_target_sl_context for {signal.get('symbol', 'UNKNOWN')}: {e}")
                            signal_copy[key] = value
                    elif isinstance(value, dict):
                        signal_copy[key] = value
                    else:
                        print(f"⚠️ Warning: futures_target_sl_context is unexpected type {type(value)} for {signal.get('symbol', 'UNKNOWN')}")
                        signal_copy[key] = value
                
                # Handle options - ensure it's a dict and fix datetime.date
                elif key == 'options':
                    if isinstance(value, str):
                        try:
                            options_str = value
                            # Replace datetime.date(...) with ISO format string
                            def replace_date(match):
                                args = match.group(1)
                                nums = re.findall(r'\d+', args)
                                if len(nums) >= 3:
                                    return f"'{nums[0]}-{nums[1].zfill(2)}-{nums[2].zfill(2)}'"
                                return match.group(0)
                            options_str = re.sub(r"datetime\.date\(([^)]+)\)", replace_date, options_str)
                            signal_copy[key] = ast.literal_eval(options_str)
                            print(f"🔧 Fixed options (was string) for {signal.get('symbol', 'UNKNOWN')}")
                        except (ValueError, SyntaxError) as e:
                            print(f"⚠️ Warning: Could not parse options for {signal.get('symbol', 'UNKNOWN')}: {e}")
                            signal_copy[key] = value
                    elif isinstance(value, dict):
                        signal_copy[key] = value
                    else:
                        print(f"⚠️ Warning: options is unexpected type {type(value)} for {signal.get('symbol', 'UNKNOWN')}")
                        signal_copy[key] = value
                
                # Copy other fields as-is
                else:
                    signal_copy[key] = value
            
            # Now serialize properly - this will handle datetime.date objects
            serialized_signal = _make_json_serializable(signal_copy)
            # Double-check that serialization didn't convert dicts to strings
            if isinstance(serialized_signal.get('futures_target_sl_context'), str):
                print(f"⚠️ ERROR: futures_target_sl_context became string after serialization for {signal.get('symbol', 'UNKNOWN')}")
            if isinstance(serialized_signal.get('options'), str):
                print(f"⚠️ ERROR: options became string after serialization for {signal.get('symbol', 'UNKNOWN')}")
            serialized_signals.append(serialized_signal)
        
        data = {
            "timestamp": timestamp.isoformat(),
            "count": len(serialized_signals),
            "signals": serialized_signals
        }
        # Final check before saving
        if serialized_signals:
            first_signal = serialized_signals[0]
            futures_type = type(first_signal.get('futures_target_sl_context'))
            options_type = type(first_signal.get('options'))
            print(f"🔍 Before json.dump - futures_target_sl_context type: {futures_type}, options type: {options_type}")
            if isinstance(first_signal.get('futures_target_sl_context'), str):
                print(f"⚠️ CRITICAL: futures_target_sl_context is STRING before json.dump!")
            if isinstance(first_signal.get('options'), str):
                print(f"⚠️ CRITICAL: options is STRING before json.dump!")
        
        with open(signals_filepath, 'w') as f:
            json.dump(data, f, indent=2, ensure_ascii=False, cls=CustomJSONEncoder)
        print(f"✅ Saved {len(serialized_signals)} signals to {signals_filepath}")
    else:
        # Signals are pandas Series, use serializer
        save_signals_to_file(signals, signals_filepath)
    
    return signals_filepath


def save_confirmed_entries(confirmed_entries, entries_dir="data_cache/confirmed_signals"):
    """Save confirmed entries to a JSON file with timestamp."""
    if not confirmed_entries:
        return
    
    timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    entries_filepath = os.path.join(entries_dir, f"confirmed_entries_{timestamp_str}.json")
    os.makedirs(entries_dir, exist_ok=True)
    
    # Convert datetime objects to ISO format strings for JSON serialization
    serialized_entries = []
    for entry in confirmed_entries:
        entry_copy = entry.copy()
        if "timestamp" in entry_copy and isinstance(entry_copy["timestamp"], datetime):
            entry_copy["timestamp"] = entry_copy["timestamp"].isoformat()
        serialized_entries.append(entry_copy)
    
    # Add metadata
    data = {
        "timestamp": datetime.now().isoformat(),
        "count": len(serialized_entries),
        "entries": serialized_entries
    }
    
    with open(entries_filepath, 'w') as f:
        json.dump(data, f, indent=2, default=str)
    
    print(f"💾 Saved {len(serialized_entries)} confirmed entries to {entries_filepath}")


def remove_confirmed_from_signals(confirmed_entries, signals_files, signals_dir):
    """Remove confirmed entries from signals JSON files."""
    if not confirmed_entries or not signals_files:
        return
    
    # Create a set of (symbol, option_symbol) tuples for quick lookup
    confirmed_set = set()
    for entry in confirmed_entries:
        confirmed_set.add((entry['symbol'], entry['option_symbol']))
    
    removed_count = 0
    
    # Process each signals file
    for signals_filepath in signals_files:
        try:
            # Load the signals file
            with open(signals_filepath, 'r') as f:
                data = json.load(f)
            
            signals = data.get("signals", [])
            if not signals:
                continue
            
            # Filter out confirmed entries
            original_count = len(signals)
            remaining_signals = []
            
            for signal in signals:
                symbol = signal.get("symbol")
                options = signal.get("options")
                
                # Check if this signal matches any confirmed entry
                is_confirmed = False
                if symbol and options and isinstance(options, dict):
                    option_symbol = options.get("tradingsymbol")
                    if option_symbol and (symbol, option_symbol) in confirmed_set:
                        is_confirmed = True
                        removed_count += 1
                
                if not is_confirmed:
                    remaining_signals.append(signal)
            
            # Only update file if signals were removed
            if len(remaining_signals) < original_count:
                # Update metadata
                data["signals"] = remaining_signals
                data["count"] = len(remaining_signals)
                data["timestamp"] = datetime.now().isoformat()
                
                # Save updated signals back to file
                with open(signals_filepath, 'w') as f:
                    json.dump(data, f, indent=2, default=str)
                
                print(f"🗑️  Removed {original_count - len(remaining_signals)} confirmed signal(s) from {os.path.basename(signals_filepath)}")
        
        except Exception as e:
            print(f"⚠️  Error removing confirmed entries from {signals_filepath}: {e}")
    
    if removed_count > 0:
        print(f"✅ Removed {removed_count} confirmed entry signal(s) from signals files")
