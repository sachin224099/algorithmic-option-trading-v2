import orjson
import pandas as pd
from datetime import datetime
from typing import List, Dict, Any


def serialize_signal(signal: pd.Series) -> Dict[str, Any]:
    """
    Convert a pandas Series signal to a JSON-serializable dictionary.
    
    Parameters:
        signal: pandas Series containing signal data
        
    Returns:
        Dictionary with all signal fields converted to JSON-compatible types
    """
    signal_dict = {}
    
    for key, value in signal.items():
        # Handle None values first
        if value is None:
            signal_dict[key] = None
        # Check for Series/DataFrame before pd.isna to avoid ambiguous truth value error
        elif isinstance(value, pd.Series):
            # Special handling for options Series - extract tradingsymbol as option_symbol
            if key == "options":
                try:
                    if not value.empty:
                        options_dict = value.to_dict()
                        signal_dict[key] = options_dict
                    else:
                        signal_dict[key] = None
                except (AttributeError, ValueError):
                    signal_dict[key] = None
            else:
                signal_dict[key] = value.to_dict() if hasattr(value, 'to_dict') else str(value)
        elif isinstance(value, pd.DataFrame):
            signal_dict[key] = value.to_dict('records') if hasattr(value, 'to_dict') else str(value)
        elif isinstance(value, (pd.Timestamp, datetime)):
            signal_dict[key] = value.isoformat()
        elif pd.isna(value):
            signal_dict[key] = None
        elif isinstance(value, (int, float, str, bool)):
            signal_dict[key] = value
        else:
            # Convert other types to string as fallback
            signal_dict[key] = str(value)
    
    return signal_dict


def serialize_signals(signals: List[pd.Series]) -> List[Dict[str, Any]]:
    """
    Convert a list of pandas Series signals to JSON-serializable list.
    
    Parameters:
        signals: List of pandas Series containing signal data
        
    Returns:
        List of dictionaries with all signal fields converted to JSON-compatible types
    """
    return [serialize_signal(signal) for signal in signals]


def save_signals_to_file(signals: List[pd.Series], filepath: str) -> None:
    """
    Save signals to a JSON file.
    
    Parameters:
        signals: List of pandas Series containing signal data
        filepath: Path to the JSON file where signals will be saved
    """
    serialized_signals = serialize_signals(signals)
    
    # Add metadata
    data = {
        "timestamp": datetime.now().isoformat(),
        "count": len(serialized_signals),
        "signals": serialized_signals
    }
    
    with open(filepath, 'wb') as f:
        f.write(orjson.dumps(data, option=orjson.OPT_INDENT_2 | orjson.OPT_NON_STR_KEYS))
    
    print(f"✅ Saved {len(serialized_signals)} signals to {filepath}")


def load_signals_from_file(filepath: str) -> List[Dict[str, Any]]:
    """
    Load signals from a JSON file.
    
    Parameters:
        filepath: Path to the JSON file containing signals
        
    Returns:
        List of signal dictionaries
    """
    try:
        with open(filepath, 'rb') as f:
            data = orjson.loads(f.read())
        
        return data.get("signals", [])
    except FileNotFoundError:
        print(f"⚠️ Signal file not found: {filepath}")
        return []
    except (ValueError, TypeError) as e:
        print(f"❌ Error decoding JSON from {filepath}: {e}")
        return []
