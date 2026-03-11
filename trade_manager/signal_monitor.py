"""
Signal Monitor module.
Monitors data_cache/final_signals directory for new signal files.
"""

import os
import glob
import orjson
from datetime import datetime
from typing import Optional, Dict, Any, List
from trade_manager.database import TradeDatabase


class SignalMonitor:
    """Monitors final_signals directory for new signals."""

    def __init__(self, signals_dir: str = "data_cache/final_signals", db: TradeDatabase = None):
        """
        Initialize signal monitor.
        
        Args:
            signals_dir: Directory containing final signal JSON files
            db: TradeDatabase instance for tracking processed files
        """
        self.signals_dir = signals_dir
        self.db = db

    def get_new_signals(self) -> List[Dict[str, Any]]:
        """
        Get new signal files that haven't been processed yet.
        
        Returns:
            List of signal dictionaries from unprocessed files
        """
        if not os.path.exists(self.signals_dir):
            return []

        # Get all signal files sorted by timestamp
        pattern = os.path.join(self.signals_dir, "final_signals_*.json")
        files = sorted(glob.glob(pattern))

        new_signals = []
        for file_path in files:
            # Check if already processed
            if self.db and self.db.is_signal_processed(file_path):
                continue

            try:
                # Load signal file
                with open(file_path, 'rb') as f:
                    data = orjson.loads(f.read())

                # Extract signals from the file
                signals = data.get("signals", [])
                file_timestamp = data.get("timestamp", "")

                if signals:
                    # Process all signals from the file
                    for signal in signals:
                        signal_copy = signal.copy()
                        # Add file metadata and timestamp
                        signal_copy["_file_path"] = file_path
                        signal_copy["_file_timestamp"] = file_timestamp
                        signal_copy["timestamp"] = file_timestamp  # Add timestamp for database
                        new_signals.append(signal_copy)

                    # Mark file as processed after processing all signals
                    if self.db:
                        self.db.mark_signal_processed(file_path, file_timestamp)

            except Exception as e:
                print(f"⚠️ Error processing signal file {file_path}: {e}")
                continue

        return new_signals

    def get_all_new_signals(self) -> List[Dict[str, Any]]:
        """
        Get all unprocessed signals from all unprocessed files.
        
        Returns:
            List of all signal dictionaries from unprocessed files
        """
        return self.get_new_signals()
