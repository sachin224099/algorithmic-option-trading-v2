"""
Main entry point for trade_manager module.
Run this script independently to start the trade manager.
"""

import sys
import os

# Add project root to Python path for independent execution
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from core.config_loader import Config
from data.zerodha_client import ZerodhaClient
from trade_manager.trade_manager import TradeManager
from exception.exception_handler import handle_exceptions


@handle_exceptions
def main():
    """Main entry point."""
    print("=" * 60)
    print("Trade Manager - Starting...")
    print("=" * 60)
    
    # Initialize config and Zerodha client
    config = Config()
    zerodha_client = ZerodhaClient(config)
    
    # Create and run trade manager
    trade_manager = TradeManager(config, zerodha_client)
    trade_manager.run()


if __name__ == "__main__":
    main()
