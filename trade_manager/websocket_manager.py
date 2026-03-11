"""
Websocket Manager module.
Handles Zerodha websocket connection for real-time price updates.
"""

import threading
import time
from typing import Dict, Callable, Optional, List
from kiteconnect import KiteTicker


class WebsocketManager:
    """Manages Zerodha websocket connection for per-tick data."""

    def __init__(self, api_key: str, access_token: str):
        """
        Initialize websocket manager.
        
        Args:
            api_key: Zerodha API key
            access_token: Zerodha access token
        """
        self.api_key = api_key
        self.access_token = access_token
        self.kws = None
        self.price_cache: Dict[int, float] = {}  # {instrument_token: current_price}
        self.subscribed_tokens: set = set()
        self.is_connected = False
        self.reconnect_attempts = 0
        self.max_reconnect_attempts = 10
        self.reconnect_delay = 5  # seconds

        # Callbacks
        self.on_ticks_callback: Optional[Callable] = None
        self.on_connect_callback: Optional[Callable] = None
        self.on_close_callback: Optional[Callable] = None

    def set_on_ticks(self, callback: Callable):
        """Set callback for tick updates."""
        self.on_ticks_callback = callback

    def set_on_connect(self, callback: Callable):
        """Set callback for connection established."""
        self.on_connect_callback = callback

    def set_on_close(self, callback: Callable):
        """Set callback for connection closed."""
        self.on_close_callback = callback

    def _on_ticks(self, ws, ticks):
        """
        Handle tick updates from websocket.
        
        Args:
            ws: WebSocket instance
            ticks: List of tick data
        """
        for tick in ticks:
            instrument_token = tick.get("instrument_token")
            last_price = tick.get("last_price")
            
            if instrument_token and last_price:
                self.price_cache[instrument_token] = last_price

        # Call user callback if set
        if self.on_ticks_callback:
            try:
                self.on_ticks_callback(ticks)
            except Exception as e:
                print(f"⚠️ Error in on_ticks callback: {e}")

    def _on_connect(self, ws, response):
        """Handle websocket connection."""
        self.is_connected = True
        self.reconnect_attempts = 0
        print("✅ Websocket connected")

        # Resubscribe to all tokens
        if self.subscribed_tokens:
            tokens_list = list(self.subscribed_tokens)
            ws.subscribe(tokens_list)
            ws.set_mode(ws.MODE_FULL, tokens_list)
            print(f"✅ Resubscribed to {len(tokens_list)} instruments")

        if self.on_connect_callback:
            try:
                self.on_connect_callback(ws, response)
            except Exception as e:
                print(f"⚠️ Error in on_connect callback: {e}")

    def _on_close(self, ws, code, reason):
        """Handle websocket close."""
        self.is_connected = False
        print(f"⚠️ Websocket closed: {code} - {reason}")

        if self.on_close_callback:
            try:
                self.on_close_callback(ws, code, reason)
            except Exception as e:
                print(f"⚠️ Error in on_close callback: {e}")

        # Attempt reconnection
        if self.reconnect_attempts < self.max_reconnect_attempts:
            self.reconnect_attempts += 1
            delay = self.reconnect_delay * (2 ** (self.reconnect_attempts - 1))  # Exponential backoff
            print(f"🔄 Attempting reconnection in {delay} seconds (attempt {self.reconnect_attempts}/{self.max_reconnect_attempts})")
            time.sleep(delay)
            self.connect()

    def _on_error(self, ws, code, reason):
        """Handle websocket error."""
        print(f"❌ Websocket error: {code} - {reason}")

    def connect(self):
        """Connect to websocket."""
        try:
            self.kws = KiteTicker(self.api_key, self.access_token)
            
            # Set callbacks
            self.kws.on_ticks = self._on_ticks
            self.kws.on_connect = self._on_connect
            self.kws.on_close = self._on_close
            self.kws.on_error = self._on_error

            # Connect
            self.kws.connect(threaded=True)
            print("🔄 Connecting to Zerodha websocket...")

        except Exception as e:
            print(f"❌ Failed to connect websocket: {e}")
            self.is_connected = False

    def subscribe(self, instrument_tokens: List[int]):
        """
        Subscribe to instrument tokens for per-tick data.
        
        Args:
            instrument_tokens: List of instrument tokens to subscribe
        """
        if not self.is_connected or not self.kws:
            print("⚠️ Websocket not connected. Tokens will be subscribed on connect.")
            self.subscribed_tokens.update(instrument_tokens)
            return

        # Convert to list if single token
        if isinstance(instrument_tokens, int):
            instrument_tokens = [instrument_tokens]

        # Add to subscribed set
        self.subscribed_tokens.update(instrument_tokens)

        # Subscribe via websocket
        try:
            self.kws.subscribe(instrument_tokens)
            self.kws.set_mode(self.kws.MODE_FULL, instrument_tokens)
            print(f"✅ Subscribed to {len(instrument_tokens)} instruments")
        except Exception as e:
            print(f"⚠️ Error subscribing to instruments: {e}")

    def unsubscribe(self, instrument_tokens: List[int]):
        """
        Unsubscribe from instrument tokens.
        
        Args:
            instrument_tokens: List of instrument tokens to unsubscribe
        """
        if not self.is_connected or not self.kws:
            return

        if isinstance(instrument_tokens, int):
            instrument_tokens = [instrument_tokens]

        self.subscribed_tokens.difference_update(instrument_tokens)

        try:
            self.kws.unsubscribe(instrument_tokens)
            print(f"✅ Unsubscribed from {len(instrument_tokens)} instruments")
        except Exception as e:
            print(f"⚠️ Error unsubscribing from instruments: {e}")

    def get_price(self, instrument_token: int) -> Optional[float]:
        """
        Get current price for an instrument token.
        
        Args:
            instrument_token: Instrument token
            
        Returns:
            Current price or None if not available
        """
        return self.price_cache.get(instrument_token)

    def disconnect(self):
        """Disconnect websocket."""
        if self.kws:
            try:
                self.kws.close()
                self.is_connected = False
                print("✅ Websocket disconnected")
            except Exception as e:
                print(f"⚠️ Error disconnecting websocket: {e}")
