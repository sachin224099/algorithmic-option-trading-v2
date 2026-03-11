"""
Main Trade Manager orchestrator.
Coordinates signal monitoring, order management, and position tracking.
"""

import time
import threading
from datetime import datetime, timedelta
from typing import Dict, Any, Optional
from core.config_loader import Config
from data.zerodha_client import ZerodhaClient
from trade_manager.database import TradeDatabase
from trade_manager.signal_monitor import SignalMonitor
from trade_manager.websocket_manager import WebsocketManager


class TradeManager:
    """Main trade manager orchestrator."""

    def __init__(self, config: Config, zerodha_client: ZerodhaClient):
        """
        Initialize trade manager.
        
        Args:
            config: Config instance
            zerodha_client: ZerodhaClient instance
        """
        self.config = config
        self.zerodha_client = zerodha_client
        self.db = TradeDatabase()
        self.signal_monitor = SignalMonitor(db=self.db)
        
        # Get API credentials for websocket
        api_key, access_token = config.get_api_credentials()
        self.websocket = WebsocketManager(api_key, access_token)
        
        # Set websocket callbacks
        self.websocket.set_on_ticks(self._on_websocket_ticks)
        self.websocket.set_on_connect(self._on_websocket_connect)
        self.websocket.set_on_close(self._on_websocket_close)
        
        # Signal check interval (15 minutes)
        self.signal_check_interval = 15 * 60  # seconds
        self.last_signal_check = datetime.now()
        
        # Threading
        self.running = False
        self.signal_check_thread: Optional[threading.Thread] = None

    def _on_websocket_ticks(self, ticks):
        """Handle websocket tick updates."""
        # Check entry conditions for open orders
        self._check_entry_conditions()
        
        # Check exit conditions for open positions
        self._check_exit_conditions()

    def _on_websocket_connect(self, ws, response):
        """Handle websocket connection."""
        # Subscribe to all open orders and positions
        self._subscribe_to_active_instruments()

    def _on_websocket_close(self, ws, code, reason):
        """Handle websocket close."""
        pass  # Reconnection handled by websocket_manager

    def _subscribe_to_active_instruments(self):
        """Subscribe websocket to all active instruments (open orders + positions)."""
        tokens = set()
        
        # Get tokens from open orders
        open_orders = self.db.get_open_orders()
        for order in open_orders:
            token = order.get("instrument_token")
            if token:
                tokens.add(token)
        
        # Get tokens from open positions
        open_positions = self.db.get_open_positions()
        for position in open_positions:
            token = position.get("instrument_token")
            if token:
                tokens.add(token)
        
        if tokens:
            self.websocket.subscribe(list(tokens))

    def _check_entry_conditions(self):
        """Check if entry conditions are met for open orders."""
        open_orders = self.db.get_open_orders()
        
        for order in open_orders:
            instrument_token = order.get("instrument_token")
            signal_type = order.get("signal")  # 'PE' or 'CE'
            close_price = order.get("close")  # Entry trigger price
            order_id = order.get("id")
            
            if not instrument_token or not signal_type or close_price is None:
                continue
            
            # Get current price from websocket
            current_price = self.websocket.get_price(instrument_token)
            if current_price is None:
                continue
            
            # Check entry condition
            entry_triggered = False
            
            if signal_type == "PE":
                # PE: Enter when futures price >= close (close is the trigger price)
                if current_price >= close_price:
                    entry_triggered = True
            elif signal_type == "CE":
                # CE: Enter when futures price <= close (close is the trigger price)
                if current_price <= close_price:
                    entry_triggered = True
            
            if entry_triggered:
                print(f"✅ Entry condition met for order {order_id}: {signal_type} signal, "
                      f"current_price={current_price}, trigger_price={close_price}")
                
                # Calculate price difference and adjust target/stop_loss
                price_diff = current_price - close_price
                original_target = order.get("target")
                original_stop_loss = order.get("stop_loss")
                
                # Adjust target and stop_loss based on entry price difference
                adjusted_target = None
                adjusted_stop_loss = None
                
                if original_target is not None and original_stop_loss is not None:
                    if signal_type == "PE":
                        # PE: Target is below close_price, stop_loss is above close_price
                        # Adjust both by the price difference to maintain relative distances
                        adjusted_target = original_target + price_diff
                        adjusted_stop_loss = original_stop_loss + price_diff
                    elif signal_type == "CE":
                        # CE: Target is above close_price, stop_loss is below close_price
                        # Adjust both by the price difference to maintain relative distances
                        adjusted_target = original_target + price_diff
                        adjusted_stop_loss = original_stop_loss + price_diff
                    
                    print(f"  Adjusted levels: target={adjusted_target:.2f} (was {original_target:.2f}), "
                          f"stop_loss={adjusted_stop_loss:.2f} (was {original_stop_loss:.2f}), "
                          f"price_diff={price_diff:.2f}")
                
                # Update order status to executed
                executed_at = datetime.now().isoformat()
                self.db.update_order_status(order_id, "executed", executed_at)
                
                # Move to open_positions with adjusted target/stop_loss
                position_id = self.db.move_to_positions(order_id, current_price, 
                                                         adjusted_target, adjusted_stop_loss)
                print(f"✅ Order {order_id} moved to position {position_id} with entry_price={current_price}")
                
                # Ensure websocket is subscribed to this instrument for exit monitoring
                self.websocket.subscribe([instrument_token])

    def _check_exit_conditions(self):
        """Check if exit conditions (target/SL) are met for open positions."""
        open_positions = self.db.get_open_positions()
        
        for position in open_positions:
            instrument_token = position.get("instrument_token")
            entry_price = position.get("entry_price")
            target = position.get("target")
            stop_loss = position.get("stop_loss")
            signal_type = position.get("signal")
            position_id = position.get("id")
            lot_size = position.get("lot_size", 1)
            
            if not instrument_token or entry_price is None:
                continue
            
            # Get current price from websocket
            current_price = self.websocket.get_price(instrument_token)
            if current_price is None:
                continue
            
            exit_triggered = False
            exit_reason = None
            
            # Check target and stop loss
            if signal_type == "PE":
                # PE: Profit when price goes down, loss when price goes up
                if target and current_price <= target:
                    exit_triggered = True
                    exit_reason = "target"
                elif stop_loss and current_price >= stop_loss:
                    exit_triggered = True
                    exit_reason = "stop_loss"
            elif signal_type == "CE":
                # CE: Profit when price goes up, loss when price goes down
                if target and current_price >= target:
                    exit_triggered = True
                    exit_reason = "target"
                elif stop_loss and current_price <= stop_loss:
                    exit_triggered = True
                    exit_reason = "stop_loss"
            
            if exit_triggered:
                # Calculate profit/loss
                if signal_type == "PE":
                    # PE: Profit = (entry_price - exit_price) * lot_size
                    profit_loss = (entry_price - current_price) * lot_size
                else:  # CE
                    # CE: Profit = (exit_price - entry_price) * lot_size
                    profit_loss = (current_price - entry_price) * lot_size
                
                profit_loss_pct = ((current_price - entry_price) / entry_price) * 100
                if signal_type == "PE":
                    profit_loss_pct = -profit_loss_pct  # Invert for PE
                
                print(f"✅ Exit condition met for position {position_id}: {exit_reason}, "
                      f"entry={entry_price}, exit={current_price}, P&L={profit_loss:.2f}")
                
                # Update position with exit information
                self.db.update_position_exit(
                    position_id, current_price, exit_reason, 
                    profit_loss, profit_loss_pct
                )
                
                # Unsubscribe from this instrument if no more active positions/orders
                # (Keep subscribed if there are other orders/positions for same token)
                self._cleanup_subscriptions()

    def _cleanup_subscriptions(self):
        """Clean up websocket subscriptions for instruments with no active orders/positions."""
        active_tokens = set()
        
        # Collect tokens from open orders
        for order in self.db.get_open_orders():
            token = order.get("instrument_token")
            if token:
                active_tokens.add(token)
        
        # Collect tokens from open positions
        for position in self.db.get_open_positions():
            token = position.get("instrument_token")
            if token:
                active_tokens.add(token)
        
        # Unsubscribe from tokens not in active set
        subscribed_tokens = set(self.websocket.subscribed_tokens)
        tokens_to_unsubscribe = subscribed_tokens - active_tokens
        
        if tokens_to_unsubscribe:
            self.websocket.unsubscribe(list(tokens_to_unsubscribe))

    def _signal_check_loop(self):
        """Background thread to check for new signals every 15 minutes."""
        while self.running:
            try:
                now = datetime.now()
                time_since_last_check = (now - self.last_signal_check).total_seconds()
                
                if time_since_last_check >= self.signal_check_interval:
                    self._process_new_signals()
                    self.last_signal_check = now
                
                # Sleep for 1 minute and check again
                time.sleep(60)
                
            except Exception as e:
                print(f"⚠️ Error in signal check loop: {e}")
                time.sleep(60)

    def _process_new_signals(self):
        """Check for new signals and add to open_orders."""
        print(f"🔍 Checking for new signals at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        new_signals = self.signal_monitor.get_all_new_signals()
        
        if new_signals:
            print(f"✅ Found {len(new_signals)} new signal(s) from unprocessed files")
            tokens_to_subscribe = []
            
            for new_signal in new_signals:
                print(f"  Processing signal: {new_signal.get('symbol')} - {new_signal.get('signal')}")
                
                # Remove internal metadata before inserting
                signal_data = {k: v for k, v in new_signal.items() 
                              if not k.startswith('_') and k != 'options'}
                
                # Insert into open_orders
                order_id = self.db.insert_order(signal_data)
                print(f"  ✅ Order {order_id} created for {signal_data.get('symbol')}")
                
                # Collect instrument tokens for websocket subscription
                instrument_token = signal_data.get("instrument_token")
                if instrument_token:
                    tokens_to_subscribe.append(instrument_token)
            
            # Subscribe to all instruments at once
            if tokens_to_subscribe:
                self.websocket.subscribe(tokens_to_subscribe)
                print(f"✅ Subscribed to {len(tokens_to_subscribe)} instrument(s) for websocket monitoring")
        else:
            print("ℹ️ No new signals found")

    def start(self):
        """Start the trade manager."""
        print("🚀 Starting Trade Manager...")
        self.running = True
        
        # Connect websocket
        self.websocket.connect()
        
        # Wait a bit for websocket to connect
        time.sleep(2)
        
        # Subscribe to existing open orders and positions
        self._subscribe_to_active_instruments()
        
        # Start signal check thread
        self.signal_check_thread = threading.Thread(target=self._signal_check_loop, daemon=True)
        self.signal_check_thread.start()
        
        # Process any existing signals immediately
        self._process_new_signals()
        
        print("✅ Trade Manager started")

    def stop(self):
        """Stop the trade manager."""
        print("🛑 Stopping Trade Manager...")
        self.running = False
        
        if self.websocket:
            self.websocket.disconnect()
        
        if self.db:
            self.db.close()
        
        print("✅ Trade Manager stopped")

    def run(self):
        """Run the trade manager (blocking)."""
        try:
            self.start()
            
            # Keep main thread alive
            while self.running:
                time.sleep(1)
                
        except KeyboardInterrupt:
            print("\n⚠️ Interrupted by user")
        except Exception as e:
            print(f"❌ Error in trade manager: {e}")
        finally:
            self.stop()
