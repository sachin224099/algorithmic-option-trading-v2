"""
Database module for trade_manager.
Handles SQLite database operations for open_orders and open_positions tables.
"""

import sqlite3
import os
from datetime import datetime
from typing import Dict, List, Optional, Any


class TradeDatabase:
    """Manages SQLite database for trade orders and positions."""

    def __init__(self, db_path: str = "trade_manager/trades.db"):
        """
        Initialize database connection.
        
        Args:
            db_path: Path to SQLite database file
        """
        self.db_path = db_path
        # Create directory if it doesn't exist
        db_dir = os.path.dirname(db_path)
        if db_dir:  # Only create directory if path contains a directory
            os.makedirs(db_dir, exist_ok=True)
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.create_tables()

    def create_tables(self):
        """Create open_orders and open_positions tables if they don't exist."""
        cursor = self.conn.cursor()

        # Create open_orders table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS open_orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                order_status TEXT NOT NULL DEFAULT 'open',
                signal_timestamp TEXT,
                symbol TEXT,
                futures_symbol TEXT,
                instrument_token INTEGER,
                expiry TEXT,
                lot_size INTEGER,
                signal TEXT,
                close REAL,
                volume_spike_ratio REAL,
                price_change_pct REAL,
                oi_change_pct REAL,
                oi_structure TEXT,
                oi_trend TEXT,
                current_atr REAL,
                atr_percentage REAL,
                candle_range REAL,
                range_ratio REAL,
                atr_expanding_3_candles INTEGER,
                atr_expanding_2_candles INTEGER,
                vwap REAL,
                is_vwap_slope_rising INTEGER,
                above_vwap_duration_min INTEGER,
                is_compression INTEGER,
                is_liquidity_sweep_breakout INTEGER,
                is_bollinger_compression INTEGER,
                target REAL,
                stop_loss REAL,
                score REAL,
                score_category TEXT,
                created_at TEXT NOT NULL,
                executed_at TEXT
            )
        """)

        # Create open_positions table (all columns from open_orders except order_status, plus position-specific fields)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS open_positions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                signal_timestamp TEXT,
                symbol TEXT,
                futures_symbol TEXT,
                instrument_token INTEGER,
                expiry TEXT,
                lot_size INTEGER,
                signal TEXT,
                close REAL,
                volume_spike_ratio REAL,
                price_change_pct REAL,
                oi_change_pct REAL,
                oi_structure TEXT,
                oi_trend TEXT,
                current_atr REAL,
                atr_percentage REAL,
                candle_range REAL,
                range_ratio REAL,
                atr_expanding_3_candles INTEGER,
                atr_expanding_2_candles INTEGER,
                vwap REAL,
                is_vwap_slope_rising INTEGER,
                above_vwap_duration_min INTEGER,
                is_compression INTEGER,
                is_liquidity_sweep_breakout INTEGER,
                is_bollinger_compression INTEGER,
                target REAL,
                stop_loss REAL,
                score REAL,
                score_category TEXT,
                entry_price REAL NOT NULL,
                exit_price REAL,
                exit_reason TEXT,
                profit_loss REAL,
                profit_loss_pct REAL,
                created_at TEXT NOT NULL,
                executed_at TEXT,
                exited_at TEXT
            )
        """)

        # Create processed_signals table to track which signal files have been processed
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS processed_signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                file_path TEXT UNIQUE NOT NULL,
                file_timestamp TEXT,
                processed_at TEXT NOT NULL
            )
        """)

        self.conn.commit()

    def insert_order(self, signal_data: Dict[str, Any]) -> int:
        """
        Insert a new order into open_orders table.
        
        Args:
            signal_data: Dictionary containing signal data (excluding options object)
            
        Returns:
            order_id: ID of the inserted order
        """
        cursor = self.conn.cursor()

        # Extract futures_target_sl_context
        target_sl = signal_data.get("futures_target_sl_context", {})
        target = target_sl.get("target") if isinstance(target_sl, dict) else None
        stop_loss = target_sl.get("stop_loss") if isinstance(target_sl, dict) else None

        # Convert boolean values to integers
        atr_expanding_3 = 1 if signal_data.get("atr_expanding_3_candles", False) else 0
        atr_expanding_2 = 1 if signal_data.get("atr_expanding_2_candles", False) else 0
        is_vwap_rising = 1 if signal_data.get("is_vwap_slope_rising", False) else 0
        is_compression = 1 if signal_data.get("is_compression", False) else 0
        is_liquidity_sweep = 1 if signal_data.get("is_liquidity_sweep_breakout", False) else 0
        is_bollinger = 1 if signal_data.get("is_bollinger_compression", False) else 0

        cursor.execute("""
            INSERT INTO open_orders (
                order_status, signal_timestamp, symbol, futures_symbol, instrument_token,
                expiry, lot_size, signal, close, volume_spike_ratio, price_change_pct,
                oi_change_pct, oi_structure, oi_trend, current_atr, atr_percentage,
                candle_range, range_ratio, atr_expanding_3_candles, atr_expanding_2_candles,
                vwap, is_vwap_slope_rising, above_vwap_duration_min, is_compression,
                is_liquidity_sweep_breakout, is_bollinger_compression, target, stop_loss,
                score, score_category, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            "open",
            signal_data.get("timestamp") or datetime.now().isoformat(),
            signal_data.get("symbol"),
            signal_data.get("futures_symbol"),
            signal_data.get("instrument_token"),
            signal_data.get("expiry"),
            signal_data.get("lot_size"),
            signal_data.get("signal"),
            signal_data.get("close"),
            signal_data.get("volume_spike_ratio"),
            signal_data.get("price_change_pct"),
            signal_data.get("oi_change_pct"),
            signal_data.get("oi_structure"),
            signal_data.get("oi_trend"),
            signal_data.get("current_atr"),
            signal_data.get("atr_percentage"),
            signal_data.get("candle_range"),
            signal_data.get("range_ratio"),
            atr_expanding_3,
            atr_expanding_2,
            signal_data.get("vwap"),
            is_vwap_rising,
            signal_data.get("above_vwap_duration_min"),
            is_compression,
            is_liquidity_sweep,
            is_bollinger,
            target,
            stop_loss,
            signal_data.get("score"),
            signal_data.get("score_category"),
            datetime.now().isoformat()
        ))

        self.conn.commit()
        return cursor.lastrowid

    def get_open_orders(self) -> List[Dict[str, Any]]:
        """
        Get all orders with status='open'.
        
        Returns:
            List of order dictionaries
        """
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM open_orders WHERE order_status = 'open'")
        rows = cursor.fetchall()
        return [dict(row) for row in rows]

    def update_order_status(self, order_id: int, status: str, executed_at: Optional[str] = None):
        """
        Update order status.
        
        Args:
            order_id: ID of the order
            status: New status ('open' or 'executed')
            executed_at: ISO timestamp when executed (optional)
        """
        cursor = self.conn.cursor()
        if executed_at is None:
            executed_at = datetime.now().isoformat()
        
        cursor.execute("""
            UPDATE open_orders 
            SET order_status = ?, executed_at = ?
            WHERE id = ?
        """, (status, executed_at, order_id))
        self.conn.commit()

    def move_to_positions(self, order_id: int, entry_price: float, 
                         adjusted_target: Optional[float] = None, 
                         adjusted_stop_loss: Optional[float] = None) -> int:
        """
        Move an order from open_orders to open_positions.
        
        Args:
            order_id: ID of the order to move
            entry_price: Price at which the order was executed
            adjusted_target: Adjusted target price (if None, uses original target)
            adjusted_stop_loss: Adjusted stop_loss price (if None, uses original stop_loss)
            
        Returns:
            position_id: ID of the new position
        """
        cursor = self.conn.cursor()

        # Get the order data
        cursor.execute("SELECT * FROM open_orders WHERE id = ?", (order_id,))
        order = cursor.fetchone()
        if not order:
            raise ValueError(f"Order {order_id} not found")

        order_dict = dict(order)
        executed_at = order_dict.get("executed_at") or datetime.now().isoformat()

        # Use adjusted target/stop_loss if provided, otherwise use original values
        final_target = adjusted_target if adjusted_target is not None else order_dict.get("target")
        final_stop_loss = adjusted_stop_loss if adjusted_stop_loss is not None else order_dict.get("stop_loss")

        # Insert into open_positions (excluding order_status and id)
        cursor.execute("""
            INSERT INTO open_positions (
                signal_timestamp, symbol, futures_symbol, instrument_token,
                expiry, lot_size, signal, close, volume_spike_ratio, price_change_pct,
                oi_change_pct, oi_structure, oi_trend, current_atr, atr_percentage,
                candle_range, range_ratio, atr_expanding_3_candles, atr_expanding_2_candles,
                vwap, is_vwap_slope_rising, above_vwap_duration_min, is_compression,
                is_liquidity_sweep_breakout, is_bollinger_compression, target, stop_loss,
                score, score_category, entry_price, created_at, executed_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            order_dict.get("signal_timestamp"),
            order_dict.get("symbol"),
            order_dict.get("futures_symbol"),
            order_dict.get("instrument_token"),
            order_dict.get("expiry"),
            order_dict.get("lot_size"),
            order_dict.get("signal"),
            order_dict.get("close"),
            order_dict.get("volume_spike_ratio"),
            order_dict.get("price_change_pct"),
            order_dict.get("oi_change_pct"),
            order_dict.get("oi_structure"),
            order_dict.get("oi_trend"),
            order_dict.get("current_atr"),
            order_dict.get("atr_percentage"),
            order_dict.get("candle_range"),
            order_dict.get("range_ratio"),
            order_dict.get("atr_expanding_3_candles"),
            order_dict.get("atr_expanding_2_candles"),
            order_dict.get("vwap"),
            order_dict.get("is_vwap_slope_rising"),
            order_dict.get("above_vwap_duration_min"),
            order_dict.get("is_compression"),
            order_dict.get("is_liquidity_sweep_breakout"),
            order_dict.get("is_bollinger_compression"),
            final_target,
            final_stop_loss,
            order_dict.get("score"),
            order_dict.get("score_category"),
            entry_price,
            order_dict.get("created_at"),
            executed_at
        ))

        # Delete from open_orders
        cursor.execute("DELETE FROM open_orders WHERE id = ?", (order_id,))
        self.conn.commit()

        return cursor.lastrowid

    def get_open_positions(self) -> List[Dict[str, Any]]:
        """
        Get all open positions (where exit_price is NULL).
        
        Returns:
            List of position dictionaries
        """
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM open_positions WHERE exit_price IS NULL")
        rows = cursor.fetchall()
        return [dict(row) for row in rows]

    def update_position_exit(self, position_id: int, exit_price: float, 
                            exit_reason: str, profit_loss: float, profit_loss_pct: float):
        """
        Update position with exit information.
        
        Args:
            position_id: ID of the position
            exit_price: Price at which position was closed
            exit_reason: 'target' or 'stop_loss'
            profit_loss: Calculated profit/loss amount
            profit_loss_pct: Calculated profit/loss percentage
        """
        cursor = self.conn.cursor()
        cursor.execute("""
            UPDATE open_positions 
            SET exit_price = ?, exit_reason = ?, profit_loss = ?, 
                profit_loss_pct = ?, exited_at = ?
            WHERE id = ?
        """, (exit_price, exit_reason, profit_loss, profit_loss_pct, 
              datetime.now().isoformat(), position_id))
        self.conn.commit()

    def mark_signal_processed(self, file_path: str, file_timestamp: str):
        """
        Mark a signal file as processed.
        
        Args:
            file_path: Path to the signal file
            file_timestamp: Timestamp from the signal file
        """
        cursor = self.conn.cursor()
        cursor.execute("""
            INSERT OR IGNORE INTO processed_signals (file_path, file_timestamp, processed_at)
            VALUES (?, ?, ?)
        """, (file_path, file_timestamp, datetime.now().isoformat()))
        self.conn.commit()

    def is_signal_processed(self, file_path: str) -> bool:
        """
        Check if a signal file has been processed.
        
        Args:
            file_path: Path to the signal file
            
        Returns:
            True if processed, False otherwise
        """
        cursor = self.conn.cursor()
        cursor.execute("SELECT 1 FROM processed_signals WHERE file_path = ?", (file_path,))
        return cursor.fetchone() is not None

    def close(self):
        """Close database connection."""
        self.conn.close()
