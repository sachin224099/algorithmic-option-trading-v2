import pandas as pd
from functools import partial
from collections import defaultdict
from typing import List, Dict, Any, Optional, Callable
import os
from indicators.breakout import breakout_signal
from indicators.indicator_metrics_calculator import (
    calculate_price_change_pct,
    calculate_oi_change_pct,
    calculate_volume_spike_ratio,
    classify_oi_structure,
    identify_oi_trend,
    calculate_atr,
    calculate_atr_percentage_from_value,
    calculate_atr_series,
    is_atr_expanding_3_candles,
    is_atr_expanding_2_candles,
    extract_vwap_context,
    detect_compression
)
from indicators.breakout import liquidity_sweep_breakout
from indicators.bollinger_compression_detector import BollingerCompressionDetector
from scoring.signal_scorer import SignalScorer
from core.config_loader import Config


def generate_signals_for_symbol(
    symbol: str,
    futures_symbol: str,
    df_closed: pd.DataFrame,
    config: Config,
    scorer: SignalScorer
) -> Optional[Dict[str, Any]]:
    """
    Generate signal for a single symbol using historical candle data.
    
    This function replicates the logic from get_fifteen_min_signals but works
    with historical data slices for backtesting.
    
    Args:
        symbol: Stock symbol
        futures_symbol: Futures symbol
        df_closed: DataFrame with closed candles (excludes current forming candle)
        config: Config object
        scorer: Pre-initialized SignalScorer object
        
    Returns:
        Signal dictionary with all metrics and score, or None if no signal
    """
    if df_closed is None or len(df_closed) < 2:
        return None
    
    # Check for breakout signal
    signal_type = breakout_signal(df_closed, config.get_lookback_candles())
    
    if signal_type is None:
        return None
    
    # Calculate all metrics
    try:
        volume_spike_ratio = calculate_volume_spike_ratio(df_closed)
        price_change_pct = calculate_price_change_pct(df_closed)
        
        # OI data might not be available in historical backtest
        oi_change_pct = None
        if 'oi' in df_closed.columns:
            oi_change_pct = calculate_oi_change_pct(df_closed)
        
        oi_structure = None
        if oi_change_pct is not None:
            oi_structure = classify_oi_structure(price_change_pct, oi_change_pct, 0.5)
        
        oi_trend = None
        if 'oi' in df_closed.columns:
            oi_trend = identify_oi_trend(df_closed)
        
        current_atr = calculate_atr(df_closed)
        atr_series = calculate_atr_series(df_closed)
        atr_percentage = calculate_atr_percentage_from_value(current_atr, df_closed.iloc[-1]["close"])
        candle_range = df_closed.iloc[-1]["high"] - df_closed.iloc[-1]["low"]
        range_ratio = candle_range / df_closed.iloc[-1]["close"] if df_closed.iloc[-1]["close"] > 0 else 0
        atr_expanding_3_candles = is_atr_expanding_3_candles(atr_series)
        atr_expanding_2_candles = is_atr_expanding_2_candles(atr_series)
        vwap_context = extract_vwap_context(df_closed)
        is_compression = detect_compression(df_closed, atr_series, config.get_compression_window())
        is_liquidity_sweep_breakout_flag = liquidity_sweep_breakout(df_closed, config.get_lookback_candles())
        
        # Calculate Bollinger compression
        try:
            bb_detector = BollingerCompressionDetector()
            is_bollinger_compression = bb_detector.latest_signal(df_closed)
        except Exception:
            is_bollinger_compression = False
        
        # Build signal dictionary
        signal = {
            "symbol": symbol,
            "futures_symbol": futures_symbol,
            "signal": signal_type,
            "close": df_closed.iloc[-1]["close"],
            "volume_spike_ratio": volume_spike_ratio,
            "price_change_pct": price_change_pct,
            "oi_change_pct": oi_change_pct,
            "oi_structure": oi_structure,
            "oi_trend": oi_trend,
            "current_atr": current_atr,
            "atr_percentage": atr_percentage,
            "candle_range": candle_range,
            "range_ratio": range_ratio,
            "atr_expanding_3_candles": atr_expanding_3_candles,
            "atr_expanding_2_candles": atr_expanding_2_candles,
            "vwap": vwap_context["vwap"],
            "is_vwap_slope_rising": vwap_context["is_vwap_slope_rising"],
            "above_vwap_duration_min": vwap_context["above_vwap_duration_min"],
            "is_compression": is_compression,
            "is_bollinger_compression": is_bollinger_compression,
            "is_liquidity_sweep_breakout": is_liquidity_sweep_breakout_flag,
            "options": {
                "volume_spike": 1.0  # Default value for backtest (options data not available)
            }
        }
        
        # Calculate score using pre-initialized scorer
        score = scorer.calculate_signal_score(signal)
        signal["score"] = score
        
        return signal
        
    except Exception as e:
        # Silently skip errors during backtest
        return None


def generate_signals(futures_df: pd.DataFrame, config: Config, scorer: SignalScorer) -> List[Dict[str, Any]]:
    """
    Default signal generator function for backtesting.
    
    Generates signals for all symbols using the data slice provided.
    This function matches the expected interface: takes a DataFrame slice
    and returns a list of signal dictionaries.
    
    Args:
        futures_df: DataFrame slice with historical candles up to current timestamp.
                   Must have columns: date, open, high, low, close, volume
                   Should have columns: symbol, futures_symbol
        config: Pre-initialized Config object
        scorer: Pre-initialized SignalScorer object
                   
    Returns:
        List of signal dictionaries with all metrics and scores
    """
    signals = []
    
    # Group by symbol
    if 'symbol' not in futures_df.columns:
        return signals
    
    if len(futures_df) == 0:
        return signals
    
    # Process each symbol
    for symbol in futures_df['symbol'].unique():
        # Filter symbol data
        symbol_mask = futures_df['symbol'] == symbol
        symbol_data = futures_df[symbol_mask]
        
        if len(symbol_data) < 2:
            continue
        
        # Get futures_symbol (should be same for all rows of a symbol)
        futures_symbol = symbol_data['futures_symbol'].iloc[0] if 'futures_symbol' in symbol_data.columns else symbol
        
        # Use closed candles (exclude the current forming candle)
        # Must copy because signal generation functions modify the DataFrame
        df_closed = symbol_data.iloc[:-1].copy() if len(symbol_data) > 1 else symbol_data.copy()
        
        if len(df_closed) < config.get_lookback_candles() + 5:
            continue
        
        signal = generate_signals_for_symbol(symbol, futures_symbol, df_closed, config, scorer)
        
        if signal is not None:
            signals.append(signal)
    
    return signals


class BreakoutBacktestEngine:
    """
    Backtesting engine for breakout signals with position tracking.
    
    Features:
    - Tracks open positions (one per symbol)
    - Positions carry overnight
    - Entry: 9:30 AM to 3:15 PM
    - Exit: Stop Loss (0.5%) or Target (1%) only
    - No forced EOD exit
    """
    
    # Trade parameters
    STOP_LOSS_PCT = 1.0  # 0.5%
    TARGET_PCT = 2.0     # 1%
    LOT_SIZE = 1
    
    # Market hours
    MARKET_OPEN_HOUR = 9
    MARKET_OPEN_MINUTE = 15
    MARKET_CLOSE_HOUR = 15
    MARKET_CLOSE_MINUTE = 30
    
    # Entry window
    FIRST_ENTRY_HOUR = 9
    FIRST_ENTRY_MINUTE = 30
    LAST_ENTRY_HOUR = 15
    LAST_ENTRY_MINUTE = 15

    def __init__(self, futures_df: pd.DataFrame, signal_generator: Optional[Callable] = None, futures_master: Optional[pd.DataFrame] = None):
        """
        Initialize backtest engine.
        
        Args:
            futures_df: DataFrame with historical futures candles.
                       Must have columns: date (or datetime), open, high, low, close, volume
                       Should have columns: symbol, futures_symbol
            signal_generator: Optional custom signal generator function.
                            If None, uses default generate_signals
            futures_master: Optional DataFrame with futures master data including lot_size.
                           If None, will load from data_cache/futures_master.csv
        """
        # Store original reference
        self.futures_df = futures_df
        self.signal_generator = signal_generator
        self.trade_results = []
        
        # Load lot_size mapping from futures_master
        if futures_master is None:
            # Try to load from CSV file
            futures_master_path = "data_cache/futures_master.csv"
            if os.path.exists(futures_master_path):
                futures_master = pd.read_csv(futures_master_path)
            else:
                print("⚠️ futures_master.csv not found. Using default lot_size = 1")
                futures_master = pd.DataFrame()
        
        # Create symbol -> lot_size mapping
        if not futures_master.empty and 'lot_size' in futures_master.columns and 'symbol' in futures_master.columns:
            self.lot_size_map = dict(zip(futures_master['symbol'], futures_master['lot_size']))
        else:
            self.lot_size_map = {}
        
        # Helper method to get lot_size for a symbol
        def get_lot_size(symbol: str) -> int:
            return self.lot_size_map.get(symbol, self.LOT_SIZE)
        
        self._get_lot_size = get_lot_size
        
        # Ensure date column exists
        if 'date' not in self.futures_df.columns:
            if isinstance(self.futures_df.index, pd.DatetimeIndex):
                self.futures_df = self.futures_df.reset_index()
                if 'date' not in self.futures_df.columns:
                    # Try to find datetime column
                    for col in self.futures_df.columns:
                        if pd.api.types.is_datetime64_any_dtype(self.futures_df[col]):
                            self.futures_df = self.futures_df.rename(columns={col: 'date'})
                            break
            else:
                raise ValueError("futures_df must have a 'date' column or datetime index")
        
        # Ensure date is datetime and convert to IST (Asia/Kolkata)
        # First, convert to datetime
        self.futures_df['date'] = pd.to_datetime(self.futures_df['date'])
        
        # Convert to IST: if timezone-naive, assume UTC and convert; if timezone-aware, convert directly
        if self.futures_df['date'].dt.tz is None:
            # Timezone-naive: assume UTC and convert to IST
            self.futures_df['date'] = self.futures_df['date'].dt.tz_localize('UTC').dt.tz_convert('Asia/Kolkata')
        else:
            # Timezone-aware: convert to IST
            self.futures_df['date'] = self.futures_df['date'].dt.tz_convert('Asia/Kolkata')
        
        # Sort by date once
        self.futures_df = self.futures_df.sort_values('date').reset_index(drop=True)
        
        # Initialize reusable objects once
        self.config = Config()
        self.scorer = SignalScorer(self.config)
        
        # Create a wrapper for the signal generator
        if signal_generator is None:
            self._signal_generator_wrapper = partial(generate_signals, config=self.config, scorer=self.scorer)
        elif hasattr(signal_generator, '__name__') and signal_generator.__name__ == 'generate_signals':
            self._signal_generator_wrapper = partial(generate_signals, config=self.config, scorer=self.scorer)
        else:
            self._signal_generator_wrapper = signal_generator
        
        # PRE-SPLIT DATA BY SYMBOL (MAJOR SPEED IMPROVEMENT)
        if 'symbol' in self.futures_df.columns:
            self.symbol_data = {}
            for symbol in self.futures_df['symbol'].unique():
                symbol_df = self.futures_df[self.futures_df['symbol'] == symbol].copy()
                symbol_df = symbol_df.sort_values('date').reset_index(drop=True)
                self.symbol_data[symbol] = symbol_df
        else:
            # Single symbol case - add dummy symbol column for consistency
            df_single = self.futures_df.copy()
            df_single['symbol'] = '_single'
            if 'futures_symbol' not in df_single.columns:
                df_single['futures_symbol'] = '_single'
            self.symbol_data = {'_single': df_single}

    def _is_within_entry_window(self, timestamp: pd.Timestamp) -> bool:
        """Check if timestamp is within entry window (9:30 AM to 3:15 PM)."""
        hour = timestamp.hour
        minute = timestamp.minute
        
        # After 9:30 AM
        if hour < self.FIRST_ENTRY_HOUR or (hour == self.FIRST_ENTRY_HOUR and minute < self.FIRST_ENTRY_MINUTE):
            return False
        
        # Before 3:15 PM
        if hour > self.LAST_ENTRY_HOUR or (hour == self.LAST_ENTRY_HOUR and minute > self.LAST_ENTRY_MINUTE):
            return False
        
        return True

    def _calculate_sl_and_target(self, entry_price: float, signal_type: str) -> tuple:
        """
        Calculate stop loss and target prices.
        
        Args:
            entry_price: Entry price
            signal_type: "CE" (long) or "PE" (short)
            
        Returns:
            (stop_loss, target) tuple
        """
        if signal_type == "CE":
            # Long position
            stop_loss = entry_price * (1 - self.STOP_LOSS_PCT / 100)
            target = entry_price * (1 + self.TARGET_PCT / 100)
        else:  # PE
            # Short position
            stop_loss = entry_price * (1 + self.STOP_LOSS_PCT / 100)
            target = entry_price * (1 - self.TARGET_PCT / 100)
        
        return stop_loss, target

    def _update_trailing_sl(self, position: Dict, candle: pd.Series) -> None:
        """
        Update trailing stop loss: once price moves 1% toward target, move SL to entry_price.
        
        Args:
            position: Position dictionary (modified in place)
            candle: Current candle data
        """
        signal_type = position['signal']
        entry_price = position['entry_price']
        target = position['target']
        high = candle['high']
        low = candle['low']
        
        # Check if trailing SL has already been activated
        if position.get('trailing_sl_activated', False):
            return
        
        if signal_type == "CE":
            # Long position: check if price moved 1% toward target
            # 1% toward target = entry_price * 1.01
            trailing_trigger_price = entry_price * 1.01
            if high >= trailing_trigger_price:
                # Move SL to entry_price
                position['stop_loss'] = entry_price
                position['trailing_sl_activated'] = True
        else:  # PE
            # Short position: check if price moved 1% toward target
            # 1% toward target = entry_price * 0.99
            trailing_trigger_price = entry_price * 0.99
            if low <= trailing_trigger_price:
                # Move SL to entry_price
                position['stop_loss'] = entry_price
                position['trailing_sl_activated'] = True

    def _check_exit(self, position: Dict, candle: pd.Series) -> Optional[Dict]:
        """
        Check if position should exit based on current candle.
        
        Args:
            position: Position dictionary
            candle: Current candle data
            
        Returns:
            Exit dictionary if exit triggered, None otherwise
        """
        # Update trailing stop loss before checking exit
        self._update_trailing_sl(position, candle)
        
        signal_type = position['signal']
        stop_loss = position['stop_loss']
        target = position['target']
        high = candle['high']
        low = candle['low']
        
        if signal_type == "CE":
            # Long position: exit on low <= SL or high >= target
            if low <= stop_loss:
                return {
                    'exit_reason': 'SL',
                    'exit_price': stop_loss,
                    'exit_time': candle['date']
                }
            elif high >= target:
                return {
                    'exit_reason': 'TARGET',
                    'exit_price': target,
                    'exit_time': candle['date']
                }
        else:  # PE
            # Short position: exit on high >= SL or low <= target
            if high >= stop_loss:
                return {
                    'exit_reason': 'SL',
                    'exit_price': stop_loss,
                    'exit_time': candle['date']
                }
            elif low <= target:
                return {
                    'exit_reason': 'TARGET',
                    'exit_price': target,
                    'exit_time': candle['date']
                }
        
        return None

    def run(self) -> pd.DataFrame:
        """
        Run backtest with position tracking.
        
        Processes ALL signals with score > 50 at each timestamp.
        
        Returns:
            DataFrame with trade results
        """
        self.trade_results = []
        
        # Track open positions: {symbol: position_dict}
        open_positions = {}
        
        # Get all candles sorted by timestamp
        all_candles = []
        for symbol, df in self.symbol_data.items():
            for idx, row in df.iterrows():
                all_candles.append({
                    'symbol': symbol,
                    'futures_symbol': row.get('futures_symbol', symbol),
                    'candle': row,
                    'timestamp': row['date']
                })
        
        # Sort all candles by timestamp
        all_candles.sort(key=lambda x: x['timestamp'])
        
        # Group candles by timestamp to process all symbols at once
        candles_by_timestamp = defaultdict(list)
        for candle_data in all_candles:
            timestamp = candle_data['timestamp']
            candles_by_timestamp[timestamp].append(candle_data)
        
        # Process each unique timestamp
        for timestamp in sorted(candles_by_timestamp.keys()):
            candles_at_timestamp = candles_by_timestamp[timestamp]
            
            # First, check for exits on all symbols at this timestamp
            for candle_data in candles_at_timestamp:
                symbol = candle_data['symbol']
                futures_symbol = candle_data['futures_symbol']
                candle = candle_data['candle']
                
                if symbol in open_positions:
                    position = open_positions[symbol]
                    exit_info = self._check_exit(position, candle)
                    
                    if exit_info:
                        # Position exited
                        entry_price = position['entry_price']
                        exit_price = exit_info['exit_price']
                        
                        # Calculate profit points
                        if position['signal'] == "CE":
                            profit_points = exit_price - entry_price
                        else:  # PE
                            profit_points = entry_price - exit_price
                        
                        # Calculate profit/loss in currency (profit_points * lot_size)
                        lot_size = position.get('lot_size', self.LOT_SIZE)
                        profit_loss = profit_points * lot_size
                        
                        # Determine target hit and SL hit flags
                        exit_reason = exit_info['exit_reason']
                        is_target_hit = 1 if exit_reason == "TARGET" else 0
                        is_sl_hit = 1 if exit_reason == "SL" else 0
                        
                        # Record trade result with all indicator variables
                        trade_result = {
                            # Basic Trade Info
                            'date': timestamp.date(),
                            'symbol': symbol,
                            'futures_symbol': futures_symbol,
                            'signal': position['signal'],
                            'score': position.get('score', 0),
                            'entry_time': position['entry_time'],
                            'entry_price': entry_price,
                            'exit_time': exit_info['exit_time'],
                            'exit_price': exit_price,
                            'stop_loss': position['stop_loss'],
                            'target': position['target'],
                            'exit_reason': exit_reason,
                            'lot_size': lot_size,
                            'profit_points': profit_points,
                            'profit_loss': profit_loss,
                            'is_target_hit': is_target_hit,
                            'is_sl_hit': is_sl_hit,
                            # Indicator Variables
                            'volume_spike_ratio': position.get('volume_spike_ratio'),
                            'price_change_pct': position.get('price_change_pct'),
                            'oi_change_pct': position.get('oi_change_pct'),
                            'oi_structure': position.get('oi_structure'),
                            'oi_trend': position.get('oi_trend'),
                            'current_atr': position.get('current_atr'),
                            'atr_percentage': position.get('atr_percentage'),
                            'candle_range': position.get('candle_range'),
                            'range_ratio': position.get('range_ratio'),
                            'atr_expanding_3_candles': position.get('atr_expanding_3_candles'),
                            'atr_expanding_2_candles': position.get('atr_expanding_2_candles'),
                            'vwap': position.get('vwap'),
                            'is_vwap_slope_rising': position.get('is_vwap_slope_rising'),
                            'above_vwap_duration_min': position.get('above_vwap_duration_min'),
                            'is_compression': position.get('is_compression'),
                            'is_bollinger_compression': position.get('is_bollinger_compression'),
                            'is_liquidity_sweep_breakout': position.get('is_liquidity_sweep_breakout')
                        }
                        
                        self.trade_results.append(trade_result)
                        
                        # Remove position
                        del open_positions[symbol]
            
            # Then, check for new entries (only during entry window)
            if self._is_within_entry_window(timestamp):
                # Build combined DataFrame with all symbols up to this timestamp
                combined_slice = []
                for symbol, df in self.symbol_data.items():
                    symbol_candles = df[df['date'] <= timestamp]
                    if len(symbol_candles) >= 50:  # Need enough lookback data
                        combined_slice.append(symbol_candles)
                
                if combined_slice:
                    # Combine all symbol data up to this timestamp
                    all_data_up_to_now = pd.concat(combined_slice, ignore_index=True)
                    
                    # Generate signals for all symbols at this timestamp
                    signals = self._signal_generator_wrapper(all_data_up_to_now)
                    
                    if signals and len(signals) > 0:
                        # Process ALL signals (no score filter - capture all for statistical analysis)
                        for signal in signals:
                            signal_score = signal.get("score", 0)
                            signal_symbol = signal.get("symbol")
                            
                            # Skip if symbol already has an open position
                            if signal_symbol in open_positions:
                                continue
                            
                            # Get the candle for this symbol at this timestamp
                            symbol_candle = None
                            for candle_data in candles_at_timestamp:
                                if candle_data['symbol'] == signal_symbol:
                                    symbol_candle = candle_data['candle']
                                    futures_symbol = candle_data['futures_symbol']
                                    break
                            
                            if symbol_candle is None:
                                continue
                            
                            entry_price = symbol_candle['close']
                            signal_type = signal.get("signal")
                            stop_loss, target = self._calculate_sl_and_target(entry_price, signal_type)
                            
                            # Get lot_size from futures_master (or use default)
                            lot_size = self._get_lot_size(signal_symbol)
                            
                            # Store all indicator variables from signal
                            # Open new position with all signal data
                            open_positions[signal_symbol] = {
                                'symbol': signal_symbol,
                                'futures_symbol': futures_symbol,
                                'signal': signal_type,
                                'score': signal_score,
                                'entry_time': timestamp,
                                'entry_price': entry_price,
                                'stop_loss': stop_loss,
                                'target': target,
                                'lot_size': lot_size,
                                # Store all indicator variables from signal
                                'volume_spike_ratio': signal.get("volume_spike_ratio"),
                                'price_change_pct': signal.get("price_change_pct"),
                                'oi_change_pct': signal.get("oi_change_pct"),
                                'oi_structure': signal.get("oi_structure"),
                                'oi_trend': signal.get("oi_trend"),
                                'current_atr': signal.get("current_atr"),
                                'atr_percentage': signal.get("atr_percentage"),
                                'candle_range': signal.get("candle_range"),
                                'range_ratio': signal.get("range_ratio"),
                                'atr_expanding_3_candles': signal.get("atr_expanding_3_candles"),
                                'atr_expanding_2_candles': signal.get("atr_expanding_2_candles"),
                                'vwap': signal.get("vwap"),
                                'is_vwap_slope_rising': signal.get("is_vwap_slope_rising"),
                                'above_vwap_duration_min': signal.get("above_vwap_duration_min"),
                                'is_compression': signal.get("is_compression"),
                                'is_bollinger_compression': signal.get("is_bollinger_compression"),
                                'is_liquidity_sweep_breakout': signal.get("is_liquidity_sweep_breakout")
                            }
        
        # Close any remaining open positions at the end of data
        for symbol, position in open_positions.items():
            # Use last available price as exit
            symbol_df = self.symbol_data[symbol]
            last_candle = symbol_df.iloc[-1]
            exit_price = last_candle['close']
            entry_price = position['entry_price']
            
            # Calculate profit points
            if position['signal'] == "CE":
                profit_points = exit_price - entry_price
            else:  # PE
                profit_points = entry_price - exit_price
            
            # Calculate profit/loss in currency (profit_points * lot_size)
            lot_size = position.get('lot_size', self.LOT_SIZE)
            profit_loss = profit_points * lot_size
            
            # Determine target hit and SL hit flags (both 0 for END_OF_DATA)
            is_target_hit = 0
            is_sl_hit = 0
            
            trade_result = {
                # Basic Trade Info
                'date': last_candle['date'].date(),
                'symbol': symbol,
                'futures_symbol': position['futures_symbol'],
                'signal': position['signal'],
                'score': position.get('score', 0),
                'entry_time': position['entry_time'],
                'entry_price': entry_price,
                'exit_time': last_candle['date'],
                'exit_price': exit_price,
                'stop_loss': position['stop_loss'],
                'target': position['target'],
                'exit_reason': 'END_OF_DATA',
                'lot_size': lot_size,
                'profit_points': profit_points,
                'profit_loss': profit_loss,
                'is_target_hit': is_target_hit,
                'is_sl_hit': is_sl_hit,
                # Indicator Variables
                'volume_spike_ratio': position.get('volume_spike_ratio'),
                'price_change_pct': position.get('price_change_pct'),
                'oi_change_pct': position.get('oi_change_pct'),
                'oi_structure': position.get('oi_structure'),
                'oi_trend': position.get('oi_trend'),
                'current_atr': position.get('current_atr'),
                'atr_percentage': position.get('atr_percentage'),
                'candle_range': position.get('candle_range'),
                'range_ratio': position.get('range_ratio'),
                'atr_expanding_3_candles': position.get('atr_expanding_3_candles'),
                'atr_expanding_2_candles': position.get('atr_expanding_2_candles'),
                'vwap': position.get('vwap'),
                'is_vwap_slope_rising': position.get('is_vwap_slope_rising'),
                'above_vwap_duration_min': position.get('above_vwap_duration_min'),
                'is_compression': position.get('is_compression'),
                'is_bollinger_compression': position.get('is_bollinger_compression'),
                'is_liquidity_sweep_breakout': position.get('is_liquidity_sweep_breakout')
            }
            
            self.trade_results.append(trade_result)
        
        return pd.DataFrame(self.trade_results)

    def export(self, df: pd.DataFrame, output_file: str = "backtest_trade_results_full.xlsx"):
        """
        Export backtest trade results to Excel.
        
        Args:
            df: DataFrame with trade results
            output_file: Output Excel file path
        """
        if df is None or len(df) == 0:
            print("⚠️ No data to export")
            return
        
        # Create a copy to avoid modifying the original DataFrame
        df_export = df.copy()
        
        # Remove any nested objects (like "options") to ensure flat dataset
        # Only keep scalar values and simple types
        for col in df_export.columns:
            # Check if column contains dict/list objects
            if df_export[col].dtype == 'object':
                # Check if any value is a dict or list
                sample_values = df_export[col].dropna().head(10)
                if len(sample_values) > 0:
                    first_val = sample_values.iloc[0]
                    if isinstance(first_val, (dict, list)):
                        # Drop this column as it contains nested objects
                        df_export = df_export.drop(columns=[col])
                        print(f"⚠️ Dropped column '{col}' - contains nested objects")
        
        # Convert datetime columns to IST and remove timezone for Excel export
        # Excel doesn't support timezone-aware datetimes, so we convert to IST first, then make naive
        for col in df_export.columns:
            if pd.api.types.is_datetime64_any_dtype(df_export[col]):
                # If timezone-aware, convert to IST first
                if df_export[col].dt.tz is not None:
                    df_export[col] = df_export[col].dt.tz_convert('Asia/Kolkata')
                # If timezone-naive, assume it's already in IST and keep as is
                # Remove timezone info to make it naive (Excel requirement)
                df_export[col] = df_export[col].apply(lambda x: x.replace(tzinfo=None) if hasattr(x, 'tzinfo') and x.tzinfo is not None else x)
        
        df_export.to_excel(output_file, index=False)
        print(f"✅ Exported {len(df_export)} trade results to {output_file}")
        print(f"   Columns: {len(df_export.columns)}")
        print(f"   Indicator columns included: {sum(1 for col in df_export.columns if col not in ['date', 'symbol', 'futures_symbol', 'signal', 'score', 'entry_time', 'entry_price', 'exit_time', 'exit_price', 'stop_loss', 'target', 'exit_reason', 'lot_size', 'profit_points', 'profit_loss', 'is_target_hit', 'is_sl_hit'])}")