"""
Example script to run breakout signal backtest.

This script demonstrates how to use the BreakoutBacktestEngine to:
1. Load historical futures candle data
2. Run backtest analysis with dynamic signal generation
3. Export results to Excel
"""

import sys
import os
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import pandas as pd
from datetime import datetime, timedelta
from backtest.breakout_backtest_engine import BreakoutBacktestEngine, generate_signals
from data.zerodha_client import ZerodhaClient
from data.historic_data import get_minutes_data
from core.config_loader import Config


def load_futures_candles(
    futures_master: pd.DataFrame,
    from_date: datetime,
    to_date: datetime,
    timeframe_minutes: int = 15
) -> pd.DataFrame:
    """
    Load historical futures candle data for all symbols in futures_master.
    
    Args:
        futures_master: DataFrame with columns: symbol, futures_symbol, instrument_token
        from_date: Start date for historical data
        to_date: End date for historical data
        timeframe_minutes: Candle timeframe (default: 15)
        
    Returns:
        DataFrame with columns: symbol, futures_symbol, date, open, high, low, close, volume, oi
    """
    config = Config()
    zerodha_client = ZerodhaClient(config)
    kite = zerodha_client.get_kite()
    
    all_candles = []
    
    for _, row in futures_master.iterrows():
        symbol = row.get("symbol")
        futures_symbol = row.get("futures_symbol")
        instrument_token = row.get("instrument_token")
        
        if not instrument_token:
            continue
        
        try:
            df = get_minutes_data(kite, instrument_token, from_date, to_date, timeframe_minutes, oi=True)
            
            if df is None or len(df) == 0:
                continue
            
            # Ensure date column exists
            if 'date' not in df.columns:
                if isinstance(df.index, pd.DatetimeIndex) or df.index.name == 'date':
                    df = df.reset_index()
                    if 'date' not in df.columns and len(df.columns) > 0:
                        for col in df.columns:
                            if pd.api.types.is_datetime64_any_dtype(df[col]):
                                df = df.rename(columns={col: 'date'})
                                break
                else:
                    print(f"⚠️ No date column found for {symbol}")
                    continue
            
            if 'date' in df.columns:
                # Convert to datetime
                df['date'] = pd.to_datetime(df['date'])
                
                # Convert to IST: if timezone-naive, assume UTC and convert; if timezone-aware, convert directly
                if df['date'].dt.tz is None:
                    # Timezone-naive: assume UTC and convert to IST
                    df['date'] = df['date'].dt.tz_localize('UTC').dt.tz_convert('Asia/Kolkata')
                else:
                    # Timezone-aware: convert to IST
                    df['date'] = df['date'].dt.tz_convert('Asia/Kolkata')
            else:
                print(f"⚠️ Could not establish date column for {symbol}")
                continue
            
            # Add symbol columns for merging
            df['symbol'] = symbol
            df['futures_symbol'] = futures_symbol
            
            all_candles.append(df)
            
        except Exception as e:
            print(f"⚠️ Error loading candles for {symbol}: {e}")
            continue
    
    if not all_candles:
        return pd.DataFrame()
    
    # Combine all candles
    futures_df = pd.concat(all_candles, ignore_index=True)
    
    print(f"✅ Loaded futures candles for {len(futures_master)} symbols")
    
    return futures_df


def run_backtest_example():
    """
    Example backtest run with dynamic signal generation.
    """
    print("=" * 60)
    print("Breakout Signal Backtest Engine")
    print("=" * 60)
    
    # Step 1: Load futures master
    print("\n📊 Step 1: Loading futures master...")
    config = Config()
    zerodha_client = ZerodhaClient(config)
    futures_master = zerodha_client.futures_master
    
    if len(futures_master) == 0:
        print("❌ No futures master data found")
        return
    
    print(f"✅ Loaded {len(futures_master)} futures instruments")
    
    # Step 2: Determine date range for historical data
    # Use last 30 days for backtest (adjust as needed)
    to_date = datetime.now()
    from_date = to_date - timedelta(days=30)
    
    # Ensure we have enough future candles (4 candles = 1 hour)
    to_date = to_date + timedelta(hours=2)
    
    print(f"\n📊 Step 2: Loading historical futures candles...")
    print(f"   Date range: {from_date} to {to_date}")
    
    # Load futures candles
    futures_df = load_futures_candles(
        futures_master=futures_master,
        from_date=from_date,
        to_date=to_date,
        timeframe_minutes=15
    )
    
    if len(futures_df) == 0:
        print("❌ No futures candle data loaded")
        return
    
    print(f"✅ Loaded {len(futures_df)} futures candles")
    print(f"   Date range: {futures_df['date'].min()} to {futures_df['date'].max()}")
    
    # Step 3: Initialize backtest engine
    print("\n📊 Step 3: Initializing backtest engine...")
    engine = BreakoutBacktestEngine(
        futures_df=futures_df,
        signal_generator=None,  # Uses default signal generator (None = default)
        futures_master=futures_master  # Pass futures_master for lot_size lookup
    )
    
    # Step 4: Run backtest
    print("\n📊 Step 4: Running backtest analysis...")
    print("   This will generate signals dynamically at each 15-minute step...")
    results_df = engine.run()
    
    if len(results_df) == 0:
        print("❌ No results generated")
        return
    
    print(f"✅ Backtest completed: {len(results_df)} trades executed")
    
    # Step 5: Display summary statistics
    print("\n📊 Step 5: Backtest Trade Results Summary")
    print("=" * 60)
    total_trades = len(results_df)
    
    print(f"Total trades: {total_trades}")
    
    # Calculate win/loss statistics
    if 'profit_points' in results_df.columns:
        winning_trades = results_df[results_df['profit_points'] > 0]
        losing_trades = results_df[results_df['profit_points'] < 0]
        breakeven_trades = results_df[results_df['profit_points'] == 0]
        
        win_rate = (len(winning_trades) / total_trades * 100) if total_trades > 0 else 0
        total_profit = results_df['profit_points'].sum()
        avg_profit = results_df['profit_points'].mean()
        avg_win = winning_trades['profit_points'].mean() if len(winning_trades) > 0 else 0
        avg_loss = losing_trades['profit_points'].mean() if len(losing_trades) > 0 else 0
        
        print(f"\nWin/Loss Statistics:")
        print(f"  Winning trades: {len(winning_trades)} ({win_rate:.2f}%)")
        print(f"  Losing trades: {len(losing_trades)}")
        print(f"  Breakeven trades: {len(breakeven_trades)}")
        print(f"  Total profit points: {total_profit:.2f}")
        print(f"  Average profit per trade: {avg_profit:.2f}")
        print(f"  Average win: {avg_win:.2f}")
        print(f"  Average loss: {avg_loss:.2f}")
    
    # Exit reason breakdown
    if 'exit_reason' in results_df.columns:
        print(f"\nExit Reason Breakdown:")
        exit_counts = results_df['exit_reason'].value_counts()
        for reason, count in exit_counts.items():
            pct = (count / total_trades * 100) if total_trades > 0 else 0
            print(f"  {reason}: {count} trades ({pct:.2f}%)")
    
    # Count by signal type
    if 'signal' in results_df.columns:
        print(f"\nSignal Breakdown:")
        signal_counts = results_df['signal'].value_counts()
        for signal_type, count in signal_counts.items():
            signal_trades = results_df[results_df['signal'] == signal_type]
            if 'profit_points' in signal_trades.columns:
                signal_profit = signal_trades['profit_points'].sum()
                signal_wins = len(signal_trades[signal_trades['profit_points'] > 0])
                signal_win_rate = (signal_wins / count * 100) if count > 0 else 0
                print(f"  {signal_type}: {count} trades, {signal_wins} wins ({signal_win_rate:.2f}%), Profit: {signal_profit:.2f}")
            else:
                print(f"  {signal_type}: {count} trades")
    
    # Step 6: Export to Excel
    print("\n📊 Step 6: Exporting results to Excel...")
    output_file = "backtest_trade_results_full.xlsx"
    engine.export(results_df, output_file=output_file)
    
    print("\n✅ Backtest completed successfully!")
    print(f"   Results saved to: {output_file}")


if __name__ == "__main__":
    run_backtest_example()
