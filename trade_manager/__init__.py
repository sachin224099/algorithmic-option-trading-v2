"""
Trade Manager Module

Manages trade lifecycle from signal detection to position monitoring:
- Monitors final_signals directory for new signals
- Records orders in open_orders table
- Monitors entry conditions via websocket
- Tracks positions and calculates P&L
"""
