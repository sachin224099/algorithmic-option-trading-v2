from exception.exception_handler import handle_exceptions

@handle_exceptions
def breakout_signal(df, lookback=12):
    if len(df) < lookback + 5:
        return None

    recent_high = df["high"].iloc[-lookback-1:-1].max()
    recent_low = df["low"].iloc[-lookback-1:-1].min()

    current_close = df["close"].iloc[-1]
    current_high = df["high"].iloc[-1]
    current_low = df["low"].iloc[-1]

    # 1️⃣ Donchian breakout
    breakout_up = current_close > recent_high
    breakout_down = current_close < recent_low

    # 2️⃣ Range expansion filter
    current_range = current_high - current_low
    avg_range = (df["high"] - df["low"]).iloc[-6:-1].mean()

    expansion = current_range > (1.2 * avg_range)

    # 3️⃣ Close strength filter
    if current_range > 0:
        close_position = (current_close - current_low) / current_range
    else:
        close_position = 0

    strong_close_up = close_position > 0.6
    strong_close_down = close_position < 0.4

    if breakout_up and expansion and strong_close_up:
        return "CE"

    if breakout_down and expansion and strong_close_down:
        return "PE"

    return None