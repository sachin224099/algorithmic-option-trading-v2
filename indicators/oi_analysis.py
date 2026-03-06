def calculate_option_oi_change(df):

    if df is None or len(df) < 2:
        return None

    latest_oi = df.iloc[-1]["oi"]
    prev_oi = df.iloc[-2]["oi"]

    if prev_oi == 0:
        return None

    oi_change_pct = ((latest_oi - prev_oi) / prev_oi) * 100

    return round(oi_change_pct, 2)