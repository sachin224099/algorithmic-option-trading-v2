import pandas as pd
from core.config_loader import Config

def filter_strikes_near_spot(df, symbol, option_type, spot, atr):

    options = df[
        (df["name"] == symbol) &
        (df["instrument_type"] == option_type) &
        (df["expiry"] == Config().get_expiry_date())
    ].copy()

    #print(f"Options: {options}")
    options["distance"] = abs(options["strike"] - spot)

    zone_limit = 1.2 * atr
    options = options[options["distance"] <= zone_limit]

    if options.empty:
        return None

    # Sort closest to spot
    options = options.sort_values("distance")

    return options.iloc[0]   # Best strike