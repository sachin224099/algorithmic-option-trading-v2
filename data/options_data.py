from data.zerodha_client import ZerodhaClient
import pandas as pd


def populate_options_ltp(options, zerodha_client: ZerodhaClient):
    """
    Populate last_price for options.
    
    Parameters:
        options: Can be:
            - pandas Series (single option row from DataFrame)
            - dict (single option)
            - list of dicts (multiple options)
            - None
        zerodha_client: ZerodhaClient instance
    
    Returns:
        Same type as input, with last_price populated
    """
    # Handle None case
    if options is None:
        return None
    
    # Handle pandas Series (single option row)
    if isinstance(options, pd.Series):
        options_dict = options.to_dict()
        tradingsymbol = options_dict.get("tradingsymbol")
        if not tradingsymbol:
            return options_dict
        
        # Fetch LTP for this single option
        response = zerodha_client.get_stock_spot_prices([tradingsymbol], exchange="NFO")
        if tradingsymbol in response:
            options_dict["last_price"] = response[tradingsymbol]
        else:
            options_dict["last_price"] = 0.0
        
        return options_dict
    
    # Handle single dict
    if isinstance(options, dict):
        tradingsymbol = options.get("tradingsymbol")
        if not tradingsymbol:
            return options
        
        response = zerodha_client.get_stock_spot_prices([tradingsymbol], exchange="NFO")
        if tradingsymbol in response:
            options["last_price"] = response[tradingsymbol]
        else:
            options["last_price"] = 0.0
        
        return options
    
    # Handle list of dicts
    if isinstance(options, list):
        options_symbols = [option.get("tradingsymbol") for option in options if option.get("tradingsymbol")]
        if not options_symbols:
            return options
        
        response = zerodha_client.get_stock_spot_prices(options_symbols, exchange="NFO")
        for option in options:
            tradingsymbol = option.get("tradingsymbol")
            if tradingsymbol and tradingsymbol in response:
                option["last_price"] = response[tradingsymbol]
            else:
                option["last_price"] = 0.0
        
        return options
    
    # Fallback: return as-is
    return options