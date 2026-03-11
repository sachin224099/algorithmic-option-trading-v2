from core.config_loader import Config

def calculate_directional_atr_target_futures(
        breakout_price,
        atr,
        signal,
        multiplier=1.5
):
    """
    Calculate ATR target based on trade direction.
    """

    if signal == "CE":
        return round(breakout_price + atr * multiplier, 2)

    if signal == "PE":
        return round(breakout_price - atr * multiplier, 2)

    return None



def calculate_atr_stop_loss_futures(
        breakout_price: float,
        atr: float,
        signal: str,
        multiplier: float = 0.8
):
    """
    Calculate ATR-based stop loss level on futures.
    """

    if signal == "CE":
        sl = breakout_price - (atr * multiplier)

    elif signal == "PE":
        sl = breakout_price + (atr * multiplier)

    else:
        return None

    return round(sl, 2)    


def calculate_target_stop_loss_futures(
        breakout_price: float,
        atr: float,
        signal: str,
        config: Config
):
    """
    Calculate target and stop loss levels on futures.
    """

    target = calculate_directional_atr_target_futures(breakout_price, atr, signal, config.get_futures_target_atr_multiplier())
    stop_loss = calculate_atr_stop_loss_futures(breakout_price, atr, signal, config.get_futures_stop_loss_atr_multiplier())
    return {
        "target": target,
        "stop_loss": stop_loss
    }


def calculate_target_stop_loss_fixed_percentage(
        entry_price: float,
        signal: str,
        target_percent: float = 2.0,
        stop_loss_percent: float = 1.0
):
    """
    Calculate target and stop loss levels based on fixed percentage.
    
    Args:
        entry_price: Entry price for the position
        signal: Signal type ('PE' or 'CE')
        target_percent: Target percentage (default: 2.0%)
        stop_loss_percent: Stop loss percentage (default: 1.0%)
    
    Returns:
        Dictionary with 'target' and 'stop_loss' keys
    
    For PE (Put):
        - Target: entry_price * (1 - target_percent/100) = 2% below entry
        - Stop Loss: entry_price * (1 + stop_loss_percent/100) = 1% above entry
    
    For CE (Call):
        - Target: entry_price * (1 + target_percent/100) = 2% above entry
        - Stop Loss: entry_price * (1 - stop_loss_percent/100) = 1% below entry
    """
    
    if signal == "PE":
        # PE: Profit when price goes down, loss when price goes up
        target = entry_price * (1 - target_percent / 100)
        stop_loss = entry_price * (1 + stop_loss_percent / 100)
    elif signal == "CE":
        # CE: Profit when price goes up, loss when price goes down
        target = entry_price * (1 + target_percent / 100)
        stop_loss = entry_price * (1 - stop_loss_percent / 100)
    else:
        return None
    
    return {
        "target": round(target, 2),
        "stop_loss": round(stop_loss, 2)
    }

def calculate_target_stop_loss_options(
        futures_target_stop_loss: dict,
        option_entry_price: float,
        futures_entry_price: float,
        config: Config
):
    """
    Calculate option target and stop loss using futures ATR levels and delta.
    """

    futures_target = futures_target_stop_loss["target"]
    futures_stop_loss = futures_target_stop_loss["stop_loss"]

    delta = config.get_options_default_delta()

    # Futures move
    target_move_futures = abs(futures_target - futures_entry_price)
    stop_loss_move_futures = abs(futures_entry_price - futures_stop_loss)

    # Convert futures move to option move
    option_target_move = target_move_futures * abs(delta)
    option_sl_move = stop_loss_move_futures * abs(delta)

    target = option_entry_price + option_target_move
    stop_loss = option_entry_price - option_sl_move

    return {
        "target": round(target, 2),
        "stop_loss": round(stop_loss, 2)
    }