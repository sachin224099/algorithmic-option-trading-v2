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