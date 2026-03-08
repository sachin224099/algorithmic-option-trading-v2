from typing import Dict, List


class SignalScorer:

    def __init__(self, config):
        self.config = config
        self.weights = config.get_scoring_weights()

    # -------------------------------------------------
    # Volume Score
    # -------------------------------------------------
    def score_volume(self, volume_spike_ratio: float) -> float:

        w = self.weights["volume"]

        if volume_spike_ratio >= 5:
            return w

        elif volume_spike_ratio >= 3:
            return w * 0.8

        elif volume_spike_ratio >= 2:
            return w * 0.6

        elif volume_spike_ratio >= 1.5:
            return w * 0.4

        return w * 0.2

    # -------------------------------------------------
    # OI Structure Score
    # -------------------------------------------------
    def score_oi_structure(self, oi_structure: str, signal: str) -> float:

        w = self.weights["oi_structure"]

        bullish = ["LONG_BUILDUP", "SHORT_COVERING"]
        bearish = ["SHORT_BUILDUP", "LONG_UNWINDING"]

        if signal == "CE" and oi_structure in bullish:
            return w

        if signal == "PE" and oi_structure in bearish:
            return w

        if oi_structure == "NEUTRAL":
            return w * 0.3

        return w * 0.5

    # -------------------------------------------------
    # Price Momentum
    # -------------------------------------------------
    def score_price_momentum(self, price_change_pct: float) -> float:

        w = self.weights["price_momentum"]
        pct = abs(price_change_pct)

        if pct >= 0.8:
            return w

        elif pct >= 0.5:
            return w * 0.8

        elif pct >= 0.3:
            return w * 0.6

        elif pct >= 0.15:
            return w * 0.4

        return w * 0.2

    # -------------------------------------------------
    # ATR Expansion Score
    # -------------------------------------------------
    def score_atr(self, signal: Dict) -> float:

        w = self.weights["atr"]

        if signal.get("atr_expanding_3_candles"):
            return w

        if signal.get("atr_expanding_2_candles"):
            return w * 0.6

        return w * 0.2

    # -------------------------------------------------
    # Compression Score
    # -------------------------------------------------
    def score_compression(self, signal: Dict) -> float:

        w = self.weights["compression"]

        atr_comp = signal.get("is_compression", False)
        bb_comp = signal.get("is_bollinger_compression", False)

        if atr_comp and bb_comp:
            return w

        if atr_comp or bb_comp:
            return w * 0.6

        return 0

    # -------------------------------------------------
    # Liquidity Sweep Score
    # -------------------------------------------------
    def score_liquidity_sweep(self, signal: Dict) -> float:

        w = self.weights["liquidity_sweep"]

        if signal.get("is_liquidity_sweep_breakout"):
            return w

        return 0

    # -------------------------------------------------
    # VWAP Direction Score
    # -------------------------------------------------
    def score_vwap(self, signal: Dict) -> float:

        w = self.weights["vwap"]

        duration = signal.get("above_vwap_duration_min", 0)
        slope_rising = signal.get("is_vwap_slope_rising", False)
        signal_type = signal.get("signal")

        if signal_type == "CE":

            if duration > 60 and slope_rising:
                return w

            if duration > 30:
                return w * 0.7

        else:  # PE

            if duration == 0 and not slope_rising:
                return w

            if duration < 10:
                return w * 0.7

        return w * 0.2

    # -------------------------------------------------
    # OI Trend Score
    # -------------------------------------------------
    def score_oi_trend(self, oi_trend: str) -> float:

        w = self.weights["oi_trend"]

        if oi_trend == "OI_RISING":
            return w

        if oi_trend == "OI_FLAT":
            return w * 0.6

        if oi_trend == "OI_FALLING":
            return w * 0.3

        return 0

    # -------------------------------------------------
    # Range Expansion Score
    # -------------------------------------------------
    def score_range_expansion(self, signal: Dict) -> float:

        w = self.weights["range_expansion"]
        ratio = signal.get("range_ratio", 0)

        if ratio > 0.012:
            return w

        elif ratio > 0.009:
            return w * 0.7

        elif ratio > 0.006:
            return w * 0.4

        return 0

    # -------------------------------------------------
    # Option Volume Score
    # -------------------------------------------------
    def score_option_volume(self, signal: Dict) -> float:

        w = self.weights["option_volume_spike_ratio"]

        volume = signal["options"]["volume_spike"]

        if volume >= 5:
            score = w

        elif volume >= 3:
            score = w * 0.8

        elif volume >= 2:
            score = w * 0.6

        else:
            score = w * 0.3

        return score

    # -------------------------------------------------
    # Final Score
    # -------------------------------------------------
    def calculate_signal_score(self, signal: Dict) -> float:

        score = 0

        score += self.score_volume(signal["volume_spike_ratio"])

        score += self.score_oi_structure(
            signal["oi_structure"],
            signal["signal"]
        )

        score += self.score_price_momentum(signal["price_change_pct"])

        score += self.score_atr(signal)

        score += self.score_compression(signal)

        score += self.score_liquidity_sweep(signal)

        score += self.score_vwap(signal)

        score += self.score_oi_trend(signal["oi_trend"])

        score += self.score_range_expansion(signal)

        score += self.score_option_volume(signal)
        max_possible = sum(self.weights.values())

        normalized_score = (score / max_possible) * 100

        return round(normalized_score, 2)

    # -------------------------------------------------
    # Score Category
    # -------------------------------------------------
    def score_category(self, score: float) -> str:

        if score >= 85:
            return "STRONG"

        if score >= 70:
            return "GOOD"

        if score >= 55:
            return "MODERATE"

        return "WEAK"

    # -------------------------------------------------
    # Rank Signals
    # -------------------------------------------------
    def rank_signals(self, signals: List[Dict]) -> List[Dict]:

        for signal in signals:

            score = self.calculate_signal_score(signal)

            signal["score"] = score
            signal["score_category"] = self.score_category(score)

        signals.sort(key=lambda x: x["score"], reverse=True)

        return signals