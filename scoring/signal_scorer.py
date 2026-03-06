from typing import Dict, List


class SignalScorer:

    def __init__(self, config):
        self.config = config
        self.weights = config.get_scoring_weights()

    # -----------------------------
    # Volume Score
    # -----------------------------
    def score_volume(self, volume_spike_ratio: float) -> float:
        #print("starting volume score")

        if volume_spike_ratio >= 5:
            return self.weights["volume"]

        elif volume_spike_ratio >= 3:
            return self.weights["volume"] * 0.8

        elif volume_spike_ratio >= 2:
            return self.weights["volume"] * 0.6

        elif volume_spike_ratio >= 1.5:
            return self.weights["volume"] * 0.4

        return self.weights["volume"] * 0.2

    # -----------------------------
    # OI Structure Score
    # -----------------------------
    def score_oi_structure(self, oi_structure: str, signal: str) -> float:

        bullish_structures = ["LONG_BUILDUP", "SHORT_COVERING"]
        bearish_structures = ["SHORT_BUILDUP", "LONG_UNWINDING"]

        if signal == "CE" and oi_structure in bullish_structures:
            return self.weights["oi_structure"]

        if signal == "PE" and oi_structure in bearish_structures:
            return self.weights["oi_structure"]

        if oi_structure == "NEUTRAL":
            return self.weights["oi_structure"] * 0.3

        return self.weights["oi_structure"] * 0.5

    # -----------------------------
    # Price Momentum Score
    # -----------------------------
    def score_price_momentum(self, price_change_pct: float) -> float:

        pct = abs(price_change_pct)

        if pct >= 0.8:
            return self.weights["price_momentum"]

        elif pct >= 0.5:
            return self.weights["price_momentum"] * 0.8

        elif pct >= 0.3:
            return self.weights["price_momentum"] * 0.6

        elif pct >= 0.15:
            return self.weights["price_momentum"] * 0.4

        return self.weights["price_momentum"] * 0.2

    # -----------------------------
    # ATR Expansion Score
    # -----------------------------
    def score_atr(self, signal: Dict) -> float:

        if signal.get("atr_expanding_3_candles"):
            return self.weights["atr"]

        if signal.get("atr_expanding_2_candles"):
            return self.weights["atr"] * 0.7

        return self.weights["atr"] * 0.2

    # -----------------------------
    # VWAP Score
    # -----------------------------
    def score_vwap(self, signal: Dict) -> float:

        duration = signal.get("above_vwap_duration_min", 0)

        if duration > 60:
            return self.weights["vwap"]

        elif duration > 30:
            return self.weights["vwap"] * 0.7

        elif duration > 10:
            return self.weights["vwap"] * 0.5

        return self.weights["vwap"] * 0.2

    # -----------------------------
    # OI Trend Score
    # -----------------------------
    def score_oi_trend(self, oi_trend: str) -> float:

        if oi_trend == "OI_RISING":
            return self.weights["oi_trend"]

        if oi_trend == "OI_FLAT":
            return self.weights["oi_trend"] * 0.6

        if oi_trend == "OI_FALLING":
            return self.weights["oi_trend"] * 0.3

        return 0

    # -----------------------------
    # Final Score Calculation
    # -----------------------------
    def calculate_signal_score(self, signal: Dict) -> float:

        score = 0

        score += self.score_volume(signal["volume_spike_ratio"])

        score += self.score_oi_structure(
            signal["oi_structure"],
            signal["signal"]
        )

        score += self.score_price_momentum(signal["price_change_pct"])

        score += self.score_atr(signal)

        score += self.score_vwap(signal)

        score += self.score_oi_trend(signal["oi_trend"])

        score += self.score_option_volume(signal["options"]["volume_spike"])

        return round(score, 2)

    # -----------------------------
    # Score Category
    # -----------------------------
    def score_category(self, score: float) -> str:

        if score >= 80:
            return "STRONG"

        if score >= 65:
            return "GOOD"

        if score >= 50:
            return "MODERATE"

        return "WEAK"

    # -----------------------------
    # Rank Signals
    # -----------------------------
    def rank_signals(self, signals: List[Dict]) -> List[Dict]:

        for signal in signals:

            score = self.calculate_signal_score(signal)

            signal["score"] = score
            signal["score_category"] = self.score_category(score)

        signals.sort(key=lambda x: x["score"], reverse=True)

        return signals


    def score_option_volume(self, volume_spike_ratio: float) -> float:
        """
        Assign score based on option volume spike.
        """

        max_weight = self.weights["option_volume_spike_ratio"]

        if volume_spike_ratio >= 5:
            return max_weight

        elif volume_spike_ratio >= 3:
            return max_weight * 0.8

        elif volume_spike_ratio >= 2:
            return max_weight * 0.6

        elif volume_spike_ratio >= 1.5:
            return max_weight * 0.4

        return max_weight * 0.2    