# algorithmic-option-trading-v2

## Algo Trading Engine – Sudden Move Detection System

### 🎯 Objective

Build a modular, extendable intraday trading engine that:

- Scans 200 F&O stocks
- Detects high-probability sudden moves
- Uses 15-min candles for structure
- Uses 5-min candles for entry timing
- Trades option buying (20% SL / 40% target)
- Runs on a single machine continuously

### 🏗️ Architecture Overview

System follows clean layered design:

```
Data Layer
    ↓
Indicator Layer
    ↓
Signal Layer
    ↓
Scoring Layer
    ↓
Engine Layer (Orchestration)
    ↓
Execution Layer
```

Each layer has single responsibility.

### 📂 Project Structure

```
algo_trading/
│
├── config/
│   ├── config.yaml
│   └── symbols.yaml
│
├── core/
│   └── config_loader.py
│
├── data/
│   ├── zerodha_client.py
│   ├── futures_data.py
│   └── options_data.py
│
├── indicators/
│   ├── breakout.py
│   ├── atr.py
│   ├── oi_analysis.py
│   └── iv_analysis.py
│
├── signals/
│   ├── fifteen_min_signal.py
│   └── five_min_entry.py
│
├── scoring/
│   └── scoring_model.py
│
├── execution/
│   ├── risk_manager.py
│   └── order_manager.py
│
├── engine/
│   ├── market_scanner.py
│   ├── entry_monitor.py
│   └── orchestrator.py
│
└── main.py
```

### ⚙️ Configuration (YAML Based)

All strategy parameters must be configurable.

**config/config.yaml**

```yaml
environment: paper

timeframes:
  structure_tf: 15m
  entry_tf: 5m

scanner:
  min_score: 8
  max_symbols_monitor: 5

breakout:
  lookback_candles: 12
  volume_multiplier: 1.5

futures:
  min_oi_change_pct: 2

options:
  strikes_around_atm: 3
  min_oi_change_pct: 3
  min_iv_change_pct: 3
  max_iv_percentile: 85
  volume_multiplier: 2

risk:
  sl_percent: 20
  target_percent: 40
  max_open_positions: 3

scoring_weights:
  breakout: 3
  futures_oi: 2
  option_oi: 2
  iv: 2
  volume: 1
  gamma: 1
```

### 🧠 Trading Logic Design

#### 1️⃣ 15-Min Structure Scan (Runs Every 15 Minutes)

**Data Required:**

- Futures 15-min candles
- Futures OI
- Futures volume

**Conditions:**

- Breakout of last 12-candle high/low
- Volume > 1.5× average
- Futures OI increasing
- ATR expansion

**Output:**

- Shortlist top candidates by score.
- Reduce 200 → ~30 → Top 5

#### 2️⃣ Option Aggression Filter

Only applied to shortlisted stocks.

**Data Required:**

- ATM ± 3 strikes
- OI change
- Volume spike
- IV change

**Bullish:**

- Put OI increasing OR Call OI decreasing
- ATM volume > 2×
- IV rising (> 3%)
- IV percentile < 85

#### 3️⃣ Scoring Model

Weighted scoring:

| Component | Weight |
|-----------|--------|
| Breakout | 3 |
| Futures OI | 2 |
| Option OI shift | 2 |
| IV expansion | 2 |
| Volume spike | 1 |
| Gamma proximity | 1 |

**Trigger:**

- Score ≥ 8 → Eligible for entry monitoring

#### 4️⃣ 5-Min Entry Monitoring

Runs every 5 minutes for shortlisted stocks.

**Entry Conditions:**

- Pullback holds above breakout level
- Higher low → Higher high pattern
- 5-min volume expansion
- Option not already spiked > 20%

Entry triggered after confirmation.

### 🔁 Runtime Execution Model (Single Machine)

Continuous loop scheduler:

```
Start program
Load config
Initialize Zerodha session

Loop:
    If new 15-min candle:
        Run market scan

    If new 5-min candle:
        Monitor shortlisted symbols

    Sleep 5 seconds
```

### 💾 State Management

Optional but recommended:

```
state/
   open_positions.json
   shortlisted_symbols.json
```

Allows recovery if system crashes.

### ⚡ Performance Strategy (200 Stocks)

- Fetch futures for all 200
- Fetch options only for shortlisted (~30)
- Cache data per cycle
- Avoid unnecessary API calls
- Never fetch full option chain for all 200

### 🎯 Risk Management

- SL = 20%
- Target = 40%
- Max open positions = 3
- Avoid entries if IV already expanded > 8%
- Avoid first 15 minutes of market

### 📌 Execution Rules

- Structure confirmation on 15-min close
- Entry precision on 5-min
- No entry during extreme IV
- No entry after option already moved 25%

### 🧩 Design Principles Followed

- Single Responsibility Principle
- Strategy isolation
- Config-driven parameters
- Broker abstraction
- Extendable scoring engine
- Easy ML integration later
