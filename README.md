# CelebCrypto: Multi-Modal Benchmark for Celebrity on Cryptocurrency Markets with Neuro-Symbolic Residual Modeling

**[Anonymous Repository for Peer Review]**

This repository contains the official implementation, dataset, and benchmark code for the paper **"CelebCrypto: Multi-Modal Benchmark for Celebrity on Cryptocurrency Markets with Neuro-Symbolic Residual Modeling"**.

## Introduction

**CelebCrypto** is a large-scale multi-modal dataset and benchmark designed to analyze and forecast how high-impact celebrity events (e.g., social signals from influential figures) influence cryptocurrency markets.

Traditional time-series models often fail to capture systematic prediction bias introduced by sudden public signals. To address this, we introduce the **Neuro-Symbolic Residual Correction (NSRC)** framework. NSRC explicitly decouples neural trend prediction from symbolic event-driven bias, injecting horizon-aware statistical priors through leakage-free calibration.

## Repository Structure

The repository is organized to align with the four modalities described in the paper: **Celebrity Entity Set**, **Social Signals**, **Contextual Events**, and **Financial Vision**.

```text
.
├── dataset/                     # Multi-modal data storage
│   ├── events/                  # Canonicalized contextual events (News/Policy)
│   ├── kline_photo/             # Financial Vision: 24h candlestick chart images
│   ├── ohlcv/                   # Market Layer: High-frequency price/volume data (Binance)
│   ├── tweet_core/              # Social Signals: Tweets from core influencers (e.g., Elon Musk)
│   ├── tweet_related/           # Social Signals: Tweets from related community accounts
│   └── model_prediction/        # Cached baseline model predictions for residual calculation
│
├── src/
│   ├── get_data/                # Data Acquisition & Annotation Pipeline
│   │   ├── get_events.py        # Scrapes and processes news events (CoinDesk/Policy)
│   │   ├── get_ohlcv.py         # Fetches market data via Binance API
│   │   ├── get_tweet.py         # Scrapes social signals (Apify/Twitter)
│   │   ├── process_event.py     # Cleans and aligns event timestamps
│   │   ├── process_tweet_core.py    # Filters/Processes core influencer tweets
│   │   ├── process_tweet_related.py # Filters/Processes related tweets
│   │   ├── gen_reason.py        # Generates Chain-of-Thought (CoT) reasoning for impact
│   │   └── ana_match_rates.py   # Analyzes alignment rates between modalities
│   │
│   └── method/                  # NSRC Framework Implementation
│       ├── gemini.py            # LLM Interface for quantitative pulse & logic extraction (Step 1)
│       ├── train.py             # Main script: Hourly Cross-Validation & Gain Optimization (Step 2-3)
│       ├── impact_features_step1.jsonl # Intermediate processed impact features
│       └── policy_impact_curve.png     # Visualization of impact curves
```
## Getting Started

### Prerequisites

* **Python 3.8+**
* **Dependencies**:
  ```bash
  pip install pandas numpy scikit-learn openai tqdm matplotlib joblib
  ```
  ### 1. Data Preparation (`src/get_data/`)

The scripts in `src/get_data/` implement the Human-in-the-Loop (HITL) pipeline described in the paper.

* **Acquisition**: Use `get_tweet.py` and `get_events.py` to collect raw data.
* **Processing**: Use `process_event.py` to canonicalize events and align them to the nearest past hourly forecast origin.
* **Reasoning**: `gen_reason.py` utilizes LLMs to generate semantic annotations (Bullish/Bearish/Consolidation).

### 2. Feature Extraction (`src/method/gemini.py`)

This script extracts the "Quantitative Pulse" (Surprise/Intensity) and "Qualitative Logic" from raw events using the System Prompt defined in the paper.

* **Input**: Raw event JSONs (e.g., `gold_standard_events.json`).
* **Output**: `impact_features_step1.jsonl` containing structured event meta-data and surprise intensity.
* **Configuration**: Update `API_KEYS` and `INPUT_FILE` paths in `gemini.py` before running.

### 3. Neuro-Symbolic Training (`src/method/train.py`)

This is the core implementation of the NSRC framework.

* **Function**: It performs the leakage-free calibration by finding optimal horizon-specific gain vectors ($\lambda$) using rolling-window cross-validation.
* **Execution**:
    ```bash
    python src/method/train.py
    ```
* **Output**:
    * Prints MAE/MAPE/MSE metrics for t+1 to t+24 horizons.
    * Saves the optimized `v7_hourly_robust_stats.pkl` model file.
    * Generates validation reports showing "Circuit Breaker" status if the symbolic prior is rejected ($\lambda \approx 0$).

## Dataset Statistics

The **CelebCrypto** benchmark covers the period from Jan 1, 2025, to Dec 30, 2025.

| Modality | Description | Count |
| :--- | :--- | :--- |
| **Celebrity Entity** | Identity and reputation profiles of public figures | 3,459 |
| **Social Signals** | Tweets with engagement dynamics (Core & Related) | 21,389 |
| **Contextual Events** | News and regulatory updates | 5,478 |
| **Assets** | Aligned market data | BTC, ETH, SOL, DOGE, TRUMP |

## Experimental Results

As reported in the paper, NSRC significantly reduces forecasting error under high-volatility celebrity events:

* **Accuracy**: Achieves up to **40.28%** reduction in MAE on DOGE compared to strong baselines like PatchTST and GPT4TS.
* **Trading**: In backtesting (Jan-May 2025), the strategy yielded a **+37.88%** cumulative return with a Sharpe Ratio of 0.89.

## License

This project is licensed under the MIT License.

## Disclaimer

This code and dataset are for research purposes only. They are not intended as financial advice. The provided dataset and models should be used with caution in real-world trading scenarios.