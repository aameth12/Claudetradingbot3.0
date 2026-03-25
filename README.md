# AI Multi-Agent Day Trading Bot v2.0

Fully async Python trading bot using multiple AI agents, Interactive Brokers, and local Ollama for zero-cost AI signals.

## Prerequisites

1. **Python 3.11+** — [python.org](https://python.org) (tick "Add to PATH" on Windows)
2. **Ollama** — [ollama.com](https://ollama.com) — after install run: `ollama pull mistral`
3. **IB Gateway** — [Interactive Brokers](https://www.interactivebrokers.com) — paper trading, port 7497, API enabled
4. **Telegram Bot** — token from [@BotFather](https://t.me/BotFather), chat ID from [@userinfobot](https://t.me/userinfobot)

## Quick Start (Windows)

1. Double-click `setup.bat` — creates venv, installs deps, sets up `.env`
2. Double-click `run.bat` — starts the bot

## Manual Setup

```bash
python -m venv venv
venv\Scripts\activate       # Windows
# source venv/bin/activate  # Linux/Mac

pip install -r requirements.txt
```

Copy `.env.example` to `.env` and fill in your Telegram credentials:

```bash
cp .env.example .env
```

## Before Running

1. Start **Ollama** — make sure `mistral` model is pulled
2. Start **IB Gateway** — login with paper trading account, ensure API is enabled on port 7497
3. Start the bot:

```bash
python main.py
```

## Architecture

7 agents communicate exclusively through the Orchestrator message bus:

| Agent | Role |
|-------|------|
| IBKRClientAgent | IB Gateway connection, market data, bracket orders, fill detection |
| DataAgent | OHLCV cache (1m/5m/15m), bar resampling |
| StrategyAgent | Technical indicators (pandas-ta) + Ollama AI signals |
| RiskAgent | 8 hard rules: daily loss halt, position sizing, R:R, direction filter |
| ExecutionAgent | Position lifecycle, P&L tracking, auto-close on EOD |
| TelegramAgent | 15 commands + proactive alerts |
| PerformanceAgent | Metrics, daily goals, confidence auto-tuning, symbol cool-downs |

## Telegram Commands

| Command | Description |
|---------|-------------|
| `/status` | Full live dashboard |
| `/positions` | Open positions detail |
| `/pnl` | Today + all-time P&L |
| `/risk` | Risk exposure dashboard |
| `/history` | Last 20 closed trades |
| `/performance` | 7-day daily table |
| `/watchlist` | Current symbols |
| `/add SYMBOL` | Add to watchlist |
| `/remove SYMBOL` | Remove from watchlist |
| `/data SYMBOL` | Live price + trigger scan |
| `/set key value` | Tune settings live |
| `/stop` | Pause trading |
| `/resume` | Resume trading |
| `/kill` | Emergency shutdown |

## Configuration

Edit `config.yaml` to tune strategy, risk, and performance settings. All settings can also be changed live via Telegram `/set` commands.

## Important Notes

- **Paper trading only** by default (port 7497). Change to 7496 for live.
- US stocks only, regular hours 09:30-16:00 ET.
- PDT rule: $25k minimum for >3 day trades / 5 days in margin accounts (paper exempt).
