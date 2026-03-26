"""DataAgent — Real-time OHLCV cache for live bar updates from IBKR.

Scanning data is now sourced directly from yfinance in StrategyAgent.
This agent handles only real-time bar updates (new_bar events) and
responds to data_request messages for live price display (/data command).
"""

import asyncio
from collections import defaultdict

import pandas as pd
from loguru import logger

from agents.base_agent import BaseAgent, Message


class DataAgent(BaseAgent):
    def __init__(self, config: dict, watchlist: list[str], orchestrator=None):
        super().__init__("DataAgent", orchestrator)
        self.watchlist = list(watchlist)
        self.max_bars = 500
        self.cache_1m: dict[str, pd.DataFrame] = {}
        self.cache_5m: dict[str, pd.DataFrame] = {}
        self.cache_15m: dict[str, pd.DataFrame] = {}
        self._ready: set[str] = set()

    async def run(self):
        """Process inbox — respond to data_request and new_bar messages."""
        while self._running:
            await self._process_inbox()
            await asyncio.sleep(0.1)

    def _bars_to_df(self, bars) -> pd.DataFrame:
        """Convert ib_insync BarData list to a pandas DataFrame."""
        if not bars:
            return pd.DataFrame()

        data = []
        for bar in bars:
            data.append({
                "datetime": bar.date if hasattr(bar, "date") else str(bar),
                "open": bar.open,
                "high": bar.high,
                "low": bar.low,
                "close": bar.close,
                "volume": bar.volume,
            })

        df = pd.DataFrame(data)
        if not df.empty:
            df["datetime"] = pd.to_datetime(df["datetime"])
            df.set_index("datetime", inplace=True)
            df.sort_index(inplace=True)
        return df

    def _resample(self, df_1m: pd.DataFrame, timeframe: str) -> pd.DataFrame:
        """Resample 1m bars to 5m or 15m."""
        if df_1m.empty:
            return pd.DataFrame()

        rule = "5min" if timeframe == "5m" else "15min"
        resampled = df_1m.resample(rule).agg({
            "open": "first",
            "high": "max",
            "low": "min",
            "close": "last",
            "volume": "sum",
        }).dropna()
        return resampled

    def _update_cache(self, symbol: str, new_bars_df: pd.DataFrame):
        """Update the 1m cache and resample to 5m/15m."""
        if new_bars_df.empty:
            return

        if symbol in self.cache_1m and not self.cache_1m[symbol].empty:
            self.cache_1m[symbol] = pd.concat([self.cache_1m[symbol], new_bars_df])
            self.cache_1m[symbol] = self.cache_1m[symbol][~self.cache_1m[symbol].index.duplicated(keep="last")]
            self.cache_1m[symbol] = self.cache_1m[symbol].tail(self.max_bars)
        else:
            self.cache_1m[symbol] = new_bars_df.tail(self.max_bars)

        self.cache_5m[symbol] = self._resample(self.cache_1m[symbol], "5m")
        self.cache_15m[symbol] = self._resample(self.cache_1m[symbol], "15m")
        self._ready.add(symbol)

    def get_bars(self, symbol: str, timeframe: str = "1m", n_bars: int = 100) -> pd.DataFrame:
        """Get cached bars for a symbol and timeframe."""
        cache_map = {"1m": self.cache_1m, "5m": self.cache_5m, "15m": self.cache_15m}
        cache = cache_map.get(timeframe, self.cache_1m)
        df = cache.get(symbol, pd.DataFrame())
        if df.empty:
            return df
        return df.tail(n_bars).copy()

    def is_ready(self, symbol: str) -> bool:
        return symbol in self._ready

    async def handle_message(self, message: Message):
        msg_type = message.type
        payload = message.payload

        if msg_type == "data_request":
            symbol = payload.get("symbol")
            timeframe = payload.get("timeframe", "1m")
            n_bars = payload.get("n_bars", 100)
            df = self.get_bars(symbol, timeframe, n_bars)
            self.send(message.sender, "data_response", {
                "symbol": symbol,
                "timeframe": timeframe,
                "bars": df.to_dict() if not df.empty else {},
                "count": len(df),
                "request_id": payload.get("request_id"),
            })

        elif msg_type == "new_bar":
            symbol = payload.get("symbol")
            bar_data = payload.get("bar")
            if bar_data:
                df = pd.DataFrame([bar_data])
                if "datetime" in df.columns:
                    df["datetime"] = pd.to_datetime(df["datetime"])
                    df.set_index("datetime", inplace=True)
                self._update_cache(symbol, df)

        elif msg_type == "watchlist_update":
            self.watchlist = payload.get("watchlist", self.watchlist)
