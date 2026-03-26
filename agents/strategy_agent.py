"""StrategyAgent — Indicators + Ollama AI signals."""

import asyncio
import json
from datetime import datetime

import pandas as pd
from loguru import logger

from agents.base_agent import BaseAgent, Message
from utils.helpers import is_market_open

try:
    import pandas_ta as ta
except ImportError:
    ta = None

try:
    import ollama as ollama_client
except ImportError:
    ollama_client = None


class StrategyAgent(BaseAgent):
    def __init__(self, config: dict, watchlist: list[str], orchestrator=None):
        super().__init__("StrategyAgent", orchestrator)
        self.watchlist = list(watchlist)
        self.scan_interval = config.get("scan_interval_seconds", 60)
        self.confidence_threshold = config.get("confidence_threshold", 0.65)
        self.allow_shorts = config.get("allow_shorts", True)
        self.allow_longs = config.get("allow_longs", True)
        self.ollama_model = "mistral"
        self.ollama_timeout = 30
        self._paused = False
        self._pending_data: dict[str, dict] = {}
        self._cool_downs: dict[str, bool] = {}

    async def run(self):
        """Main loop: scan symbols periodically during market hours."""
        while self._running:
            await self._process_inbox()

            if is_market_open() and not self._paused:
                await self._scan_all_symbols()

            await asyncio.sleep(self.scan_interval)

    async def _scan_all_symbols(self):
        """Scan each watchlist symbol for trade signals."""
        logger.info("Starting strategy scan cycle")
        for symbol in self.watchlist:
            if self._paused:
                break

            # Check cool-down
            if self._cool_downs.get(symbol, False):
                logger.debug(f"Skipping {symbol} — cooled down")
                continue

            # Check cool-down with PerformanceAgent
            self.send("PerformanceAgent", "check_cool_down", {"symbol": symbol})

            await self._request_data_and_analyze(symbol)
            await asyncio.sleep(2)  # Rate limit between symbols

    async def _request_data_and_analyze(self, symbol: str):
        """Request data from DataAgent and run analysis."""
        for timeframe in ["1m", "5m", "15m"]:
            self.send("DataAgent", "data_request", {
                "symbol": symbol,
                "timeframe": timeframe,
                "n_bars": 100,
                "request_id": f"{symbol}_{timeframe}",
            })

    def _compute_indicators(self, df: pd.DataFrame) -> dict:
        """Compute all technical indicators using pandas-ta."""
        if df.empty or len(df) < 20 or ta is None:
            return {}

        indicators = {}
        try:
            # Trend indicators
            ema9 = ta.ema(df["close"], length=9)
            ema21 = ta.ema(df["close"], length=21)
            ema50 = ta.ema(df["close"], length=50)
            ema200 = ta.ema(df["close"], length=200)
            sma20 = ta.sma(df["close"], length=20)
            sma50 = ta.sma(df["close"], length=50)

            if ema9 is not None and len(ema9) > 0:
                indicators["ema_9"] = round(float(ema9.iloc[-1]), 4) if pd.notna(ema9.iloc[-1]) else None
            if ema21 is not None and len(ema21) > 0:
                indicators["ema_21"] = round(float(ema21.iloc[-1]), 4) if pd.notna(ema21.iloc[-1]) else None
            if ema50 is not None and len(ema50) > 0:
                indicators["ema_50"] = round(float(ema50.iloc[-1]), 4) if pd.notna(ema50.iloc[-1]) else None
            if ema200 is not None and len(ema200) > 0:
                indicators["ema_200"] = round(float(ema200.iloc[-1]), 4) if pd.notna(ema200.iloc[-1]) else None
            if sma20 is not None and len(sma20) > 0:
                indicators["sma_20"] = round(float(sma20.iloc[-1]), 4) if pd.notna(sma20.iloc[-1]) else None
            if sma50 is not None and len(sma50) > 0:
                indicators["sma_50"] = round(float(sma50.iloc[-1]), 4) if pd.notna(sma50.iloc[-1]) else None

            # MACD
            macd = ta.macd(df["close"], fast=12, slow=26, signal=9)
            if macd is not None and not macd.empty:
                for col in macd.columns:
                    val = macd[col].iloc[-1]
                    key = col.lower().replace("[", "").replace("]", "").replace(",", "_").replace(" ", "")
                    indicators[f"macd_{key}"] = round(float(val), 4) if pd.notna(val) else None

            # ADX
            adx = ta.adx(df["high"], df["low"], df["close"], length=14)
            if adx is not None and not adx.empty:
                for col in adx.columns:
                    val = adx[col].iloc[-1]
                    indicators[f"adx_{col.lower()}"] = round(float(val), 4) if pd.notna(val) else None

            # Momentum indicators
            rsi = ta.rsi(df["close"], length=14)
            if rsi is not None and len(rsi) > 0:
                indicators["rsi_14"] = round(float(rsi.iloc[-1]), 4) if pd.notna(rsi.iloc[-1]) else None

            stoch = ta.stoch(df["high"], df["low"], df["close"], k=14, d=3)
            if stoch is not None and not stoch.empty:
                for col in stoch.columns:
                    val = stoch[col].iloc[-1]
                    indicators[f"stoch_{col.lower()}"] = round(float(val), 4) if pd.notna(val) else None

            cci = ta.cci(df["high"], df["low"], df["close"], length=20)
            if cci is not None and len(cci) > 0:
                indicators["cci_20"] = round(float(cci.iloc[-1]), 4) if pd.notna(cci.iloc[-1]) else None

            willr = ta.willr(df["high"], df["low"], df["close"], length=14)
            if willr is not None and len(willr) > 0:
                indicators["williams_r"] = round(float(willr.iloc[-1]), 4) if pd.notna(willr.iloc[-1]) else None

            roc = ta.roc(df["close"], length=10)
            if roc is not None and len(roc) > 0:
                indicators["roc_10"] = round(float(roc.iloc[-1]), 4) if pd.notna(roc.iloc[-1]) else None

            # Volatility
            bbands = ta.bbands(df["close"], length=20, std=2)
            if bbands is not None and not bbands.empty:
                for col in bbands.columns:
                    val = bbands[col].iloc[-1]
                    indicators[f"bb_{col.lower()}"] = round(float(val), 4) if pd.notna(val) else None

            atr = ta.atr(df["high"], df["low"], df["close"], length=14)
            if atr is not None and len(atr) > 0:
                indicators["atr_14"] = round(float(atr.iloc[-1]), 4) if pd.notna(atr.iloc[-1]) else None

            # Volume
            obv = ta.obv(df["close"], df["volume"])
            if obv is not None and len(obv) > 0:
                indicators["obv"] = round(float(obv.iloc[-1]), 2) if pd.notna(obv.iloc[-1]) else None

            mfi = ta.mfi(df["high"], df["low"], df["close"], df["volume"], length=14)
            if mfi is not None and len(mfi) > 0:
                indicators["mfi_14"] = round(float(mfi.iloc[-1]), 4) if pd.notna(mfi.iloc[-1]) else None

            # Price info
            if len(df) > 0:
                last_close = float(df["close"].iloc[-1])
                day_high = float(df["high"].max())
                day_low = float(df["low"].min())
                indicators["last_close"] = round(last_close, 4)
                indicators["day_high"] = round(day_high, 4)
                indicators["day_low"] = round(day_low, 4)
                if day_high > 0:
                    indicators["dist_from_high_pct"] = round((last_close - day_high) / day_high * 100, 2)
                if day_low > 0:
                    indicators["dist_from_low_pct"] = round((last_close - day_low) / day_low * 100, 2)

        except Exception as e:
            logger.warning(f"Error computing indicators: {e}")

        # Remove None values
        return {k: v for k, v in indicators.items() if v is not None}

    def _compute_confluence(self, indicators_by_tf: dict) -> dict:
        """Compute multi-timeframe confluence score."""
        bull_count = 0
        bear_count = 0
        signals = {"bullish": [], "bearish": []}

        for tf, ind in indicators_by_tf.items():
            if not ind:
                continue

            # RSI signals
            rsi = ind.get("rsi_14")
            if rsi is not None:
                if rsi < 30:
                    bull_count += 1
                    signals["bullish"].append(f"RSI_{tf}_oversold")
                elif rsi > 70:
                    bear_count += 1
                    signals["bearish"].append(f"RSI_{tf}_overbought")

            # EMA trend
            ema9 = ind.get("ema_9")
            ema21 = ind.get("ema_21")
            if ema9 is not None and ema21 is not None:
                if ema9 > ema21:
                    bull_count += 1
                    signals["bullish"].append(f"EMA_{tf}_bullish_cross")
                else:
                    bear_count += 1
                    signals["bearish"].append(f"EMA_{tf}_bearish_cross")

            # MACD
            for key in ind:
                if "macdh" in key.lower() or "histogram" in key.lower():
                    val = ind[key]
                    if val is not None:
                        if val > 0:
                            bull_count += 1
                            signals["bullish"].append(f"MACD_{tf}_positive")
                        else:
                            bear_count += 1
                            signals["bearish"].append(f"MACD_{tf}_negative")

        total = bull_count + bear_count
        score = (bull_count - bear_count) / max(total, 1)

        return {
            "bull_count": bull_count,
            "bear_count": bear_count,
            "score": round(score, 3),
            "signals": signals,
        }

    async def _call_ollama(self, symbol: str, indicators_by_tf: dict, confluence: dict) -> dict:
        """Call Ollama with structured prompt for trade signal."""
        if ollama_client is None:
            logger.warning("Ollama not installed, returning HOLD")
            return {"action": "HOLD"}

        direction_rules = ""
        if self.allow_longs and self.allow_shorts:
            direction_rules = "You may suggest BUY (long), SELL (short), or HOLD."
        elif self.allow_longs:
            direction_rules = "You may only suggest BUY (long) or HOLD. No short selling."
        elif self.allow_shorts:
            direction_rules = "You may only suggest SELL (short) or HOLD. No long positions."
        else:
            return {"action": "HOLD", "reasoning": "Both longs and shorts disabled"}

        last_price = None
        for tf in ["1m", "5m", "15m"]:
            ind = indicators_by_tf.get(tf, {})
            if "last_close" in ind:
                last_price = ind["last_close"]
                break

        system_prompt = (
            "You are a professional day trader AI. Analyze the provided technical indicators "
            "and give a trading signal. " + direction_rules + "\n\n"
            "Rules:\n"
            "- BUY means open a LONG position\n"
            "- SELL means open a SHORT position\n"
            "- HOLD means do nothing\n"
            "- For BUY: stop_loss < entry_price, take_profit > entry_price\n"
            "- For SELL: stop_loss > entry_price, take_profit < entry_price\n\n"
            "Respond ONLY with valid JSON, no markdown, no explanation outside JSON."
        )

        user_prompt = (
            f"Symbol: {symbol}\n"
            f"Current Price: ${last_price}\n\n"
            f"1-Minute Indicators: {json.dumps(indicators_by_tf.get('1m', {}))}\n\n"
            f"5-Minute Indicators: {json.dumps(indicators_by_tf.get('5m', {}))}\n\n"
            f"15-Minute Indicators: {json.dumps(indicators_by_tf.get('15m', {}))}\n\n"
            f"Confluence: {json.dumps(confluence)}\n\n"
            "Respond with JSON:\n"
            '{"action": "BUY|SELL|HOLD", "confidence": 0.0-1.0, '
            '"reasoning": "max 200 chars", '
            '"entry_price": <price>, "stop_loss": <price>, "take_profit": <price>, '
            '"timeframe": "scalp|intraday", '
            '"indicators_bullish": [...], "indicators_bearish": [...]}'
        )

        try:
            response = await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: ollama_client.chat(
                    model=self.ollama_model,
                    messages=[
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_prompt},
                    ],
                    options={"temperature": 0.3},
                ),
            )

            content = response["message"]["content"].strip()
            # Try to extract JSON from response
            # Handle markdown code blocks
            if "```" in content:
                content = content.split("```")[1]
                if content.startswith("json"):
                    content = content[4:]
                content = content.strip()

            # Try to find JSON object in the response text
            import re
            json_match = re.search(r'\{[^{}]*\}', content, re.DOTALL)
            if json_match:
                content = json_match.group()

            signal = json.loads(content)
            logger.info(f"Ollama signal for {symbol}: {signal.get('action')} (conf: {signal.get('confidence')})")
            return signal

        except json.JSONDecodeError as e:
            logger.warning(f"Ollama JSON parse error for {symbol}: {e}")
            return {"action": "HOLD", "reasoning": "Parse error"}
        except Exception as e:
            logger.warning(f"Ollama error for {symbol}: {e}")
            return {"action": "HOLD", "reasoning": f"Ollama error: {str(e)[:100]}"}

    async def _analyze_symbol(self, symbol: str, data_by_tf: dict):
        """Run full analysis on a symbol with all timeframe data."""
        indicators_by_tf = {}
        for tf, bars_dict in data_by_tf.items():
            if bars_dict:
                try:
                    df = pd.DataFrame(bars_dict)
                    indicators_by_tf[tf] = self._compute_indicators(df)
                except Exception as e:
                    logger.warning(f"Error building df for {symbol} {tf}: {e}")
                    indicators_by_tf[tf] = {}
            else:
                indicators_by_tf[tf] = {}

        confluence = self._compute_confluence(indicators_by_tf)
        signal = await self._call_ollama(symbol, indicators_by_tf, confluence)

        action = signal.get("action", "HOLD").upper()
        confidence = float(signal.get("confidence", 0))

        if action != "HOLD" and confidence >= self.confidence_threshold:
            # Direction filter
            if action == "BUY" and not self.allow_longs:
                return
            if action == "SELL" and not self.allow_shorts:
                return

            logger.info(f"Trade signal: {action} {symbol} (confidence: {confidence})")
            self.send("RiskAgent", "trade_signal", {
                "symbol": symbol,
                "action": action,
                "confidence": confidence,
                "entry_price": signal.get("entry_price"),
                "stop_loss": signal.get("stop_loss"),
                "take_profit": signal.get("take_profit"),
                "timeframe": signal.get("timeframe", "intraday"),
                "reasoning": signal.get("reasoning", ""),
                "indicators_bullish": signal.get("indicators_bullish", []),
                "indicators_bearish": signal.get("indicators_bearish", []),
            })

    async def handle_message(self, message: Message):
        msg_type = message.type
        payload = message.payload

        if msg_type == "data_response":
            symbol = payload.get("symbol")
            timeframe = payload.get("timeframe")
            request_id = payload.get("request_id", "")

            if symbol not in self._pending_data:
                self._pending_data[symbol] = {}

            self._pending_data[symbol][timeframe] = payload.get("bars", {})

            # Check if we have all 3 timeframes
            if len(self._pending_data[symbol]) >= 3:
                data = self._pending_data.pop(symbol)
                await self._analyze_symbol(symbol, data)

        elif msg_type == "cool_down_response":
            symbol = payload.get("symbol")
            is_cooled = payload.get("cooled_down", False)
            self._cool_downs[symbol] = is_cooled

        elif msg_type == "force_scan":
            symbol = payload.get("symbol")
            if symbol:
                await self._request_data_and_analyze(symbol)
            else:
                await self._scan_all_symbols()

        elif msg_type == "pause":
            self._paused = True
            logger.info("StrategyAgent paused")

        elif msg_type == "resume":
            self._paused = False
            logger.info("StrategyAgent resumed")

        elif msg_type == "update_setting":
            key = payload.get("key")
            value = payload.get("value")
            if key == "confidence_threshold":
                self.confidence_threshold = float(value)
                logger.info(f"Confidence threshold updated to {self.confidence_threshold}")
            elif key == "allow_shorts":
                self.allow_shorts = bool(value)
            elif key == "allow_longs":
                self.allow_longs = bool(value)

        elif msg_type == "watchlist_update":
            self.watchlist = payload.get("watchlist", self.watchlist)
