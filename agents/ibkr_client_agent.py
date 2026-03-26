"""IBKRClientAgent — Interactive Brokers connection, market data, orders, account.

This agent is the sole gateway to IB Gateway. It implements:
  - Auto-reconnection with exponential backoff
  - Real-time market data subscriptions
  - Account polling (every 10s)
  - Bracket order placement (entry + SL + TP)
  - ORDER FILL DETECTION via execDetailsEvent (Known Gap #1 — now implemented)
  - POSITION CLOSE DETECTION via orderStatusEvent (Known Gap #2 — now implemented)

All IB API calls use the async variants (qualifyContractsAsync, etc.)
to avoid "event loop is already running" errors on Python 3.12+.
"""

import asyncio
from datetime import datetime

from ib_insync import IB, Contract, LimitOrder, MarketOrder, Stock, StopOrder, Trade, util
from loguru import logger

from agents.base_agent import BaseAgent, Message


class IBKRClientAgent(BaseAgent):
    def __init__(self, config: dict, orchestrator=None):
        super().__init__("IBKRClientAgent", orchestrator)
        self.host = config.get("host", "127.0.0.1")
        self.port = config.get("port", 4002)
        self.client_id = config.get("client_id", 1)
        self.ib = IB()
        self._connected = False
        self._reconnect_delays = [5, 10, 30, 60, 120]
        self._subscriptions: dict[str, Contract] = {}
        self._market_data: dict[str, dict] = {}
        self._account_data: dict = {}
        self._active_orders: dict[int, dict] = {}
        self._bracket_groups: dict[int, dict] = {}
        self._acct_summary_reqid = None
        self._failed_symbols: set[str] = set()
        self._paused = False  # local flag to prevent double-pause broadcasts

    async def run(self):
        """Main loop: connect, subscribe events, poll account."""
        await self._connect()
        if self._connected:
            self.ib.execDetailsEvent += self._on_exec_detail
            self.ib.orderStatusEvent += self._on_order_status
            self.ib.newOrderEvent += self._on_new_order
            self.ib.disconnectedEvent += self._on_disconnect
            self.ib.errorEvent += self._on_error

            asyncio.ensure_future(self._account_poll_loop())

        while self._running:
            await self._process_inbox()
            # ib_insync processes events via the event loop automatically
            await asyncio.sleep(0.1)

    async def _connect(self):
        """Connect to IB Gateway with exponential backoff retries."""
        for attempt, delay in enumerate(self._reconnect_delays):
            try:
                logger.info(f"Connecting to IB Gateway at {self.host}:{self.port} (attempt {attempt + 1})")
                await self.ib.connectAsync(self.host, self.port, clientId=self.client_id)
                self._connected = True
                logger.info("Connected to IB Gateway successfully")
                self.broadcast("ibkr_reconnected", {"status": "connected"})
                return
            except Exception as e:
                logger.warning(f"IB connection attempt {attempt + 1} failed: {e}")
                if attempt < len(self._reconnect_delays) - 1:
                    await asyncio.sleep(delay)

        logger.error("Failed to connect to IB Gateway after all retries")
        self._connected = False
        self.broadcast("ibkr_disconnect_fatal", {"reason": "All connection attempts failed"})

    def _on_error(self, reqId, errorCode, errorString, contract):
        """Handle IB error events gracefully."""
        # 10089 = market data subscription required
        if errorCode == 10089 and contract:
            symbol = getattr(contract, 'symbol', '???')
            if symbol not in self._failed_symbols:
                self._failed_symbols.add(symbol)
                logger.warning(f"No market data subscription for {symbol} — removing from subscriptions")
                self._subscriptions.pop(symbol, None)
        # 10349 = TIF warning (informational, not critical)
        elif errorCode == 10349:
            pass  # Already handled
        # 201 = Order rejected
        elif errorCode == 201:
            if "Exchange is closed" in errorString or "exchange is closed" in errorString:
                # Order arrived just after market close — not a code bug, just bad timing
                symbol = "???"
                for parent_id, info in self._bracket_groups.items():
                    if reqId in (parent_id, info.get("sl_order_id"), info.get("tp_order_id")):
                        symbol = info.get("symbol", "???")
                        break
                logger.warning(f"Order rejected — exchange closed for {symbol} (reqId={reqId}). Will retry next session.")
                self.broadcast("order_rejected", {
                    "symbol": symbol,
                    "order_id": reqId,
                    "reason": "Exchange is closed. Order arrived after market close.",
                })
            elif "Pattern Day Trader" in errorString or "PDT" in errorString:
                # contract is None for 201 in ib_insync — look up symbol from bracket groups
                symbol = "???"
                for parent_id, info in self._bracket_groups.items():
                    if reqId in (parent_id, info.get("sl_order_id"), info.get("tp_order_id")):
                        symbol = info.get("symbol", "???")
                        break
                logger.warning(f"PDT rejection on {symbol} (reqId={reqId}): {errorString[:120]}")
                # Prevent double-pause (also triggered by _on_order_status)
                if not self._paused:
                    self._paused = True
                    self.broadcast("pause", {})
                    logger.warning("Auto-paused trading due to PDT rejection")
                    self.broadcast("order_rejected", {
                        "symbol": symbol,
                        "order_id": reqId,
                        "reason": (
                            "PDT rejection on paper account. Fix: IB Client Portal → "
                            "Settings → Paper Trading Account → Reset → select Cash type. "
                            "Your real account is unaffected."
                        ),
                    })
        # Log other errors
        elif errorCode not in (2104, 2106, 2158, 2119):  # Skip info/connection msgs
            symbol = getattr(contract, 'symbol', '') if contract else ''
            logger.warning(f"IB Error {errorCode} (reqId={reqId}): {errorString}" +
                          (f" [{symbol}]" if symbol else ""))

    def _on_disconnect(self):
        """Handle unexpected disconnection."""
        logger.warning("IB Gateway disconnected")
        self._connected = False
        asyncio.ensure_future(self._reconnect())

    async def _reconnect(self):
        """Attempt reconnection with backoff."""
        for delay in self._reconnect_delays:
            await asyncio.sleep(delay)
            try:
                await self.ib.connectAsync(self.host, self.port, clientId=self.client_id)
                self._connected = True
                logger.info("Reconnected to IB Gateway")
                self.broadcast("ibkr_reconnected", {"status": "reconnected"})
                return
            except Exception as e:
                logger.warning(f"Reconnection failed: {e}")

        logger.error("Fatal: unable to reconnect to IB Gateway")
        self.broadcast("ibkr_disconnect_fatal", {"reason": "Reconnection failed after all retries"})

    # ─── Order Fill Detection (Known Gap #1) ───────────────────────────

    def _on_exec_detail(self, trade: Trade, fill):
        """Called by ib_insync on execution detail — detects order fills."""
        try:
            execution = fill.execution
            contract = fill.contract

            order_id = execution.orderId
            symbol = contract.symbol
            fill_price = execution.avgPrice
            quantity = int(execution.shares)
            side = execution.side
            exec_time = execution.time

            logger.info(
                f"FILL DETECTED: {symbol} {side} x{quantity} @ ${fill_price} "
                f"(orderId={order_id}, execId={execution.execId})"
            )

            bracket_info = self._bracket_groups.get(order_id)
            is_entry = bracket_info is not None and bracket_info.get("role") == "parent"

            is_child = False
            parent_order_id = None
            child_role = None
            for parent_id, info in self._bracket_groups.items():
                if order_id == info.get("sl_order_id"):
                    is_child = True
                    parent_order_id = parent_id
                    child_role = "stop_loss"
                    break
                elif order_id == info.get("tp_order_id"):
                    is_child = True
                    parent_order_id = parent_id
                    child_role = "take_profit"
                    break

            if is_entry:
                self.broadcast("order_filled", {
                    "symbol": symbol, "fill_price": fill_price,
                    "quantity": quantity, "side": side,
                    "order_id": order_id, "exec_time": str(exec_time),
                    "fill_type": "entry",
                })
            elif is_child:
                exit_reason = "SL" if child_role == "stop_loss" else "TP"
                logger.info(f"POSITION CLOSE DETECTED: {symbol} closed by {exit_reason} @ ${fill_price}")
                self.broadcast("position_closed", {
                    "symbol": symbol, "fill_price": fill_price,
                    "quantity": quantity, "side": side,
                    "order_id": order_id, "parent_order_id": parent_order_id,
                    "exit_reason": exit_reason, "exec_time": str(exec_time),
                    "fill_type": "exit",
                })
            else:
                self.broadcast("order_filled", {
                    "symbol": symbol, "fill_price": fill_price,
                    "quantity": quantity, "side": side,
                    "order_id": order_id, "exec_time": str(exec_time),
                    "fill_type": "close",
                })
        except Exception as e:
            logger.exception(f"Error in _on_exec_detail: {e}")

    # ─── Position Close Detection (Known Gap #2) ──────────────────────

    def _on_order_status(self, trade: Trade):
        """Called by ib_insync on order status changes — secondary close detection."""
        try:
            order = trade.order
            order_status = trade.orderStatus
            order_id = order.orderId
            status = order_status.status

            for parent_id, info in self._bracket_groups.items():
                sl_id = info.get("sl_order_id")
                tp_id = info.get("tp_order_id")

                if order_id in (sl_id, tp_id) and status == "Cancelled":
                    other_role = "TP" if order_id == sl_id else "SL"
                    logger.info(
                        f"Bracket child cancelled (orderId={order_id}): "
                        f"position closed by {other_role} for {info.get('symbol')}"
                    )
                    info["resolved"] = True
                    break

                if order_id == parent_id and status == "Filled":
                    logger.info(f"Parent order filled (orderId={order_id}) for {info.get('symbol')}")
                    break

                if order_id == parent_id and status == "Cancelled":
                    symbol = info.get("symbol", "???")
                    logger.warning(f"Parent order CANCELLED (orderId={order_id}) for {symbol}")
                    info["resolved"] = True

                    # Check if this is a PDT rejection from the trade log
                    log_entries = getattr(trade, 'log', [])
                    reason = "Parent order cancelled by broker"
                    for log_entry in log_entries:
                        msg = getattr(log_entry, 'message', '')
                        if 'Pattern Day Trader' in msg or 'PDT' in msg:
                            reason = (
                                "PDT rejection on paper account. Fix: IB Client Portal → "
                                "Settings → Paper Trading Account → Reset → select Cash type."
                            )
                            if not self._paused:
                                self._paused = True
                                self.broadcast("pause", {})
                                logger.warning("Auto-paused trading due to PDT rejection")
                            break

                    self.broadcast("order_rejected", {
                        "symbol": symbol,
                        "order_id": order_id,
                        "reason": reason,
                    })
                    break
        except Exception as e:
            logger.exception(f"Error in _on_order_status: {e}")

    def _on_new_order(self, trade: Trade):
        logger.debug(f"New order tracked: orderId={trade.order.orderId}, {trade.contract.symbol}")

    # ─── Account Polling ──────────────────────────────────────────────

    async def _account_poll_loop(self):
        """Poll account info every 10 seconds and broadcast."""
        while self._running and self._connected:
            try:
                await self._poll_account()
            except Exception as e:
                logger.warning(f"Account poll error: {e}")
            await asyncio.sleep(30)

    async def _poll_account(self):
        """Fetch account and position data from IB using managed accounts."""
        try:
            # Use managedAccounts to get account ID, then request updates
            accounts = self.ib.managedAccounts()
            if not accounts:
                return
            account_id = accounts[0]

            # Request account values via async wrapper
            await self.ib.reqAccountUpdatesAsync(account_id)
            await asyncio.sleep(1)  # Give IB time to send the data

            # Now read the cached values
            account_values = self.ib.accountValues(account_id)

            nav = 0.0
            buying_power = 0.0
            realized_pnl = 0.0
            unrealized_pnl = 0.0

            for av in account_values:
                tag = av.tag
                cur = av.currency
                try:
                    val = float(av.value)
                except (ValueError, TypeError):
                    continue
                if tag in ("NetLiquidation", "NetLiquidationByCurrency") and cur == "USD":
                    nav = val
                elif tag == "BuyingPower":
                    buying_power = val
                elif tag == "RealizedPnL" and cur == "USD":
                    realized_pnl = val
                elif tag == "UnrealizedPnL" and cur == "USD":
                    unrealized_pnl = val

            # Read cached positions
            positions = self.ib.positions(account_id)

            open_positions = []
            for pos in positions:
                if pos.position != 0:
                    open_positions.append({
                        "symbol": pos.contract.symbol,
                        "quantity": int(pos.position),
                        "avg_cost": pos.avgCost,
                        "unrealized_pnl": 0.0,
                    })

            self._account_data = {
                "nav": nav,
                "buying_power": buying_power,
                "realized_pnl": realized_pnl,
                "unrealized_pnl": unrealized_pnl,
                "open_positions": open_positions,
                "timestamp": datetime.utcnow().isoformat(),
            }

            self.broadcast("account_update", self._account_data)
        except Exception as e:
            logger.warning(f"Error polling account: {e}")

    # ─── Market Data ──────────────────────────────────────────────────

    async def subscribe_market_data(self, symbols: list[str]):
        """Subscribe to real-time market data for given symbols."""
        for symbol in symbols:
            if symbol in self._failed_symbols:
                continue  # Skip symbols without data subscriptions
            if symbol not in self._subscriptions:
                contract = Stock(symbol, "SMART", "USD")
                try:
                    await self.ib.qualifyContractsAsync(contract)
                    self._subscriptions[symbol] = contract
                    self.ib.reqMktData(contract)
                    logger.info(f"Subscribed to market data for {symbol}")
                except Exception as e:
                    logger.warning(f"Failed to subscribe to {symbol}: {e}")
                await asyncio.sleep(0.5)

    def get_snapshot(self, symbol: str) -> dict | None:
        """Get current market data snapshot for a symbol."""
        contract = self._subscriptions.get(symbol)
        if not contract:
            return None

        ticker = self.ib.ticker(contract)
        if not ticker:
            return None

        import math

        def _clean(val):
            """Return None if value is nan, -1, or missing."""
            if val is None:
                return None
            try:
                if math.isnan(val) or val == -1:
                    return None
            except TypeError:
                return None
            return val

        return {
            "symbol": symbol,
            "bid": _clean(ticker.bid),
            "ask": _clean(ticker.ask),
            "last": _clean(ticker.last),
            "volume": _clean(ticker.volume),
            "open": _clean(ticker.open),
            "high": _clean(ticker.high),
            "low": _clean(ticker.low),
            "close": _clean(ticker.close),
        }

    async def get_historical_bars(self, symbol: str, duration: str = "5 D", bar_size: str = "1 min") -> list:
        """Fetch historical bar data from IB."""
        contract = self._subscriptions.get(symbol)
        if not contract:
            contract = Stock(symbol, "SMART", "USD")
            await self.ib.qualifyContractsAsync(contract)

        try:
            bars = await self.ib.reqHistoricalDataAsync(
                contract,
                endDateTime="",
                durationStr=duration,
                barSizeSetting=bar_size,
                whatToShow="TRADES",
                useRTH=True,
                formatDate=1,
            )
            return bars or []
        except Exception as e:
            logger.warning(f"Error fetching historical bars for {symbol}: {e}")
            return []

    # ─── Order Placement ──────────────────────────────────────────────

    async def place_bracket_order(self, order_params: dict) -> dict | None:
        """Place a bracket order: entry (Limit) + SL (Stop) + TP (Limit)."""
        symbol = order_params["symbol"]
        action = order_params["action"]
        quantity = order_params["quantity"]
        entry_price = order_params["entry_price"]
        stop_loss = order_params["stop_loss"]
        take_profit = order_params["take_profit"]

        contract = self._subscriptions.get(symbol)
        if not contract:
            contract = Stock(symbol, "SMART", "USD")
            await self.ib.qualifyContractsAsync(contract)

        try:
            bracket = self.ib.bracketOrder(
                action=action,
                quantity=quantity,
                limitPrice=round(entry_price, 2),
                takeProfitPrice=round(take_profit, 2),
                stopLossPrice=round(stop_loss, 2),
            )

            parent_order, tp_order, sl_order = bracket

            # Explicitly set TIF to DAY and outsideRth to False
            # This prevents IB Gateway order presets from interfering
            for order in [parent_order, tp_order, sl_order]:
                order.tif = "DAY"
                order.outsideRth = False

            trades = []
            for order in [parent_order, tp_order, sl_order]:
                trade = self.ib.placeOrder(contract, order)
                trades.append(trade)

            parent_trade, tp_trade, sl_trade = trades

            parent_id = parent_trade.order.orderId
            self._bracket_groups[parent_id] = {
                "symbol": symbol, "action": action,
                "quantity": quantity, "entry_price": entry_price,
                "stop_loss": stop_loss, "take_profit": take_profit,
                "sl_order_id": sl_trade.order.orderId,
                "tp_order_id": tp_trade.order.orderId,
                "role": "parent", "resolved": False,
            }

            logger.info(
                f"Bracket order placed for {symbol}: "
                f"parent={parent_id}, SL={sl_trade.order.orderId}, TP={tp_trade.order.orderId}"
            )

            return {
                "parent_order_id": parent_id,
                "sl_order_id": sl_trade.order.orderId,
                "tp_order_id": tp_trade.order.orderId,
                "symbol": symbol, "action": action, "quantity": quantity,
            }
        except Exception as e:
            logger.exception(f"Error placing bracket order for {symbol}: {e}")
            return None

    async def close_position_market(self, symbol: str) -> bool:
        """Close a position at market price."""
        try:
            positions = self.ib.positions()
            for pos in positions:
                if pos.contract.symbol == symbol and pos.position != 0:
                    qty = abs(int(pos.position))
                    close_action = "SELL" if pos.position > 0 else "BUY"
                    contract = pos.contract
                    order = MarketOrder(close_action, qty)
                    self.ib.placeOrder(contract, order)
                    logger.info(f"Market close order placed for {symbol}: {close_action} x{qty}")
                    return True
            logger.warning(f"No open position found for {symbol}")
            return False
        except Exception as e:
            logger.exception(f"Error closing position for {symbol}: {e}")
            return False

    async def cancel_order(self, order_id: int) -> bool:
        """Cancel an open order by ID."""
        try:
            open_trades = self.ib.openTrades()
            for trade in open_trades:
                if trade.order.orderId == order_id:
                    self.ib.cancelOrder(trade.order)
                    logger.info(f"Cancelled order {order_id}")
                    return True
            logger.warning(f"Order {order_id} not found in open trades")
            return False
        except Exception as e:
            logger.exception(f"Error cancelling order {order_id}: {e}")
            return False

    # ─── Message Handling ─────────────────────────────────────────────

    async def handle_message(self, message: Message):
        msg_type = message.type
        payload = message.payload

        if msg_type == "get_snapshot":
            symbol = payload.get("symbol")
            snapshot = self.get_snapshot(symbol)
            self.send(message.sender, "snapshot_response", {
                "symbol": symbol, "data": snapshot,
                "request_id": payload.get("request_id"),
            })

        elif msg_type == "place_bracket_order":
            result = await self.place_bracket_order(payload)
            self.send(message.sender, "bracket_order_response", {
                "result": result, "original_order": payload,
            })

        elif msg_type == "close_position":
            symbol = payload.get("symbol")
            reason = payload.get("reason", "MANUAL")
            success = await self.close_position_market(symbol)
            self.send(message.sender, "close_position_response", {
                "symbol": symbol, "success": success, "reason": reason,
            })

        elif msg_type == "cancel_order":
            order_id = payload.get("order_id")
            success = await self.cancel_order(order_id)
            self.send(message.sender, "cancel_order_response", {
                "order_id": order_id, "success": success,
            })

        elif msg_type == "get_historical_bars":
            symbol = payload.get("symbol")
            duration = payload.get("duration", "5 D")
            bar_size = payload.get("bar_size", "1 min")
            bars = await self.get_historical_bars(symbol, duration, bar_size)
            self.send(message.sender, "historical_bars_response", {
                "symbol": symbol, "bars": bars,
                "duration": duration, "bar_size": bar_size,
            })

        elif msg_type == "subscribe_symbols":
            symbols = payload.get("symbols", [])
            await self.subscribe_market_data(symbols)

        elif msg_type == "resume":
            self._paused = False

        elif msg_type == "get_account":
            self.send(message.sender, "account_response", self._account_data)

    async def stop(self):
        """Disconnect from IB on shutdown."""
        if self._connected:
            try:
                self.ib.disconnect()
                logger.info("Disconnected from IB Gateway")
            except Exception as e:
                logger.warning(f"Error disconnecting from IB: {e}")
        await super().stop()
