"""SQLite async database layer for the trading bot."""

import json
from datetime import datetime

import aiosqlite
from loguru import logger

DB_PATH = "trading_bot.db"


async def init_db():
    """Create tables if they don't exist."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                action TEXT NOT NULL,
                quantity INTEGER NOT NULL,
                entry_price REAL NOT NULL,
                exit_price REAL,
                stop_loss REAL,
                take_profit REAL,
                pnl REAL,
                risk_dollars REAL,
                reward_dollars REAL,
                rr_achieved REAL,
                hold_minutes REAL,
                exit_reason TEXT,
                indicators_bullish TEXT,
                indicators_bearish TEXT,
                confidence REAL,
                reasoning TEXT,
                entry_time TEXT,
                exit_time TEXT
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS daily_performance (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT UNIQUE NOT NULL,
                total_trades INTEGER DEFAULT 0,
                wins INTEGER DEFAULT 0,
                losses INTEGER DEFAULT 0,
                win_rate REAL DEFAULT 0.0,
                gross_pnl REAL DEFAULT 0.0,
                avg_win REAL DEFAULT 0.0,
                avg_loss REAL DEFAULT 0.0,
                avg_rr REAL DEFAULT 0.0,
                biggest_win REAL DEFAULT 0.0,
                biggest_loss REAL DEFAULT 0.0,
                goal_target REAL DEFAULT 100.0,
                goal_hit INTEGER DEFAULT 0
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS account_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                nav REAL,
                buying_power REAL,
                realized_pnl REAL,
                unrealized_pnl REAL,
                total_pnl REAL
            )
        """)
        await db.commit()
    logger.info("Database initialized")


async def insert_trade(trade: dict) -> int:
    """Insert a new trade record (entry only). Returns the row id."""
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            """INSERT INTO trades
               (symbol, action, quantity, entry_price, stop_loss, take_profit,
                risk_dollars, reward_dollars, confidence, reasoning,
                indicators_bullish, indicators_bearish, entry_time)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                trade["symbol"],
                trade["action"],
                trade["quantity"],
                trade["entry_price"],
                trade.get("stop_loss"),
                trade.get("take_profit"),
                trade.get("risk_dollars"),
                trade.get("reward_dollars"),
                trade.get("confidence"),
                trade.get("reasoning"),
                json.dumps(trade.get("indicators_bullish", [])),
                json.dumps(trade.get("indicators_bearish", [])),
                trade.get("entry_time", datetime.utcnow().isoformat()),
            ),
        )
        await db.commit()
        logger.info(f"Inserted trade for {trade['symbol']}, id={cursor.lastrowid}")
        return cursor.lastrowid


async def update_trade_exit(trade_id: int, exit_data: dict):
    """Update a trade record with exit information."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """UPDATE trades SET
               exit_price = ?, pnl = ?, rr_achieved = ?,
               hold_minutes = ?, exit_reason = ?, exit_time = ?
               WHERE id = ?""",
            (
                exit_data["exit_price"],
                exit_data["pnl"],
                exit_data.get("rr_achieved"),
                exit_data.get("hold_minutes"),
                exit_data.get("exit_reason", "MANUAL"),
                exit_data.get("exit_time", datetime.utcnow().isoformat()),
                trade_id,
            ),
        )
        await db.commit()
        logger.info(f"Updated trade {trade_id} with exit data")


async def get_todays_trades() -> list[dict]:
    """Get all trades entered today."""
    today = datetime.utcnow().strftime("%Y-%m-%d")
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT * FROM trades WHERE entry_time LIKE ? ORDER BY entry_time DESC",
            (f"{today}%",),
        )
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]


async def get_recent_trades(limit: int = 20) -> list[dict]:
    """Get the most recent closed trades."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT * FROM trades WHERE exit_price IS NOT NULL ORDER BY exit_time DESC LIMIT ?",
            (limit,),
        )
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]


async def get_all_trades() -> list[dict]:
    """Get all trades with exit data (for performance analysis)."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT * FROM trades WHERE exit_price IS NOT NULL ORDER BY exit_time DESC"
        )
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]


async def get_open_trades() -> list[dict]:
    """Get trades with no exit_time (still open per DB)."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT * FROM trades WHERE exit_time IS NULL ORDER BY entry_time DESC"
        )
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]


async def get_symbol_trades(symbol: str, limit: int = 100) -> list[dict]:
    """Get recent trades for a specific symbol."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT * FROM trades WHERE symbol = ? AND exit_price IS NOT NULL ORDER BY exit_time DESC LIMIT ?",
            (symbol, limit),
        )
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]


async def upsert_daily_performance(perf: dict):
    """Insert or update daily performance record."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """INSERT INTO daily_performance
               (date, total_trades, wins, losses, win_rate, gross_pnl,
                avg_win, avg_loss, avg_rr, biggest_win, biggest_loss,
                goal_target, goal_hit)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(date) DO UPDATE SET
               total_trades=excluded.total_trades, wins=excluded.wins,
               losses=excluded.losses, win_rate=excluded.win_rate,
               gross_pnl=excluded.gross_pnl, avg_win=excluded.avg_win,
               avg_loss=excluded.avg_loss, avg_rr=excluded.avg_rr,
               biggest_win=excluded.biggest_win, biggest_loss=excluded.biggest_loss,
               goal_target=excluded.goal_target, goal_hit=excluded.goal_hit""",
            (
                perf["date"],
                perf.get("total_trades", 0),
                perf.get("wins", 0),
                perf.get("losses", 0),
                perf.get("win_rate", 0.0),
                perf.get("gross_pnl", 0.0),
                perf.get("avg_win", 0.0),
                perf.get("avg_loss", 0.0),
                perf.get("avg_rr", 0.0),
                perf.get("biggest_win", 0.0),
                perf.get("biggest_loss", 0.0),
                perf.get("goal_target", 100.0),
                perf.get("goal_hit", 0),
            ),
        )
        await db.commit()


async def get_daily_performance(days: int = 7) -> list[dict]:
    """Get recent daily performance records."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT * FROM daily_performance ORDER BY date DESC LIMIT ?",
            (days,),
        )
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]


async def insert_account_snapshot(snap: dict):
    """Insert an account snapshot."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """INSERT INTO account_snapshots
               (timestamp, nav, buying_power, realized_pnl, unrealized_pnl, total_pnl)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (
                snap.get("timestamp", datetime.utcnow().isoformat()),
                snap.get("nav"),
                snap.get("buying_power"),
                snap.get("realized_pnl"),
                snap.get("unrealized_pnl"),
                snap.get("total_pnl"),
            ),
        )
        await db.commit()
