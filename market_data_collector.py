import sys
import json
import sqlite3
import requests
import os
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from loguru import logger
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
from typing import List, Dict, Optional

# Ensure logs directory exists
os.makedirs(os.path.join(os.path.dirname(__file__), "logs"), exist_ok=True)

UTC = timezone.utc
EDT = ZoneInfo("America/New_York")
logger._core.timestamp = lambda: datetime.now(UTC).astimezone(EDT)

# Remove any existing loggers
logger.remove()

# File logger (Eastern Time via patch)
logger.add(
    os.path.join("logs", "app.log"),
    rotation="1 MB",
    retention="10 days",
    enqueue=True,
    format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}"
)

# Console logger (also Eastern Time via patch)
logger.add(
    sys.stdout,
    level="INFO",
    colorize=True,
    enqueue=True,
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{message}</level>",
    backtrace=True,
    diagnose=True,
)

# SIMPLIFIED APPROACH: End-of-day minute data collection only with 250-day retention
# Database paths - use environment-aware path
DATABASE_PATH = os.getenv('DATA_DIR', './data') + "/market_data.db"

# Data retention configuration
MAX_RETENTION_DAYS = 250

class MarketDataCollector:
    """
    SIMPLIFIED Market Data Collector - END OF DAY ONLY with 250-day retention
    
    Collects:
    - Neural net picks (from trade_plan.json) with minute OHLCV data
    - Trade outcomes (from trades.csv)
    - Basic daily summaries for picks
    
    Features:
    - Automatic cleanup of data older than 250 days
    - Rolling window approach to maintain database size
    
    NO MORE: Morning collection, market context, real-time data
    """
    
    def __init__(self):
        self.db_path = DATABASE_PATH
        self.session = requests.Session()
        self.max_retention_days = MAX_RETENTION_DAYS
        self.init_database()
    
    def init_database(self):
        """Initialize SQLite database with required tables for optimizer"""
        # Create data directory if it doesn't exist
        db_dir = os.path.dirname(self.db_path)
        if db_dir and not os.path.exists(db_dir):
            os.makedirs(db_dir, exist_ok=True)
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # Daily picks table - CORE TABLE
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS daily_picks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    date TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    direction TEXT NOT NULL,
                    confidence REAL DEFAULT 0.7,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(date, symbol)
                )
            """)
            
            # Minute OHLCV data - CORE FOR OPTIMIZER (picks only)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS minute_ohlcv (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    date TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    minute_timestamp TEXT NOT NULL,
                    open REAL NOT NULL,
                    high REAL NOT NULL,
                    low REAL NOT NULL,
                    close REAL NOT NULL,
                    volume INTEGER NOT NULL,
                    vwap REAL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(date, symbol, minute_timestamp)
                )
            """)
            
            # Trade outcomes - CORE TABLE (links to trades.csv)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS trade_outcomes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    date TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    entry_price REAL NOT NULL,
                    exit_price REAL,
                    entry_time TEXT,
                    exit_time TEXT,
                    pnl_percent REAL,
                    direction TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(date, symbol)
                )
            """)
            
            # Daily summaries - Generated from minute data
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS daily_summaries (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    date TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    prev_close REAL NOT NULL,
                    open_price REAL NOT NULL,
                    gap_percent REAL NOT NULL,
                    day_high REAL NOT NULL,
                    day_low REAL NOT NULL,
                    day_close REAL NOT NULL,
                    day_volume INTEGER NOT NULL,
                    volatility REAL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(date, symbol)
                )
            """)
            
            # Create indexes for faster queries
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_picks_date ON daily_picks(date)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_picks_symbol ON daily_picks(symbol)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_ohlcv_date_symbol ON minute_ohlcv(date, symbol)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_ohlcv_timestamp ON minute_ohlcv(minute_timestamp)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_summaries_date ON daily_summaries(date)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_outcomes_date ON trade_outcomes(date)")
            
            conn.commit()
        
        logger.debug(f"[DATA] Database initialized - EOD minute data collection with {self.max_retention_days}-day retention")
    
    def cleanup_old_data(self, cutoff_date: str):
        """
        Remove data older than cutoff_date to maintain rolling 250-day window
        
        Args:
            cutoff_date: Date string in YYYY-MM-DD format. Data before this date will be deleted.
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Count records before cleanup
                tables = ['daily_picks', 'minute_ohlcv', 'trade_outcomes', 'daily_summaries']
                before_counts = {}
                
                for table in tables:
                    cursor.execute(f"SELECT COUNT(*) FROM {table} WHERE date < ?", (cutoff_date,))
                    before_counts[table] = cursor.fetchone()[0]
                
                # Delete old data from each table
                total_deleted = 0
                for table in tables:
                    cursor.execute(f"DELETE FROM {table} WHERE date < ?", (cutoff_date,))
                    deleted = cursor.rowcount
                    total_deleted += deleted
                    
                    if deleted > 0:
                        logger.info(f"[DATA] Deleted {deleted} old records from {table} (before {cutoff_date})")
                
                # Clean up any orphaned data and optimize
                cursor.execute("VACUUM")
                conn.commit()
                
                if total_deleted > 0:
                    logger.info(f"[DATA] Cleanup complete: Removed {total_deleted} total records older than {cutoff_date}")
                    logger.info(f"[DATA] Database optimized - maintaining {self.max_retention_days}-day rolling window")
                else:
                    logger.debug(f"[DATA] No cleanup needed - no data older than {cutoff_date}")
                    
        except Exception as e:
            logger.error(f"[DATA] Error during cleanup: {e}")
            raise
    
    def _get_trading_day_cutoff(self, keep_days: int) -> Optional[str]:
        """
        Get the cutoff date to keep only the most recent N trading days
        
        Args:
            keep_days: Number of most recent trading days to keep
            
        Returns:
            Date string (YYYY-MM-DD) representing the cutoff. Data before this date should be deleted.
            Returns None if there aren't enough trading days or on error.
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Get all distinct trading dates in descending order (newest first)
                cursor.execute("""
                    SELECT DISTINCT date 
                    FROM daily_summaries 
                    ORDER BY date DESC
                """)
                
                trading_dates = [row[0] for row in cursor.fetchall()]
                
                if len(trading_dates) <= keep_days:
                    # We don't have more trading days than we want to keep
                    logger.debug(f"[DATA] Only {len(trading_dates)} trading days in database, no cleanup needed")
                    return None
                
                # The cutoff date is the date at index keep_days (0-indexed)
                # This means we keep dates [0] through [keep_days-1] and delete everything before [keep_days]
                cutoff_date = trading_dates[keep_days]
                
                logger.debug(f"[DATA] Trading day calculation: Keep most recent {keep_days} days, "
                           f"cutoff at {cutoff_date} (will keep {keep_days} days: {trading_dates[0]} to {trading_dates[keep_days-1]})")
                
                return cutoff_date
                
        except Exception as e:
            logger.error(f"[DATA] Error calculating trading day cutoff: {e}")
            return None
    
    def get_data_date_range(self) -> tuple:
        """
        Get the oldest and newest dates in the database along with total trading days
        
        Returns:
            tuple: (oldest_date, newest_date, total_trading_days) or (None, None, 0) if no data
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Get date range and count distinct trading days from daily_summaries
                cursor.execute("""
                    SELECT MIN(date) as oldest, MAX(date) as newest, COUNT(DISTINCT date) as trading_days
                    FROM daily_summaries
                """)
                
                result = cursor.fetchone()
                oldest, newest, trading_days = result
                
                return oldest, newest, trading_days or 0
                
        except Exception as e:
            logger.error(f"[DATA] Error getting date range: {e}")
            return None, None, 0
    
    def enforce_retention_policy(self, current_date: str):
        """
        Enforce the 250-trading-day retention policy by removing old data if necessary
        
        This counts actual trading days in the database, not calendar days.
        
        Args:
            current_date: The date being added (YYYY-MM-DD format)
        """
        try:
            oldest_date, newest_date, total_trading_days = self.get_data_date_range()
            
            if not oldest_date:
                logger.debug("[DATA] No existing data - retention policy not needed")
                return
            
            logger.info(f"[DATA] Current data range: {oldest_date} to {newest_date} ({total_trading_days} trading days)")
            
            # Check if we exceed the trading day limit
            if total_trading_days <= self.max_retention_days:
                logger.debug(f"[DATA] Retention policy check passed: {total_trading_days} trading days ≤ {self.max_retention_days} limit")
                return
            
            # We have too many trading days - need to remove the oldest ones
            excess_days = total_trading_days - self.max_retention_days
            logger.info(f"[DATA] Retention policy triggered: {total_trading_days} trading days exceeds {self.max_retention_days} limit by {excess_days} days")
            
            # Get the cutoff date by finding the Nth oldest trading day to keep
            cutoff_date = self._get_trading_day_cutoff(self.max_retention_days)
            
            if cutoff_date:
                logger.info(f"[DATA] Removing data older than {cutoff_date} to maintain {self.max_retention_days} trading days")
                
                self.cleanup_old_data(cutoff_date)
                
                # Verify cleanup
                new_oldest, new_newest, new_total = self.get_data_date_range()
                if new_oldest:
                    logger.success(f"[DATA] Retention policy applied: New range {new_oldest} to {new_newest} ({new_total} trading days)")
                else:
                    logger.warning("[DATA] No data remaining after cleanup")
            else:
                logger.error("[DATA] Could not determine trading day cutoff - skipping cleanup")
                
        except Exception as e:
            logger.error(f"[DATA] Error enforcing retention policy: {e}")
            # Don't raise - we want data collection to continue even if cleanup fails
    
    def get_todays_picks_symbols(self) -> List[str]:
        """Get today's neural net picks from trade_plan.json"""
        try:
            # Look in ./data/ folder first (new location)
            trade_plan_path = "./data/trade_plan.json"
            if not os.path.exists(trade_plan_path):
                # Fallback to root directory (old location)
                trade_plan_path = "trade_plan.json"
                
            if os.path.exists(trade_plan_path):
                with open(trade_plan_path, "r") as f:
                    trade_plan = json.load(f)
                
                picks = []
                picks.extend(trade_plan.get("buy", []))
                picks.extend(trade_plan.get("short", []))
                
                logger.info(f"[DATA] Found {len(picks)} picks in {trade_plan_path}: {picks}")
                return picks
            else:
                logger.warning("[DATA] trade_plan.json not found in ./data/ or root directory, no picks to collect")
                return []
        except Exception as e:
            logger.error(f"[DATA] Error loading trade_plan.json: {e}")
            return []
    
    def collect_end_of_day_data(self, date: str = None):
        """
        END-OF-DAY ONLY collection with automatic retention policy enforcement
        
        Collects:
        - Minute OHLCV data for today's neural net picks (for optimizer)
        - Trade outcomes from trades.csv
        - Daily picks record
        - Generated daily summaries
        - Enforces 250-day retention policy
        """
        if date is None:
            date = datetime.now(EDT).strftime("%Y-%m-%d")
        
        logger.info(f"[DATA] Starting END-OF-DAY collection for {date}")
        
        # Enforce retention policy BEFORE adding new data
        logger.info(f"[DATA] Checking {self.max_retention_days}-trading-day retention policy...")
        self.enforce_retention_policy(date)
        
        # Get today's picks
        picks = self.get_todays_picks_symbols()
        
        if not picks:
            logger.warning("[DATA] No picks found for collection")
            # Still load trade outcomes even if no picks
            self._load_trade_outcomes(date)
            return
        
        logger.info(f"[DATA] Collecting minute data for {len(picks)} picks (optimizer needs timing data)")
        
        # Collect minute data for picks (needed for optimizer)
        self._collect_minute_data_batch(picks, date)
        
        # Generate daily summaries from minute data
        self._generate_daily_summaries(date)
        
        # Load today's picks into database
        self._load_todays_picks(date)
        
        # Load trade outcomes from CSV
        self._load_trade_outcomes(date)
        
        logger.info(f"[DATA] END-OF-DAY collection completed for {date}")
        logger.info(f"[DATA] Focused on {len(picks)} picks - maintaining {self.max_retention_days}-trading-day rolling window")
    
    def _collect_minute_data_batch(self, symbols: List[str], date: str):
        """Collect minute data for picks using threading (needed for optimizer)"""
        logger.info(f"[DATA] Collecting minute data for {len(symbols)} picks...")
        
        def collect_symbol_data(symbol):
            try:
                return self._collect_symbol_minute_data(symbol, date)
            except Exception as e:
                logger.error(f"[DATA] Error collecting {symbol}: {e}")
                return symbol, 0
        
        # Use threading to speed up collection
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(collect_symbol_data, symbol) for symbol in symbols]
            
            completed = 0
            total_records = 0
            
            for future in as_completed(futures):
                try:
                    symbol, records = future.result()
                    total_records += records
                    completed += 1
                    
                    logger.info(f"[DATA] Processed {completed}/{len(symbols)} symbols ({symbol}: {records} records)")
                        
                except Exception as e:
                    logger.error(f"[DATA] Thread error: {e}")
        
        logger.info(f"[DATA] Collected {total_records} minute records for {len(symbols)} picks")
    
    def _collect_symbol_minute_data(self, symbol: str, date: str) -> tuple:
        """Collect minute data for a single symbol (needed for optimizer timing analysis)"""
        try:
            ticker = yf.Ticker(symbol)
            
            # Get minute data for the trading day
            data = ticker.history(period="1d", interval="1m")
            
            if data.empty:
                logger.warning(f"[DATA] No minute data for {symbol} on {date}")
                return symbol, 0
            
            records_inserted = 0
            
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                for timestamp, row in data.iterrows():
                    # Convert timestamp to string
                    ts_str = timestamp.strftime("%H:%M:%S")
                    
                    # Calculate VWAP
                    typical_price = (row['High'] + row['Low'] + row['Close']) / 3
                    vwap = typical_price  # Simplified VWAP for now
                    
                    try:
                        cursor.execute("""
                            INSERT OR REPLACE INTO minute_ohlcv 
                            (date, symbol, minute_timestamp, open, high, low, close, volume, vwap)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, (
                            date, symbol, ts_str,
                            round(row['Open'], 4),
                            round(row['High'], 4),
                            round(row['Low'], 4),
                            round(row['Close'], 4),
                            int(row['Volume']),
                            round(vwap, 4)
                        ))
                        records_inserted += 1
                    except sqlite3.IntegrityError:
                        # Record already exists
                        pass
                
                conn.commit()
            
            return symbol, records_inserted
            
        except Exception as e:
            logger.error(f"[DATA] Error collecting minute data for {symbol}: {e}")
            return symbol, 0
        
    def _generate_daily_summaries(self, date: str):
        """Generate daily summaries from minute OHLCV data"""
        try:
            logger.info(f"[DATA] Generating daily summaries for {date}")
            
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Get all symbols that have minute data for this date
                cursor.execute("""
                    SELECT DISTINCT symbol FROM minute_ohlcv 
                    WHERE date = ?
                """, (date,))
                
                symbols_with_data = [row[0] for row in cursor.fetchall()]
                
                if not symbols_with_data:
                    logger.warning(f"[DATA] No minute data found for {date} to generate summaries")
                    return
                
                logger.info(f"[DATA] Generating summaries for {len(symbols_with_data)} symbols")
                
                for symbol in symbols_with_data:
                    try:
                        # Get minute data for this symbol and date
                        cursor.execute("""
                            SELECT open, high, low, close, volume, minute_timestamp
                            FROM minute_ohlcv 
                            WHERE date = ? AND symbol = ?
                            ORDER BY minute_timestamp
                        """, (date, symbol))
                        
                        minute_data = cursor.fetchall()
                        
                        if not minute_data:
                            logger.warning(f"[DATA] No minute data for {symbol} on {date}")
                            continue
                        
                        # Calculate daily summary statistics
                        opens = [row[0] for row in minute_data if row[0] is not None]
                        highs = [row[1] for row in minute_data if row[1] is not None]
                        lows = [row[2] for row in minute_data if row[2] is not None]
                        closes = [row[3] for row in minute_data if row[3] is not None]
                        volumes = [row[4] for row in minute_data if row[4] is not None]
                        
                        if not opens or not closes:
                            logger.warning(f"[DATA] Insufficient data for {symbol} summary")
                            continue
                        
                        # Daily summary values
                        open_price = opens[0]  # First minute's open
                        day_high = max(highs)
                        day_low = min(lows)
                        day_close = closes[-1]  # Last minute's close
                        day_volume = sum(volumes)
                        
                        # Get previous day's close for gap calculation
                        prev_close = self._get_previous_close(symbol, date)
                        
                        # Calculate gap percentage
                        if prev_close and prev_close > 0:
                            gap_percent = ((open_price - prev_close) / prev_close) * 100
                        else:
                            gap_percent = 0.0
                        
                        # Calculate volatility (standard deviation of close prices)
                        if len(closes) > 1:
                            import statistics
                            volatility = statistics.stdev(closes)
                        else:
                            volatility = 0.0
                        
                        # Insert daily summary
                        cursor.execute("""
                            INSERT OR REPLACE INTO daily_summaries
                            (date, symbol, prev_close, open_price, gap_percent, day_high, day_low, day_close, day_volume, volatility)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, (date, symbol, prev_close or 0.0, open_price, gap_percent, 
                              day_high, day_low, day_close, day_volume, volatility))
                        
                        logger.debug(f"[DATA] Generated summary for {symbol}: Open=${open_price:.2f}, Close=${day_close:.2f}, Gap={gap_percent:+.1f}%")
                        
                    except Exception as e:
                        logger.error(f"[DATA] Error generating summary for {symbol}: {e}")
                        continue
                
                conn.commit()
                logger.info(f"[DATA] Daily summaries generated for {len(symbols_with_data)} symbols")
                
        except Exception as e:
            logger.error(f"[DATA] Error generating daily summaries: {e}")

    def _get_previous_close(self, symbol: str, current_date: str) -> Optional[float]:
        """Get previous trading day's close price for gap calculation"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Look for the most recent daily summary before current date
                cursor.execute("""
                    SELECT day_close FROM daily_summaries 
                    WHERE symbol = ? AND date < ? 
                    ORDER BY date DESC 
                    LIMIT 1
                """, (symbol, current_date))
                
                result = cursor.fetchone()
                if result:
                    return result[0]
                
                # If no previous daily summary, try to get from yfinance
                try:
                    import yfinance as yf
                    from datetime import datetime, timedelta
                    
                    ticker = yf.Ticker(symbol)
                    current_dt = datetime.strptime(current_date, "%Y-%m-%d")
                    
                    # Get last 5 days to ensure we catch the previous trading day
                    start_date = current_dt - timedelta(days=5)
                    end_date = current_dt
                    
                    hist = ticker.history(start=start_date, end=end_date)
                    
                    if not hist.empty:
                        # Get the last available close before current date
                        return float(hist['Close'].iloc[-1])
                    
                except Exception as e:
                    logger.debug(f"[DATA] Could not get previous close for {symbol} from yfinance: {e}")
                
                return None
                
        except Exception as e:
            logger.error(f"[DATA] Error getting previous close for {symbol}: {e}")
            return None
    
    def _load_todays_picks(self, date: str):
        """Load today's picks into database from trade_plan.json"""
        try:
            if os.path.exists("trade_plan.json"):
                with open("trade_plan.json", "r") as f:
                    plan = json.load(f)
                
                picks = []
                for symbol in plan.get("buy", []):
                    picks.append((date, symbol, "BUY", 0.7))
                for symbol in plan.get("short", []):
                    picks.append((date, symbol, "SELL_SHORT", 0.7))
                
                if picks:
                    with sqlite3.connect(self.db_path) as conn:
                        cursor = conn.cursor()
                        cursor.executemany("""
                            INSERT OR REPLACE INTO daily_picks
                            (date, symbol, direction, confidence)
                            VALUES (?, ?, ?, ?)
                        """, picks)
                        conn.commit()
                    
                    logger.info(f"[DATA] Loaded {len(picks)} picks for {date}")
                else:
                    logger.info(f"[DATA] No picks found in trade_plan.json for {date}")
        
        except Exception as e:
            logger.error(f"[DATA] Error loading picks: {e}")
    
    def _load_trade_outcomes(self, date: str):
        """Load trade outcomes from trades.csv"""
        try:
            # Use environment-aware path
            trades_path = os.path.join(os.getenv('DATA_DIR', './data'), "trades.csv")
            if not os.path.exists(trades_path):
                trades_path = "trades.csv"
            
            if os.path.exists(trades_path):
                df = pd.read_csv(trades_path)
                df['date'] = pd.to_datetime(df['date'], errors='coerce')
                
                # Filter for today's trades
                today_trades = df[df['date'].dt.strftime('%Y-%m-%d') == date]
                
                if not today_trades.empty:
                    with sqlite3.connect(self.db_path) as conn:
                        cursor = conn.cursor()
                        
                        for _, row in today_trades.iterrows():
                            pnl_pct = row.get('pnl_pct', None)
                            if pd.isna(pnl_pct):
                                pnl_pct = None
                            
                            cursor.execute("""
                                INSERT OR REPLACE INTO trade_outcomes
                                (date, symbol, entry_price, exit_price, pnl_percent, direction)
                                VALUES (?, ?, ?, ?, ?, ?)
                            """, (
                                date,
                                row['symbol'],
                                row['entry'],
                                row.get('exit', None),
                                pnl_pct,
                                row['action']
                            ))
                        
                        conn.commit()
                        logger.info(f"[DATA] Loaded {len(today_trades)} trade outcomes for {date}")
                else:
                    logger.info(f"[DATA] No trades found in CSV for {date}")
            else:
                logger.warning(f"[DATA] No trades.csv file found")
        
        except Exception as e:
            logger.error(f"[DATA] Error loading trade outcomes: {e}")
    
    def verify_database_integrity(self):
        """Verify database exists and has proper structure"""
        try:
            if not os.path.exists(self.db_path):
                logger.warning(f"[DATA] Database file does not exist at {self.db_path}")
                return False
            
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Check if core tables exist
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
                tables = [row[0] for row in cursor.fetchall()]
                
                expected_tables = ["daily_picks", "minute_ohlcv", "daily_summaries", "trade_outcomes"]
                missing_tables = [t for t in expected_tables if t not in tables]
                
                if missing_tables:
                    logger.warning(f"[DATA] Missing core tables: {missing_tables}")
                    return False
                
                # Check record counts and date range
                total_records = 0
                oldest_date, newest_date, total_days = self.get_data_date_range()
                
                for table in expected_tables:
                    cursor.execute(f"SELECT COUNT(*) FROM {table}")
                    count = cursor.fetchone()[0]
                    total_records += count
                    logger.info(f"[DATA] Table {table}: {count} records")
                
                if oldest_date and newest_date:
                    logger.info(f"[DATA] Data range: {oldest_date} to {newest_date} ({total_days} days)")
                    logger.info(f"[DATA] Retention policy: Maintaining max {self.max_retention_days} days")
                
                logger.info(f"[DATA] Database verification complete. Total records: {total_records}")
                return True
                
        except Exception as e:
            logger.error(f"[DATA] Database verification failed: {e}")
            return False

    def get_database_stats(self) -> Dict:
        """Get comprehensive database statistics including retention info"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                stats = {
                    "retention_policy": {
                        "max_days": self.max_retention_days,
                        "policy_active": True,
                        "policy_type": "trading_days"
                    },
                    "tables": {},
                    "date_range": {},
                    "total_records": 0
                }
                
                # Get table stats
                tables = ["daily_picks", "minute_ohlcv", "daily_summaries", "trade_outcomes"]
                for table in tables:
                    cursor.execute(f"SELECT COUNT(*) FROM {table}")
                    count = cursor.fetchone()[0]
                    stats["tables"][table] = count
                    stats["total_records"] += count
                
                # Get date range
                oldest_date, newest_date, total_trading_days = self.get_data_date_range()
                stats["date_range"] = {
                    "oldest_date": oldest_date,
                    "newest_date": newest_date,
                    "total_trading_days": total_trading_days,
                    "within_retention_limit": total_trading_days <= self.max_retention_days
                }
                
                # Database file size
                if os.path.exists(self.db_path):
                    size_bytes = os.path.getsize(self.db_path)
                    size_mb = round(size_bytes / (1024 * 1024), 2)
                    stats["database_size"] = {
                        "bytes": size_bytes,
                        "mb": size_mb
                    }
                
                return stats
                
        except Exception as e:
            logger.error(f"[DATA] Error getting database stats: {e}")
            return {"error": str(e)}

def run_end_of_day_collection():
    """Run END-OF-DAY ONLY data collection with retention policy"""
    collector = MarketDataCollector()
    
    # Get today's date in Eastern time
    now_et = datetime.now(EDT)
    if now_et.hour >= 17:  # After 5 PM, collect for today
        date = now_et.strftime("%Y-%m-%d")
    else:  # Before market close, collect for yesterday
        date = (now_et - timedelta(days=1)).strftime("%Y-%m-%d")
    
    logger.info(f"[DATA] Running END-OF-DAY collection for {date}")
    logger.info(f"[DATA] Retention policy: Max {collector.max_retention_days} trading days of data")
    
    collector.collect_end_of_day_data(date)
    
    # Show final stats
    stats = collector.get_database_stats()
    if "date_range" in stats and stats["date_range"]["oldest_date"]:
        date_info = stats["date_range"]
        size_info = stats.get("database_size", {})
        logger.info(f"[DATA] Collection completed - Database spans {date_info['total_trading_days']} trading days "
                   f"({date_info['oldest_date']} to {date_info['newest_date']})")
        if size_info:
            logger.info(f"[DATA] Database size: {size_info['mb']} MB")
        
        # Check retention compliance
        if date_info["total_trading_days"] > collector.max_retention_days:
            logger.warning(f"[DATA] ⚠️ Database exceeds {collector.max_retention_days}-trading-day limit - "
                          f"cleanup may be needed")
        else:
            logger.info(f"[DATA] ✅ Retention policy compliant: {date_info['total_trading_days']}/{collector.max_retention_days} trading days")
    
    logger.info(f"[DATA] Collection completed - picks with minute data for optimizer")

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="END-OF-DAY Market Data Collector with 250-trading-day retention")
    parser.add_argument("--mode", choices=["eod", "test", "cleanup", "stats"], default="eod",
                       help="Collection mode: eod (end of day), test, cleanup (force cleanup), stats (show database stats)")
    parser.add_argument("--days", type=int, default=None,
                       help="For cleanup mode: specify retention trading days (default: use configured limit)")
    
    args = parser.parse_args()
    
    if args.mode == "eod":
        logger.info("[DATA] Running END-OF-DAY collection with retention policy...")
        run_end_of_day_collection()
        
    elif args.mode == "test":
        collector = MarketDataCollector()
        logger.info(f"[DATA] Database initialized - ready for END-OF-DAY data collection!")
        logger.info(f"[DATA] Retention policy: Max {collector.max_retention_days} trading days")
        
        # Show current stats
        stats = collector.get_database_stats()
        if "date_range" in stats:
            date_info = stats["date_range"]
            if date_info["oldest_date"]:
                logger.info(f"[DATA] Current data: {date_info['total_trading_days']} trading days "
                           f"({date_info['oldest_date']} to {date_info['newest_date']})")
            else:
                logger.info("[DATA] No data in database yet")
                
    elif args.mode == "cleanup":
        collector = MarketDataCollector()
        
        # Override retention days if specified
        if args.days:
            collector.max_retention_days = args.days
            logger.info(f"[DATA] Using custom retention period: {args.days} trading days")
        
        # Get current stats
        stats = collector.get_database_stats()
        if "date_range" in stats and stats["date_range"]["oldest_date"]:
            date_info = stats["date_range"]
            logger.info(f"[DATA] Before cleanup: {date_info['total_trading_days']} trading days "
                       f"({date_info['oldest_date']} to {date_info['newest_date']})")
            
            # Force cleanup based on today's date
            today = datetime.now(EDT).strftime("%Y-%m-%d")
            collector.enforce_retention_policy(today)
            
            # Show after stats
            new_stats = collector.get_database_stats()
            if "date_range" in new_stats and new_stats["date_range"]["oldest_date"]:
                new_date_info = new_stats["date_range"]
                logger.info(f"[DATA] After cleanup: {new_date_info['total_trading_days']} trading days "
                           f"({new_date_info['oldest_date']} to {new_date_info['newest_date']})")
            else:
                logger.info("[DATA] No data remaining after cleanup")
        else:
            logger.info("[DATA] No data to clean up")
            
    elif args.mode == "stats":
        collector = MarketDataCollector()
        stats = collector.get_database_stats()
        
        print("\n=== DATABASE STATISTICS ===")
        print(f"Database Path: {collector.db_path}")
        print(f"Retention Policy: {stats['retention_policy']['max_days']} trading days")
        
        if "database_size" in stats:
            size_info = stats["database_size"]
            print(f"Database Size: {size_info['mb']} MB ({size_info['bytes']:,} bytes)")
        
        if "date_range" in stats and stats["date_range"]["oldest_date"]:
            date_info = stats["date_range"]
            print(f"Date Range: {date_info['oldest_date']} to {date_info['newest_date']}")
            print(f"Trading Days: {date_info['total_trading_days']}")
            print(f"Within Retention Limit: {'✅ Yes' if date_info['within_retention_limit'] else '❌ No'}")
        else:
            print("Date Range: No data")
        
        print("\nTable Record Counts:")
        for table, count in stats.get("tables", {}).items():
            print(f"  {table}: {count:,} records")
        
        print(f"\nTotal Records: {stats.get('total_records', 0):,}")
        
        # Calculate estimated cleanup if over limit
        if "date_range" in stats and stats["date_range"]["total_trading_days"] > collector.max_retention_days:
            excess_days = stats["date_range"]["total_trading_days"] - collector.max_retention_days
            print(f"\n⚠️  Database exceeds retention limit by {excess_days} trading days")
            print(f"Run with --mode cleanup to remove old data")