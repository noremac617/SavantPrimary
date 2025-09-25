import os
import sys
import json
import time
import requests
import pandas as pd
import schedule
import logging
from loguru import logger
from io import StringIO
from datetime import datetime, timedelta, timezone
from logging import StreamHandler
from flask import Flask, render_template, redirect, request, url_for, session, flash, jsonify, g
from threading import Thread, Lock
from dotenv import load_dotenv, set_key
import exchange_calendars as ecals
from functools import lru_cache
from concurrent.futures import ThreadPoolExecutor
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from flask_compress import Compress
import threading
from market_data_collector import MarketDataCollector, run_end_of_day_collection
from execution_optimizer import ExecutionOptimizer, run_weekly_optimization_job
from trade_plan_fetcher import get_trade_plan_from_google_sheet, convert_site_data
from zoneinfo import ZoneInfo
from schwab_api import refresh_tokens, get_open_positions, get_token_status, make_schwab_request, get_account_hash, proactive_token_maintenance
from schwab_auth import construct_headers_and_payload, retrieve_tokens
from config import get_allocation, set_allocation, get_demo_mode, set_demo_mode
from place_order import main as place_orders_main
from close_positions import main as close_positions_main

load_dotenv()

# Ensure log directory exists
os.makedirs("logs", exist_ok=True)

# Timezones
UTC = timezone.utc
EDT = ZoneInfo("America/New_York")

# Patch Loguru's timestamp method BEFORE adding any logger
logger._core.timestamp = lambda: datetime.now(UTC).astimezone(EDT)

# Remove default logger
logger.remove()

# File logger logs/app.log with EDT timestamps
logger.add(
    "logs/app.log",
    rotation="1 MB",
    retention="10 days",
    enqueue=True,
    format="{time:YYYY-MM-DD HH:mm:ss z} | {level} | {message}"
)

#  Console logger also uses EDT timestamps
logger.add(
    sys.stdout,
    level="INFO",
    colorize=True,
    enqueue=True,
    format="<green>{time:YYYY-MM-DD HH:mm:ss z}</green> | <level>{message}</level>",
    backtrace=True,
    diagnose=True
)

# Suppress noisy third-party logs
logging.getLogger("yfinance").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

log_capture_string = StringIO()
stream_handler = StreamHandler(log_capture_string)
stream_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s | %(levelname)-8s | %(message)s')
stream_handler.setFormatter(formatter)
logging.getLogger().addHandler(stream_handler)

class InterceptHandler(logging.Handler):
    def emit(self, record):
        logger.opt(depth=6, exception=record.exc_info).log(record.levelname, record.getMessage())

logging.basicConfig(handlers=[InterceptHandler()], level=0, force=True)

if os.environ.get('TERM_PROGRAM') == 'vscode':
    logging.getLogger('werkzeug').setLevel(logging.ERROR)

# --- Optimized HTTP Session with Connection Pooling ---
def create_optimized_session():
    session = requests.Session()
    
    # Retry strategy
    retry = Retry(
        total=3,
        read=3,
        connect=3,
        backoff_factor=0.3,
        status_forcelist=(500, 502, 504)
    )
    
    # HTTP adapter with connection pooling
    adapter = HTTPAdapter(
        pool_connections=10,
        pool_maxsize=20,
        max_retries=retry
    )
    
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    
    return session

optimized_session = create_optimized_session()

# --- Data Caching Layer ---
class DataCache:
    def __init__(self):
        self.trades_cache = None
        self.trades_cache_time = None
        self.positions_cache = None
        self.positions_cache_time = None
        self.trade_plan_cache = None
        self.trade_plan_cache_time = None
        self.cache_duration = timedelta(minutes=1)  # Cache for 1 minute
        self.lock = Lock()
    
    def get_trades_data(self):
        now = datetime.now()
        with self.lock:
            if (self.trades_cache is None or 
                self.trades_cache_time is None or 
                now - self.trades_cache_time > self.cache_duration):
                
                try:
                    self.trades_cache = pd.read_csv("./data/trades.csv")
                    self.trades_cache["date"] = pd.to_datetime(self.trades_cache["date"], errors="coerce")
                    self.trades_cache = self.trades_cache.dropna(subset=["date"])
                    self.trades_cache_time = now
                    logger.debug("Trades data refreshed from CSV")
                except Exception as e:
                    logger.error(f"Failed to load trades.csv: {e}")
                    self.trades_cache = pd.DataFrame(columns=["date", "symbol", "action", "entry", "exit", "pnl_pct"])
            
            return self.trades_cache.copy()
    
    def get_positions_data(self):
        now = datetime.now()
        with self.lock:
            if (self.positions_cache is None or 
                self.positions_cache_time is None or 
                now - self.positions_cache_time > self.cache_duration):
                
                try:
                    if is_authenticated():
                        self.positions_cache = get_open_positions()
                        self.positions_cache_time = now
                        logger.debug("Positions data refreshed from Schwab API")
                    else:
                        self.positions_cache = []
                except Exception as e:
                    logger.error(f"Failed to get positions: {e}")
                    self.positions_cache = []
            
            return self.positions_cache.copy()
    
    def get_trade_plan_data(self):  # ✅ NOW CORRECTLY A CLASS METHOD
        now = datetime.now()
        with self.lock:
            if (self.trade_plan_cache is None or 
                self.trade_plan_cache_time is None or 
                now - self.trade_plan_cache_time > self.cache_duration):
                
                try:
                    json_path = os.path.join(os.path.dirname(__file__), "./data/trade_plan.json")
                    with open(json_path, "r") as f:
                        self.trade_plan_cache = json.load(f)
                        self.trade_plan_cache_time = now
                        logger.debug("Trade plan data refreshed from JSON")
                except Exception as e:
                    logger.error(f"Failed to load trade_plan.json: {e}")
                    self.trade_plan_cache = {"buy": [], "short": []}
            
            return self.trade_plan_cache.copy()
    
    def invalidate_trades(self):
        """Force refresh of trades data on next request"""
        with self.lock:
            self.trades_cache_time = None
    
    def invalidate_positions(self):
        """Force refresh of positions data on next request"""
        with self.lock:
            self.positions_cache_time = None
    
    def invalidate_trade_plan(self):  # ✅ ALSO ADD THIS METHOD
        """Force refresh of trade plan data on next request"""
        with self.lock:
            self.trade_plan_cache_time = None

# Global cache instance
data_cache = DataCache()

# --- Background Task Executor ---
background_executor = ThreadPoolExecutor(max_workers=3)

# --- Optimized Yahoo Price Fetching ---
@lru_cache(maxsize=100, typed=True)
def cached_yahoo_price(symbol, cache_key):
    """Cache Yahoo prices with time-based cache key"""
    return safe_get_yahoo_price(symbol)

def get_cached_yahoo_price(symbol):
    # Create cache key that changes every minute
    cache_key = datetime.now().strftime("%Y%m%d%H%M")
    return cached_yahoo_price(symbol, cache_key)

def batch_refresh_prices(symbols):
    """Fetch multiple prices concurrently"""
    with ThreadPoolExecutor(max_workers=5) as executor:
        future_to_symbol = {
            executor.submit(get_cached_yahoo_price, symbol): symbol 
            for symbol in symbols
        }
        
        results = {}
        for future in future_to_symbol:
            symbol = future_to_symbol[future]
            try:
                price = future.result(timeout=10)
                results[symbol] = price
            except Exception as e:
                logger.error(f"Error fetching price for {symbol}: {e}")
                results[symbol] = None
        
        return results

# --- Global Scheduler Variables ---
scheduler_thread = None
scheduler_running_flag = False
scheduler_lock = Lock()
scheduler_initialized = False

_schedule_lock = Lock()

next_refresh_time = None

# --- Flask App Setup ---
app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET", "dev")
app.permanent_session_lifetime = timedelta(days=1)

# Add compression
Compress(app)

@app.before_request
def make_session_permanent():
    session.permanent = True
    # Start timer for slow request monitoring
    g.start_time = time.time()

@app.after_request
def after_request(response):
    # Log slow requests
    if hasattr(g, 'start_time'):
        duration = time.time() - g.start_time
        if duration > 2.0:
            logger.warning(f"Slow request: {request.endpoint} took {duration:.2f}s")
    
    # Cache static files
    if request.endpoint == 'static':
        response.cache_control.max_age = 3600  # Cache for 1 hour
    return response

USERNAME = os.getenv("DASHBOARD_USER", "admin")
PASSWORD = os.getenv("DASHBOARD_PASS", "password")

sheet_cache = {}
market_close_cache = {
    "date": None,
    "close_time": None
}

import shutil

INITIAL_TRADES_PATH = "./data/trades.csv"
PERSISTENT_TRADES_PATH = "./data/trades.csv"

if not os.path.exists(PERSISTENT_TRADES_PATH) and os.path.exists(INITIAL_TRADES_PATH):
    print("Copying initial trades.csv to ./data/...")
    shutil.copy(INITIAL_TRADES_PATH, PERSISTENT_TRADES_PATH)

last_refresh_path = "./data/last_refresh.txt"
if not os.path.exists(last_refresh_path):
    with open("last_refresh.txt", "w") as f:
        f.write(datetime.now(EDT).isoformat())

def is_authenticated():
    """Check if token file exists - use same logic as schwab_api.py"""
    # Import here to avoid circular imports
    from schwab_api import TOKEN_FILE
    return os.path.exists(TOKEN_FILE)

def auto_fill_account_id_if_blank():
    """Auto-fill ACCOUNT_ID in .env if it's missing or empty"""
    current_account_id = os.getenv("ACCOUNT_ID")
    
    # Only proceed if ACCOUNT_ID is missing, empty, or just whitespace
    if current_account_id and current_account_id.strip():
        logger.debug(f"ACCOUNT_ID already set: {current_account_id}")
        return
    
    # Check if we have valid tokens before attempting API call
    if not os.path.exists("./data/token.json"):
        logger.debug("No token file found - ACCOUNT_ID will be auto-filled after authentication")
        return
        
    try:
        logger.info("ACCOUNT_ID missing - attempting to fetch...")
        response = make_schwab_request("GET", "/trader/v1/accounts")
        
        if not isinstance(response, list) or len(response) == 0:
            logger.error(f"Invalid account response: {response}")
            return
            
        acct_num = response[0].get("securitiesAccount", {}).get("accountNumber")
        if acct_num:
            hashed = get_account_hash(acct_num)
            if hashed:
                # Save to .env file
                set_key(".env", "ACCOUNT_ID", hashed)
                logger.info(f"ACCOUNT_ID saved to .env: {hashed}")
                
                # CRITICAL: Update the current environment so it takes effect immediately
                os.environ["ACCOUNT_ID"] = hashed
                logger.success(f"✅ ACCOUNT_ID updated in current session: {hashed}")
            else:
                logger.error("Could not get hashed account ID")
        else:
            logger.error("No account number found in response")
            
    except Exception as e:
        logger.warning(f"Unable to auto-fill ACCOUNT_ID: {e}")
        import traceback
        logger.debug(traceback.format_exc())
        
def save_trade_plan_to_cache(plan_data: dict):
    global sheet_cache
    sheet_cache.clear()
    sheet_cache.update(plan_data)
    try:
        json_path = os.path.join(os.path.dirname(__file__), "./data/trade_plan.json")
        with open(json_path, "w") as f:
            json.dump(plan_data, f, indent=2)
        logger.info("trade_plan.json successfully updated.")
        # Invalidate the cache so it gets refreshed on next request
        data_cache.invalidate_trade_plan()
    except Exception as e:
        logger.error(f"Failed to write trade_plan.json: {e}")

def load_trade_plan_from_cache() -> dict:
    global sheet_cache
    if sheet_cache:
        return sheet_cache
    try:
        json_path = os.path.join(os.path.dirname(__file__), "./data/trade_plan.json")
        with open(json_path, "r") as f:
            plan_data = json.load(f)
            sheet_cache = plan_data
            return plan_data
    except Exception as e:
        logger.warning(f"Failed to load trade plan from file: {e}")
        return {"buy": [], "short": []}
    
def get_database_status_for_template():
    """Get database status for template rendering"""
    try:
        response = get_database_status()
        return response.get_json()
    except:
        return {"connected": False}

def get_collection_history_for_template():
    """Get collection history for template"""
    try:
        response = get_collection_history()
        return response.get_json()
    except:
        return []

def get_market_close_time():
    today_str = datetime.now(UTC).astimezone(EDT).strftime("%Y-%m-%d")
    if market_close_cache["date"] == today_str:
        return market_close_cache["close_time"]

    try:
        nyse = ecals.get_calendar("XNYS")
        now = pd.Timestamp.now(tz=UTC)
        today = now.normalize().tz_localize(None)

        schedule_range = nyse.schedule.loc[today - pd.Timedelta(days=7):today + pd.Timedelta(days=7)]

        if today not in schedule_range.index:
            raise ValueError(f"{today} not in NYSE schedule")

        row = schedule_range.loc[today]
        if isinstance(row, pd.Series) and "close" in row:
            close_time = row["close"].tz_convert(EDT).to_pydatetime()
            logger.info(f"Market close: {close_time}")

            # Cache the result
            market_close_cache["date"] = today_str
            market_close_cache["close_time"] = close_time
            return close_time

        else:
            raise ValueError(f"'close' not found in row: {row}")

    except Exception as e:
        logger.warning(f"exchange-calendars fallback: {e}")
        fallback = datetime.now(UTC).astimezone(EDT).replace(hour=16, minute=0, second=0, microsecond=0)
        market_close_cache["date"] = today_str
        market_close_cache["close_time"] = fallback
        return fallback

def is_after_market_close_cutoff():
    eastern = datetime.now(UTC).astimezone(EDT)
    now = datetime.now(UTC).astimezone(EDT)
    cutoff = now.replace(hour=19, minute=55, second=0, microsecond=0)
    return now >= cutoff

def is_market_open_now():
    nyse = ecals.get_calendar("XNYS")
    now_utc = datetime.now(ZoneInfo("UTC"))
    return nyse.is_open_on_minute(now_utc)

def refresh_trade_plan():
    try:
        logger.info("Refreshing trade plan from Google Sheets...")
        raw_data = get_trade_plan_from_google_sheet()
        
        # NEW: Check for the updated format (buy/short keys) instead of old format (direction/tickers)
        if raw_data.get("buy") or raw_data.get("short"):
            # Data is already in the correct format, use it directly
            plan_data = raw_data
            
            # DEBUG: Log what we're about to save
            logger.info(f"About to save plan_data: {plan_data}")
            
            save_trade_plan_to_cache(plan_data)
            return True, "Trade plan successfully refreshed from Google Sheets."
        
        # FALLBACK: Check for old format for backward compatibility
        elif raw_data.get("direction") and raw_data.get("tickers"):
            # Convert old format to new format
            plan_data = convert_site_data(raw_data)
            
            # DEBUG: Log what we're about to save
            logger.info(f"About to save converted plan_data: {plan_data}")
            
            save_trade_plan_to_cache(plan_data)
            return True, "Trade plan successfully refreshed from Google Sheets (converted from old format)."
        
        else:
            # No valid data found
            logger.warning(f"No valid trade plan found. Raw data: {raw_data}")
            return False, "No valid trade plan found in Google Sheets for today."
            
    except Exception as e:
        logger.error(f"Error refreshing trade plan: {e}")
        return False, f"Error refreshing trade plan: {str(e)}"

class TimeoutException(Exception):
    pass

def timeout_handler(signum, frame):
    raise TimeoutException()

def safe_get_yahoo_price(symbol):
    import yfinance as yf
    from concurrent.futures import ThreadPoolExecutor, TimeoutError

    def fetch_price():
        data = yf.Ticker(symbol).history(period="1d")
        if not data.empty:
            return float(data["Close"].iloc[-1])
        return None

    try:
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(fetch_price)
            return future.result(timeout=10)  # 10s timeout
    except TimeoutError:
        logger.error(f"[Yahoo] Timeout fetching price for {symbol}")
        return None
    except Exception as e:
        logger.error(f"[Yahoo] Error fetching price for {symbol}: {e}")
        return None

def refresh_pl_logic():
    trades_path = "./data/trades.csv"
    if not os.path.exists(trades_path):
        return False, "No trades.csv found."

    if is_after_market_close_cutoff():
        return False, "Market closed skipping P/L refresh."

    df = pd.read_csv(trades_path, parse_dates=["date"])
    today_str = datetime.now(UTC).astimezone(EDT).strftime("%Y-%m-%d")

    columns = ["date", "symbol", "action", "entry", "current", "exit", "pnl_pct"]
    for col in columns:
        if col not in df.columns:
            df[col] = ""
    df = df[columns]

    # Get unique symbols that need price updates
    today_df = df[df["date"].dt.strftime("%Y-%m-%d") == today_str]
    open_trades = today_df[pd.isna(today_df["exit"])]
    
    if open_trades.empty:
        return False, "No open trades to refresh."
    
    symbols_to_update = open_trades["symbol"].unique().tolist()
    
    # Batch fetch prices
    price_results = batch_refresh_prices(symbols_to_update)
    
    modified = False
    for i, row in df.iterrows():
        row_date = pd.to_datetime(row["date"]).strftime("%Y-%m-%d")
        if row_date == today_str and pd.isna(row["exit"]):
            symbol = row["symbol"]
            entry = float(row["entry"])
            current = price_results.get(symbol)

            if current is None:
                continue

            pnl = ((current - entry) / entry) * 100 if row["action"].lower() == "buy" else ((entry - current) / entry) * 100
            df.at[i, "current"] = round(current, 2)
            df.at[i, "pnl_pct"] = round(pnl, 2)
            modified = True

    if modified:
        temp_path = trades_path + ".tmp"
        df.to_csv(temp_path, index=False)
        os.replace(temp_path, trades_path)
        # Invalidate cache to force refresh
        data_cache.invalidate_trades()
        return True, "P/L refreshed."
    else:
        return False, "No open trades to refresh."

def async_refresh_pl():
    """Non-blocking P/L refresh"""
    def refresh_task():
        try:
            success, msg = refresh_pl_logic()
            logger.info(f"Background P/L refresh: {msg}")
            with open("./data/last_refresh.txt", "w") as f:
                f.write(datetime.now(EDT).isoformat())
        except Exception as e:
            logger.error(f"Background P/L refresh failed: {e}")
    
    background_executor.submit(refresh_task)

# --- FIXED SCHEDULER FUNCTIONS ---

def run_place_order():
    """Wrapper to prevent concurrent executions"""
    if hasattr(run_place_order, '_running') and run_place_order._running:
        logger.warning("[SCHEDULER] run_place_order already running, skipping duplicate execution")
        return
    
    run_place_order._running = True
    try:
        refresh_tokens()
        logger.info("[SCHEDULER] Placing trades...")
        place_orders_main()
        data_cache.invalidate_trades()
        data_cache.invalidate_positions()
    except Exception as e:
        logger.error(f"[SCHEDULER] Place order failed: {e}")
    finally:
        run_place_order._running = False

def run_close_positions():
    """Wrapper to prevent concurrent executions"""
    if hasattr(run_close_positions, '_running') and run_close_positions._running:
        logger.warning("[SCHEDULER] run_close_positions already running, skipping duplicate execution")
        return
    
    run_close_positions._running = True
    try:
        refresh_tokens()
        logger.info("[SCHEDULER] Closing positions...")
        close_positions_main()
        data_cache.invalidate_trades()
        data_cache.invalidate_positions()
    except Exception as e:
        logger.error(f"[SCHEDULER] Close positions failed: {e}")
    finally:
        run_close_positions._running = False

def scheduled_refresh():
    """Token refresh with duplicate prevention"""
    if hasattr(scheduled_refresh, '_running') and scheduled_refresh._running:
        logger.warning("[SCHEDULER] scheduled_refresh already running, skipping duplicate execution")
        return
    
    scheduled_refresh._running = True
    try:
        global next_refresh_time
        success = refresh_tokens()
        if success:
            logger.info("[SCHEDULER] Token refresh successful.")
        else:
            logger.warning("[SCHEDULER] Token refresh failed.")
        next_refresh_time = datetime.now(EDT) + timedelta(hours=6)
    except Exception as e:
        logger.error(f"[SCHEDULER] Token refresh failed: {e}")
    finally:
        scheduled_refresh._running = False

def scheduled_refresh_trade_plan():
    """Trade plan refresh with duplicate prevention"""
    if hasattr(scheduled_refresh_trade_plan, '_running') and scheduled_refresh_trade_plan._running:
        logger.warning("[SCHEDULER] scheduled_refresh_trade_plan already running, skipping duplicate execution")
        return
    
    scheduled_refresh_trade_plan._running = True
    try:
        success, msg = refresh_trade_plan()
        logger.info(f"[SCHEDULER] Trade plan refresh: {msg}")
    except Exception as e:
        logger.error(f"[SCHEDULER] Trade plan refresh failed: {e}")
    finally:
        scheduled_refresh_trade_plan._running = False

def schedule_close():
    try:
        # Clear existing close_positions jobs first
        schedule.clear('close_positions')
        
        market_close_et = get_market_close_time()
        close_time_et = market_close_et - timedelta(minutes=5)
        close_time_utc = close_time_et.astimezone(ZoneInfo("UTC"))
        time_str_utc = close_time_utc.strftime("%H:%M")

        def close_positions_market_close():
            """Close positions 5 minutes before market close"""
            weekday_only(run_close_positions)()
        
        close_positions_market_close.__name__ = "close_positions_market_close"

        job = schedule.every().day.at(time_str_utc).do(close_positions_market_close)
        job.tag('close_positions')
        
        logger.info(f"[SCHEDULER] Scheduled close_positions for {close_time_et.strftime('%H:%M')} EDT / {time_str_utc} UTC")
        
    except Exception as e:
        # Single fallback block
        schedule.clear('close_positions')
        fallback_time = "19:55"
        
        def fallback_close_positions():
            """Fallback close positions function"""
            weekday_only(run_close_positions)()
        
        fallback_close_positions.__name__ = "close_positions_fallback"
        
        job = schedule.every().day.at(fallback_time).do(fallback_close_positions)
        job.tag('close_positions')
        
        logger.warning(f"[SCHEDULER] Fallback close time scheduled for {fallback_time} UTC. Error: {e}")
        
    except Exception as e:
        # Fallback scheduling - UNCHANGED
        schedule.clear('close_positions')
        fallback_time = "19:55"  # 3:55pm EDT = 7:55pm UTC
        
        def fallback_close_positions():
            """Fallback close positions function"""
            weekday_only(run_close_positions)()  # <-- IDENTICAL execution as before
        
        fallback_close_positions.__name__ = "close_positions_fallback"
        
        job = schedule.every().day.at(fallback_time).do(fallback_close_positions)
        job.tag('close_positions')
        
        logger.warning(f"[SCHEDULER] Fallback close time scheduled for {fallback_time} UTC. Error: {e}")

        
    except Exception as e:
        # Fallback scheduling - UNCHANGED
        schedule.clear('close_positions')
        fallback_time = "19:55"  # 3:55pm EDT = 7:55pm UTC
        
        def fallback_close_positions():
            """Fallback close positions function"""
            weekday_only(run_close_positions)()  # <-- IDENTICAL execution as before
        
        fallback_close_positions.__name__ = "close_positions_fallback"
        
        job = schedule.every().day.at(fallback_time).do(fallback_close_positions)
        job.tag('close_positions')
        
        logger.warning(f"[SCHEDULER] Fallback close time scheduled for {fallback_time} UTC. Error: {e}")

        
    except Exception as e:
        # Fallback scheduling - UNCHANGED
        schedule.clear('close_positions')
        fallback_time = "19:55"  # 3:55pm EDT = 7:55pm UTC
        
        def fallback_close_positions():
            """Fallback close positions function"""
            weekday_only(run_close_positions)()  # <-- IDENTICAL execution as before
        
        fallback_close_positions.__name__ = "close_positions_fallback"
        
        job = schedule.every().day.at(fallback_time).do(fallback_close_positions)
        job.tag('close_positions')
        
        logger.warning(f"[SCHEDULER] Fallback close time scheduled for {fallback_time} UTC. Error: {e}")

        
    except Exception as e:
        # Fallback scheduling
        schedule.clear('close_positions')
        fallback_time = "19:55"  # 3:55pm EDT = 7:55pm UTC
        
        def fallback_close_positions():
            """Fallback close positions function"""
            weekday_only(run_close_positions)()
        
        fallback_close_positions.__name__ = "close_positions_fallback"
        
        job = schedule.every().day.at(fallback_time).do(fallback_close_positions)
        job.tag('close_positions')
        
        logger.warning(f"[SCHEDULER] Fallback close time scheduled for {fallback_time} UTC. Error: {e}")

def is_weekday():
    today = datetime.now(UTC).astimezone(EDT).weekday()
    return today < 5  # True for Mon-Fri

def get_morning_data_for_picks():
    """Get morning data for today's picks only (no market context)"""
    try:
        collector = MarketDataCollector()
        
        # Get picks data
        plan_data = data_cache.get_trade_plan_data()
        symbols = plan_data.get("buy", []) + plan_data.get("short", [])
        
        if symbols:
            morning_data = collector.get_morning_data(symbols)
            logger.info(f"[DATA] Retrieved morning data for {len(symbols)} picks")
        else:
            logger.info("[DATA] No picks found for morning data collection")
        
        # No more market context collection - removed
        
    except Exception as e:
        logger.error(f"[DATA] Error in morning data collection: {e}")

def health_check():
    logger.info("[SCHEDULER] Health check - Scheduler is running.")

def weekday_only(job_func):
    """Simple weekday wrapper"""
    def wrapper():
        current_day = datetime.now(EDT).weekday()
        if current_day < 5:  # Monday=0, Friday=4
            job_func()
        else:
            logger.info(f"[SCHEDULER] Skipping {job_func.__name__} - weekend")
    
    wrapper.__name__ = job_func.__name__
    wrapper.__wrapped__ = job_func
    return wrapper

def schedule_jobs():
    """Schedule all daily jobs - SIMPLE AND CLEAN"""
    logger.info("[SCHEDULER] Scheduling daily jobs...")
    
    # Clear everything first
    schedule.clear()
    
    # === TRADING JOBS ===
    schedule.every().day.at("13:00").do(weekday_only(schedule_close))              # 9:00 AM EDT
    schedule.every().day.at("13:30").do(weekday_only(run_place_order))             # 9:30 AM EDT  
    
    # === TOKEN MAINTENANCE ===
    schedule.every().day.at("12:00").do(proactive_token_maintenance)              # 8:00 AM EDT
    schedule.every().day.at("13:15").do(weekday_only(scheduled_refresh))          # 9:15 AM EDT
    schedule.every().day.at("19:45").do(weekday_only(scheduled_refresh))          # 3:45 PM EDT
    
    # === TRADE PLAN ===
    schedule.every().day.at("13:20").do(weekday_only(scheduled_refresh_trade_plan)) # 9:20 AM EDT
    
    # === DATA COLLECTION ===
    schedule.every().day.at("21:30").do(weekday_only(run_end_of_day_collection))  # 5:30 PM EDT
    
    # === HEALTH CHECK ===
    schedule.every().day.at("03:59").do(health_check)                             # 11:59 PM EDT

    # Weekly optimization (runs Sunday at 6 AM EDT)
    schedule.every().sunday.at("10:00").do(weekday_only(run_weekly_optimization_job))  # 6:00 AM EDT
    
    logger.info(f"[SCHEDULER] Scheduled {len(schedule.jobs)} jobs")


def scheduler_loop():
    """Simple scheduler loop - just run pending jobs"""
    global scheduler_running_flag
    
    logger.info("[SCHEDULER] Scheduler loop started")
    
    while scheduler_running_flag:
        try:
            # Run any pending jobs
            schedule.run_pending()
            
            # Sleep for 1 second
            time.sleep(1)
            
        except Exception as e:
            logger.error(f"[SCHEDULER] Error in scheduler loop: {e}")
            time.sleep(5)  # Wait a bit longer on error
    
    logger.info("[SCHEDULER] Scheduler loop stopped")

def start_scheduler_once():
    """Start the scheduler - SIMPLE"""
    global scheduler_thread, scheduler_running_flag
    
    with scheduler_lock:
        if scheduler_running_flag:
            logger.warning("[SCHEDULER] Scheduler already running")
            return True
        
        # Schedule all jobs
        schedule_jobs()
        
        # Start the scheduler thread
        scheduler_running_flag = True
        scheduler_thread = threading.Thread(target=scheduler_loop, daemon=True, name="SchedulerThread")
        scheduler_thread.start()
        
        logger.info("[SCHEDULER] Scheduler started successfully")
        return True

def stop_scheduler():
    """Properly stop the scheduler without blocking web requests"""
    global scheduler_running_flag, scheduler_thread
    
    # Use very short timeout to avoid blocking
    if not scheduler_lock.acquire(timeout=0.5):
        logger.warning("[SCHEDULER] Could not acquire lock to stop scheduler")
        return False
    
    try:
        if not scheduler_running_flag:
            logger.info("[SCHEDULER] Scheduler already stopped")
            return True
        
        logger.info("[SCHEDULER] Stopping scheduler...")
        scheduler_running_flag = False
        schedule.clear()
        
        # Don't wait for thread to join - just signal it to stop
        if scheduler_thread and scheduler_thread.is_alive():
            logger.info("[SCHEDULER] Scheduler thread will stop gracefully")
        
        logger.info("[SCHEDULER] Scheduler stop signal sent.")
        return True
        
    except Exception as e:
        logger.error(f"[SCHEDULER] Error stopping scheduler: {e}")
        return False
    finally:
        scheduler_lock.release()

def initialize_scheduler_once():
    """Initialize scheduler only once per application lifecycle"""
    global scheduler_initialized
    
    if scheduler_initialized:
        logger.info("[SCHEDULER] Already initialized, skipping")
        return
    
    # Only auto-start in production or when explicitly requested
    if os.environ.get("RENDER") or os.environ.get("RUN_MAIN") == "true":
        logger.info("[SCHEDULER] Auto-starting scheduler for production...")
        start_scheduler_once()
    else:
        logger.info("[SCHEDULER] Development mode - scheduler not auto-started")

# --- FLASK ROUTES ---

@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        if request.form["username"] == USERNAME and request.form["password"] == PASSWORD:
            session["logged_in"] = True
            return redirect(url_for("index"))
        return render_template("login.html", error="Invalid credentials")
    return render_template("login.html")

@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))

@app.route("/authorize")
def authorize():
    app_key = os.getenv("APP_KEY")
    auth_url = f"https://api.schwabapi.com/v1/oauth/authorize?client_id={app_key}&redirect_uri=https://127.0.0.1"
    return render_template("authorize.html", auth_url=auth_url)

@app.route("/submit_auth_url", methods=["POST"])
def submit_auth_url():
    returned_url = request.form["auth_url"]
    app_key = os.getenv("APP_KEY")
    app_secret = os.getenv("APP_SECRET")

    try:
        headers, payload = construct_headers_and_payload(returned_url, app_key, app_secret)
        tokens = retrieve_tokens(headers, payload)
        if not tokens or "access_token" not in tokens:
            flash("Token exchange failed. Try again manually.", "danger")
            return redirect(url_for("index"))

        # Add the original authentication time for tracking the 7-day window
        tokens["issued_at"] = time.time()
        tokens["original_auth_time"] = time.time()  # Track when user first authenticated
        
        with open("./data/token.json", "w") as f:
            json.dump(tokens, f, indent=4)

        logger.success("✅ Authentication successful - tokens saved")
        
        # NOW: Auto-fill the ACCOUNT_ID immediately after successful authentication
        try:
            logger.info("🔄 Auto-filling ACCOUNT_ID after successful authentication...")
            auto_fill_account_id_if_blank()
        except Exception as e:
            logger.error(f"Failed to auto-fill ACCOUNT_ID after authentication: {e}")
            # Don't fail the whole authentication process for this
        
        flash("Authentication successful! Account ID has been configured.", "success")
        
    except Exception as e:
        logger.error(f"OAuth flow error: {e}")
        flash("OAuth process failed. Check logs.", "danger")

    return redirect(url_for("index"))

def get_daily_avg(df):
    if df.empty:
        return 0.0
    daily_means = df.groupby(df["date"].dt.date)["pnl_pct"].mean()
    return round(daily_means.mean(), 2)

def calculate_performance(df):
    eastern = ZoneInfo("America/New_York")
    today = datetime.now(UTC).astimezone(EDT).date()
    this_week = today - timedelta(days=today.weekday())  # Monday
    this_month = today.replace(day=1)

    # Group by date and calculate daily average P/L
    daily_avg_df = df.groupby(df["date"].dt.date)["pnl_pct"].mean().reset_index(name="daily_avg_pl")

    def compute_sum(start_date):
        subset = daily_avg_df[daily_avg_df["date"] >= start_date]
        return round(subset["daily_avg_pl"].sum(), 2) if not subset.empty else 0.0

    today_perf = round(daily_avg_df[daily_avg_df["date"] == today]["daily_avg_pl"].sum(), 2)

    return {
        "today": today_perf,
        "week": compute_sum(this_week),
        "month": compute_sum(this_month),
        "overall": round(daily_avg_df["daily_avg_pl"].sum(), 2)
    }

def calculate_live_open_positions():
    positions = data_cache.get_positions_data()  # Use cached positions
    today_positions = []

    for pos in positions:
        entry = pos.get("entry_price")
        last = pos.get("last_price")
        qty = pos.get("quantity")
        symbol = pos.get("symbol")
        action = pos.get("position_type")

        if not all([entry, last, qty]):
            continue

        # Calculate P/L based on long or short
        if action == "BUY":
            pl = ((last - entry) / entry) * 100
        elif action == "SELL":
            pl = ((entry - last) / entry) * 100
        else:
            pl = 0

        today_positions.append({
            "symbol": symbol,
            "quantity": qty,
            "entry_price": entry,
            "last_price": last,
            "position_type": action,
            "pl": round(pl, 2)
        })

    return today_positions

def get_optimizer_status_for_template():
    """Get optimizer status for template rendering"""
    try:
        response = optimizer_status()
        return response.get_json()
    except:
        return {
            "success": False,
            "status": "error",
            "historical_trades": 0,
            "symbol_specific_count": 0,
            "universal_count": 0,
            "direction_trade_counts": {"buy_trades": 0, "short_trades": 0},
            "system_type": "three_strategy_direction_specific"
        }

@app.post("/refresh_pl")
def refresh_pl():
    """Recalculate or reload today's P/L; return JSON and no-cache headers."""
    try:
        # Call the actual refresh P/L logic function that exists
        success, message = refresh_pl_logic()
        
        if success:
            # Update the last refresh timestamp
            with open("./data/last_refresh.txt", "w") as f:
                f.write(datetime.now(EDT).isoformat())
            
            # Invalidate trades cache to force refresh
            data_cache.invalidate_trades()

        payload = {"ok": success, "message": message, "ts": datetime.now(EDT).isoformat()}
        resp = jsonify(payload)
        resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
        resp.headers["Pragma"] = "no-cache"
        return resp, 200
    except Exception as e:
        logger.error(f"Error in refresh_pl: {e}")
        payload = {"ok": False, "error": str(e)}
        resp = jsonify(payload)
        resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
        resp.headers["Pragma"] = "no-cache"
        return resp, 500

# Add these routes after your existing routes but before the main template functions

@app.route("/toggle_demo_mode", methods=["POST"])
def toggle_demo_mode():
    """Toggle demo mode on/off"""
    try:
        current_demo = get_demo_mode()
        new_demo = not current_demo
        set_demo_mode(new_demo)
        
        status = "enabled" if new_demo else "disabled"
        flash(f"Demo mode {status}.", "success" if new_demo else "info")
        logger.info(f"[DEMO] Demo mode {status} by user")
        
    except Exception as e:
        logger.error(f"Error toggling demo mode: {e}")
        flash("Error toggling demo mode.", "danger")
    
    return redirect(url_for("index"))

@app.route("/api/demo_status")
def demo_status():
    """Get demo mode status for frontend"""
    try:
        from demo_mode import DemoModeSimulator
        demo_sim = DemoModeSimulator()
        status = demo_sim.get_demo_status()
        return jsonify(status)
    except Exception as e:
        logger.error(f"Error getting demo status: {e}")
        return jsonify({"error": str(e)})

@app.route("/api/database_status")
def get_database_status():
    """Get database status for the data collection tab"""
    try:
        from market_data_collector import MarketDataCollector
        collector = MarketDataCollector()
        
        # Check if database exists and is accessible
        db_connected = os.path.exists(collector.db_path)
        
        status = {
            "connected": db_connected,
            "db_path": collector.db_path,
            "total_days": 0,
            "total_symbols": 0,
            "pick_records": 0,
            "last_updated": None
        }
        
        if db_connected:
            import sqlite3
            try:
                with sqlite3.connect(collector.db_path) as conn:
                    cursor = conn.cursor()
                    
                    # Get total days with data
                    cursor.execute("SELECT COUNT(DISTINCT date) FROM daily_summaries")
                    status["total_days"] = cursor.fetchone()[0] or 0
                    
                    # Get total symbols tracked
                    cursor.execute("SELECT COUNT(DISTINCT symbol) FROM daily_summaries")
                    status["total_symbols"] = cursor.fetchone()[0] or 0
                    
                    # Get pick records count
                    cursor.execute("SELECT COUNT(*) FROM daily_picks")
                    status["pick_records"] = cursor.fetchone()[0] or 0
                    
                    # Get last update time
                    cursor.execute("SELECT MAX(created_at) FROM daily_summaries")
                    last_update = cursor.fetchone()[0]
                    if last_update:
                        status["last_updated"] = last_update
                        
            except Exception as e:
                logger.error(f"Database query error: {e}")
                status["connected"] = False
        
        return jsonify(status)
        
    except Exception as e:
        logger.error(f"Error getting database status: {e}")
        return jsonify({"connected": False, "error": str(e)})

@app.route("/api/collection_history")
def get_collection_history():
    """Get recent collection history"""
    try:
        from market_data_collector import MarketDataCollector
        collector = MarketDataCollector()
        
        history = []
        
        if os.path.exists(collector.db_path):
            import sqlite3
            with sqlite3.connect(collector.db_path) as conn:
                cursor = conn.cursor()
                
                # Get recent collection dates from different tables
                queries = [
                    ("End-of-Day", "SELECT date, COUNT(*) as records FROM daily_summaries GROUP BY date ORDER BY date DESC LIMIT 10"),
                    ("Morning", "SELECT date, COUNT(*) as records FROM daily_picks GROUP BY date ORDER BY date DESC LIMIT 10"),
                ]
                
                for collection_type, query in queries:
                    try:
                        cursor.execute(query)
                        results = cursor.fetchall()
                        
                        for date, records in results:
                            status_class = "success" if records > 0 else "warning"
                            status = "Complete" if records > 0 else "No Data"
                            
                            history.append({
                                "date": date,
                                "type": collection_type,
                                "status": status,
                                "status_class": status_class,
                                "records": records
                            })
                    except Exception as e:
                        logger.error(f"Error querying {collection_type}: {e}")
        
        # Sort by date descending
        history.sort(key=lambda x: x["date"], reverse=True)
        
        return jsonify(history[:20])  # Return last 20 entries
        
    except Exception as e:
        logger.error(f"Error getting collection history: {e}")
        return jsonify([])

@app.route("/api/data_browser")
def data_browser():
    """Browse database tables with filtering"""
    try:
        from market_data_collector import MarketDataCollector
        collector = MarketDataCollector()
        
        table = request.args.get('table', 'daily_summaries')
        date_range = request.args.get('date_range', '7')
        symbol_filter = request.args.get('symbol_filter', '')
        limit = int(request.args.get('limit', '100'))
        
        if not os.path.exists(collector.db_path):
            return jsonify({"success": False, "error": "Database not found"})
        
        import sqlite3
        with sqlite3.connect(collector.db_path) as conn:
            cursor = conn.cursor()
            
            # Updated base queries - removed market_context and minute_ohlcv from UI
            base_queries = {
                "daily_summaries": "SELECT date, symbol, prev_close, open_price, gap_percent, day_high, day_low, day_close, day_volume, volatility FROM daily_summaries",
                "daily_picks": "SELECT date, symbol, direction, confidence FROM daily_picks",
                "trade_outcomes": "SELECT date, symbol, entry_price, exit_price, pnl_percent, direction FROM trade_outcomes"
            }
            
            if table not in base_queries:
                return jsonify({"success": False, "error": "Invalid table"})
            
            query = base_queries[table]
            params = []
            
            # Add date filter
            if date_range != 'all':
                days = int(date_range)
                query += " WHERE date >= date('now', '-{} days')".format(days)
            
            # Add symbol filter ONLY for tables that have a symbol column
            if symbol_filter and table in ["daily_summaries", "daily_picks", "trade_outcomes"]:
                symbols = [s.strip().upper() for s in symbol_filter.split(',')]
                if 'WHERE' in query:
                    query += " AND symbol IN ({})".format(','.join('?' for _ in symbols))
                else:
                    query += " WHERE symbol IN ({})".format(','.join('?' for _ in symbols))
                params.extend(symbols)
            
            # Add ordering and limit
            query += " ORDER BY date DESC, symbol"
            
            if limit:
                query += f" LIMIT {limit}"
            
            cursor.execute(query, params)
            columns = [description[0] for description in cursor.description]
            data = []
            
            for row in cursor.fetchall():
                row_dict = {}
                for i, value in enumerate(row):
                    row_dict[columns[i]] = value
                data.append(row_dict)
            
            return jsonify({
                "success": True,
                "data": data,
                "columns": columns,
                "table": table
            })
            
    except Exception as e:
        logger.error(f"Error browsing data: {e}")
        return jsonify({"success": False, "error": str(e)})

@app.route("/api/manual_collection", methods=["POST"])
def manual_collection():
    """Run manual data collection"""
    try:
        data = request.get_json()
        collection_type = data.get('type', 'morning')
        
        from market_data_collector import MarketDataCollector, run_end_of_day_collection
        
        if collection_type == 'morning':
            collector = MarketDataCollector()
            
            # Test morning collection with current picks
            plan_data = data_cache.get_trade_plan_data()
            symbols = plan_data.get("buy", []) + plan_data.get("short", [])
            
            picks_successful = 0
            context_successful = False
            
            # Collect picks data if available
            if symbols:
                morning_data = collector.get_morning_data(symbols)
                picks_successful = len([d for d in morning_data.values() if 'error' not in d])
                logger.info(f"[DATA] Picks collection: {picks_successful}/{len(symbols)} successful")
            else:
                logger.info("[DATA] No current picks to test")
            
            # Always collect market context
            try:
                context_data = collector.get_morning_market_context()
                context_successful = len([d for d in context_data.values() if 'error' not in d]) > 0
                logger.info(f"[DATA] Market context collection: {'successful' if context_successful else 'failed'}")
            except Exception as e:
                logger.error(f"[DATA] Market context collection failed: {e}")
            
            return jsonify({
                "success": True,
                "message": f"Morning collection completed",
                "picks_successful": picks_successful,
                "picks_total": len(symbols) if symbols else 0,
                "context_successful": context_successful,
                "details": f"Picks: {picks_successful}/{len(symbols) if symbols else 0}, Context: {'✓' if context_successful else '✗'}"
            })
            
        elif collection_type == 'eod':
            # Run end-of-day collection
            run_end_of_day_collection()
            
            return jsonify({
                "success": True,
                "message": "End-of-day collection completed",
                "records": "See logs for details"
            })
        
        else:
            return jsonify({"success": False, "error": "Invalid collection type"})
            
    except Exception as e:
        logger.error(f"Manual collection error: {e}")
        return jsonify({"success": False, "error": str(e)})

@app.route("/api/database_info")
def database_info():
    """Get detailed database information"""
    try:
        from market_data_collector import MarketDataCollector
        collector = MarketDataCollector()
        
        if not os.path.exists(collector.db_path):
            return jsonify({"success": False, "error": "Database not found"})
        
        import sqlite3
        
        # Get file size
        db_size = os.path.getsize(collector.db_path)
        db_size_mb = round(db_size / (1024 * 1024), 2)
        
        with sqlite3.connect(collector.db_path) as conn:
            cursor = conn.cursor()
            
            # Get table information - updated table list
            tables = {}
            table_names = ["daily_summaries", "daily_picks", "trade_outcomes", "minute_ohlcv"]
            
            for table in table_names:
                try:
                    cursor.execute(f"SELECT COUNT(*) FROM {table}")
                    count = cursor.fetchone()[0]
                    tables[table] = {"count": count, "size": "N/A"}
                except Exception as e:
                    logger.error(f"Error counting {table}: {e}")
                    tables[table] = {"count": 0, "size": "N/A"}
            
            # Get date range
            try:
                cursor.execute("SELECT MIN(date), MAX(date) FROM daily_summaries")
                date_range_result = cursor.fetchone()
                date_range = f"{date_range_result[0]} to {date_range_result[1]}" if date_range_result[0] else "No data"
            except Exception as e:
                logger.error(f"Error getting date range: {e}")
                date_range = "Error getting date range"
            
            # Get last collection time
            try:
                cursor.execute("SELECT MAX(created_at) FROM daily_summaries")
                last_collection = cursor.fetchone()[0] or "Never"
            except Exception as e:
                logger.error(f"Error getting last collection: {e}")
                last_collection = "Error getting last collection"
            
            return jsonify({
                "success": True,
                "tables": tables,
                "total_size": f"{db_size_mb} MB",
                "date_range": date_range,
                "last_collection": last_collection,
                "db_path": collector.db_path
            })
            
    except Exception as e:
        logger.error(f"Database info error: {e}")
        return jsonify({"success": False, "error": str(e)})

@app.route("/api/export_data")
def export_data():
    """Export data as CSV"""
    try:
        from market_data_collector import MarketDataCollector
        import csv
        import io
        
        collector = MarketDataCollector()
        table = request.args.get('table', 'daily_summaries')
        date_range = request.args.get('date_range', '30')
        symbol_filter = request.args.get('symbol_filter', '')
        
        if not os.path.exists(collector.db_path):
            return "Database not found", 404
        
        import sqlite3
        with sqlite3.connect(collector.db_path) as conn:
            cursor = conn.cursor()
            
            # Use same query logic as data_browser
            base_queries = {
                "daily_summaries": "SELECT * FROM daily_summaries",
                "daily_picks": "SELECT * FROM daily_picks",
                "trade_outcomes": "SELECT * FROM trade_outcomes",
                "market_context": "SELECT * FROM market_context"
            }
            
            query = base_queries.get(table, base_queries["daily_summaries"])
            params = []
            
            # Add filters
            if date_range != 'all':
                days = int(date_range)
                query += " WHERE date >= date('now', '-{} days')".format(days)
            
            if symbol_filter:
                symbols = [s.strip().upper() for s in symbol_filter.split(',')]
                if 'WHERE' in query:
                    query += " AND symbol IN ({})".format(','.join('?' for _ in symbols))
                else:
                    query += " WHERE symbol IN ({})".format(','.join('?' for _ in symbols))
                params.extend(symbols)
            
            query += " ORDER BY date DESC"
            
            cursor.execute(query, params)
            columns = [description[0] for description in cursor.description]
            rows = cursor.fetchall()
            
            # Create CSV
            output = io.StringIO()
            writer = csv.writer(output)
            writer.writerow(columns)
            writer.writerows(rows)
            
            # Return as file download
            from flask import Response
            return Response(
                output.getvalue(),
                mimetype='text/csv',
                headers={"Content-Disposition": f"attachment;filename={table}_{date_range}days.csv"}
            )
            
    except Exception as e:
        logger.error(f"Export error: {e}")
        return "Export failed", 500
    
@app.route("/api/download_database")
def download_database():
    """Download the entire market data database"""
    try:
        from market_data_collector import MarketDataCollector
        collector = MarketDataCollector()
        
        if not os.path.exists(collector.db_path):
            return "Database not found", 404
        
        # Get database size for logging
        db_size_mb = round(os.path.getsize(collector.db_path) / (1024 * 1024), 2)
        logger.info(f"[DATA] Database download requested - Size: {db_size_mb} MB")
        
        from flask import send_file
        return send_file(
            collector.db_path,
            as_attachment=True,
            download_name=f"market_data_{datetime.now(EDT).strftime('%Y%m%d_%H%M%S')}.db",
            mimetype='application/octet-stream'
        )
        
    except Exception as e:
        logger.error(f"Database download error: {e}")
        return "Download failed", 500

# Helper functions for template data
def get_database_status_for_template():
    """Get database status for template rendering"""
    try:
        response = get_database_status()
        return response.get_json()
    except:
        return {"connected": False}

def get_collection_history_for_template():
    """Get collection history for template"""
    try:
        response = get_collection_history()
        return response.get_json()
    except:
        return []

# Add the as_edt function that was missing
def validate_optimizer_request(data, required_fields):
    """Validate optimizer API request data"""
    missing_fields = [field for field in required_fields if field not in data]
    if missing_fields:
        return False, f"Missing required fields: {', '.join(missing_fields)}"
    return True, None

def format_optimizer_error(error_msg):
    """Format optimizer errors for consistent API responses"""
    return {
        "success": False,
        "error": str(error_msg),
        "timestamp": datetime.now(EDT).isoformat()
    }

@app.route("/api/database_test")
def database_test():
    """Test database creation and basic operations"""
    try:
        from market_data_collector import MarketDataCollector
        collector = MarketDataCollector()
        
        # Verify database integrity
        is_valid = collector.verify_database_integrity()
        
        # Get basic info
        db_exists = os.path.exists(collector.db_path)
        db_size = os.path.getsize(collector.db_path) if db_exists else 0
        
        # Try to create a test record
        test_record_created = False
        try:
            import sqlite3
            with sqlite3.connect(collector.db_path) as conn:
                cursor = conn.cursor()
                today = datetime.now(EDT).strftime("%Y-%m-%d")
                
                # Insert a test pick
                cursor.execute("""
                    INSERT OR REPLACE INTO daily_picks
                    (date, symbol, direction, confidence)
                    VALUES (?, ?, ?, ?)
                """, (today, "TEST", "BUY", 0.9))
                
                # Verify it was inserted
                cursor.execute("SELECT COUNT(*) FROM daily_picks WHERE symbol = 'TEST'")
                count = cursor.fetchone()[0]
                test_record_created = count > 0
                
                conn.commit()
        except Exception as e:
            logger.error(f"Test record creation failed: {e}")
        
        return jsonify({
            "success": True,
            "db_path": collector.db_path,
            "db_exists": db_exists,
            "db_size": db_size,
            "is_valid": is_valid,
            "test_record_created": test_record_created,
            "timestamp": datetime.now(EDT).isoformat()
        })
        
    except Exception as e:
        logger.error(f"Database test error: {e}")
        return jsonify({"success": False, "error": str(e)})

@app.route("/api/initialize_database", methods=["POST"])
def initialize_database():
    """Force database initialization with sample data"""
    try:
        from market_data_collector import MarketDataCollector
        collector = MarketDataCollector()
        
        # Force re-initialization
        collector.init_database()
        
        # Add sample data
        today = datetime.now(EDT).strftime("%Y-%m-%d")
        
        import sqlite3
        with sqlite3.connect(collector.db_path) as conn:
            cursor = conn.cursor()
            
            # Sample picks
            sample_picks = [
                (today, "AAPL", "BUY", 0.8),
                (today, "TSLA", "BUY", 0.7),
                (today, "NVDA", "BUY", 0.9),
                (today, "MSFT", "BUY", 0.6),
                (today, "AMZN", "BUY", 0.7)
            ]
            
            cursor.executemany("""
                INSERT OR REPLACE INTO daily_picks
                (date, symbol, direction, confidence)
                VALUES (?, ?, ?, ?)
            """, sample_picks)
            
            # Sample daily summary
            sample_summary = [
                (today, "AAPL", 150.0, 151.0, 0.67, 152.0, 149.5, 151.5, 1000000, 2.1),
                (today, "TSLA", 800.0, 805.0, 0.63, 810.0, 798.0, 808.0, 500000, 3.2),
            ]
            
            cursor.executemany("""
                INSERT OR REPLACE INTO daily_summaries
                (date, symbol, prev_close, open_price, gap_percent, day_high, day_low, day_close, day_volume, volatility)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, sample_summary)
            
            conn.commit()
        
        # Verify the data was inserted
        verification = collector.verify_database_integrity()
        
        return jsonify({
            "success": True,
            "message": "Database initialized with sample data",
            "verification_passed": verification,
            "sample_records_added": len(sample_picks) + len(sample_summary)
        })
        
    except Exception as e:
        logger.error(f"Database initialization error: {e}")
        return jsonify({"success": False, "error": str(e)})
    
@app.route("/api/optimizer/current_performance")
def optimizer_current_performance():
    """Analyze three-strategy performance comparison"""
    try:
        optimizer = ExecutionOptimizer()
        performance = optimizer.generate_strategy_comparison()

        if "error" in performance:
            return jsonify({
                "success": False,
                "error": performance["error"]
            })

        # Add direction-specific trade counts for UI
        all_trades = optimizer.get_all_historical_trades()
        buy_trades = len(all_trades[all_trades['direction'] == 'BUY']) if not all_trades.empty else 0
        short_trades = len(all_trades[all_trades['direction'] == 'SELL_SHORT']) if not all_trades.empty else 0

        return jsonify({
            "success": True,
            "direction_trade_counts": {
                "buy_trades": buy_trades,
                "short_trades": short_trades,
                "total_trades": len(all_trades) if not all_trades.empty else 0
            },
            **performance
        })
        
    except Exception as e:
        logger.error(f"[OPTIMIZER] Performance analysis error: {e}")
        return jsonify({
            "success": False,
            "error": str(e)
        })
    
@app.route("/api/optimizer/status")
def optimizer_status():
    """Get optimizer status and data availability with strategy distribution"""
    try:
        optimizer = ExecutionOptimizer()
        
        # Get basic stats
        historical_picks = optimizer.get_historical_picks()
        
        # Get strategy distribution using the correct method name
        strategy_analysis = optimizer.analyze_strategy_distribution()
        
        # Handle the actual return format from analyze_strategy_distribution
        strategy_counts = strategy_analysis.get('strategy_distribution', {})
        total_symbol_directions = 0
        symbol_specific_count = 0
        universal_count = 0
        default_count = 0
        
        # Extract counts from the actual distribution format
        if 'symbol_specific' in strategy_counts:
            symbol_specific_count = strategy_counts['symbol_specific'].get('combinations', 0)
        if 'universal' in strategy_counts:
            universal_count = strategy_counts['universal'].get('combinations', 0)
        if 'default' in strategy_counts:
            default_count = strategy_counts['default'].get('combinations', 0)
            
        total_symbol_directions = symbol_specific_count + universal_count + default_count
        optimization_coverage = (symbol_specific_count / total_symbol_directions) if total_symbol_directions > 0 else 0
        
        status = {
            "success": True,
            "status": "ready",
            "historical_trades": len(historical_picks),
            "total_symbol_directions": total_symbol_directions,
            "optimization_coverage": optimization_coverage,
            "strategy_distribution": {
                "symbol_specific": symbol_specific_count,
                "universal": universal_count,
                "default": default_count,
                "total": total_symbol_directions
            },
            "database_path": optimizer.db_path,
            "system_type": "three_strategy_simplified",
            "symbol_specific_count": symbol_specific_count,
            "universal_count": universal_count
        }
        
        # Check if we have sufficient data
        all_trades = optimizer.get_all_historical_trades()
        buy_trades = len(all_trades[all_trades['direction'] == 'BUY']) if not all_trades.empty else 0
        short_trades = len(all_trades[all_trades['direction'] == 'SELL_SHORT']) if not all_trades.empty else 0

        if len(historical_picks) < 10:
            status["status"] = "insufficient_data"
            status["message"] = f"Need more historical data. Have {len(historical_picks)} trades, recommend 50+"
        elif buy_trades < 50 and short_trades < 50:
            status["status"] = "insufficient_universal"
            status["message"] = f"Need 50+ trades per direction for universal optimization. Have {buy_trades} BUY, {short_trades} SHORT trades"
        else:
            status["status"] = "ready"
            status["message"] = f"Ready for optimization with {len(historical_picks)} total trades ({buy_trades} BUY, {short_trades} SHORT)"
        
        return jsonify(status)
        
    except Exception as e:
        logger.error(f"[OPTIMIZER] Status check error: {e}")
        return jsonify({
            "success": False,
            "error": str(e),
            "status": "error"
        })

@app.route("/api/optimizer/strategy_distribution")
def optimizer_strategy_distribution():
    """Get strategy distribution analysis for the direction-specific three-strategy system"""
    try:
        optimizer = ExecutionOptimizer()
        strategy_analysis = optimizer.analyze_strategy_distribution()
        
        if "error" in strategy_analysis:
            return jsonify({
                "success": False,
                "error": strategy_analysis["error"]
            })
        
        # Get direction-specific trade counts
        all_trades = optimizer.get_all_historical_trades()
        buy_trades = len(all_trades[all_trades['direction'] == 'BUY']) if not all_trades.empty else 0
        short_trades = len(all_trades[all_trades['direction'] == 'SELL_SHORT']) if not all_trades.empty else 0
        
        # Format the response to match what the frontend expects
        strategy_counts = strategy_analysis.get('strategy_distribution', {})
        
        response = {
            "success": True,
            "strategy_counts": {
                "symbol_specific": strategy_counts.get('symbol_specific', {}).get('combinations', 0),
                "universal": strategy_counts.get('universal', {}).get('combinations', 0),
                "default": strategy_counts.get('default', {}).get('combinations', 0),
                "total": strategy_analysis.get('total_symbol_direction_combinations', 0)
            },
            "optimization_coverage": (strategy_counts.get('symbol_specific', {}).get('combinations', 0) / 
                                    strategy_analysis.get('total_symbol_direction_combinations', 1)) if strategy_analysis.get('total_symbol_direction_combinations', 0) > 0 else 0,
            "direction_trade_counts": {
                "buy_trades": buy_trades,
                "short_trades": short_trades,
                "total_trades": len(all_trades) if not all_trades.empty else 0
            },
            "trade_distribution_by_strategy": strategy_analysis.get('trade_distribution_by_strategy', {}),
            "data_period": strategy_analysis.get('data_period', 'Unknown'),
            "total_trades_analyzed": strategy_analysis.get('total_trades_analyzed', 0)
        }
        
        logger.debug(f"[OPTIMIZER] Direction trade counts: BUY={buy_trades}, SHORT={short_trades}")
        
        return jsonify(response)
        
    except Exception as e:
        logger.error(f"[OPTIMIZER] Strategy distribution error: {e}")
        return jsonify({
            "success": False,
            "error": str(e)
        })

@app.route("/token_status_api")
def token_status_api():
    """API endpoint to get current token status with refresh countdown"""
    try:
        status = get_token_status() if is_authenticated() else {"valid": False, "message": "Not authenticated"}
        
        # Add additional context for the frontend
        if status.get("valid"):
            refresh_days = status.get("refresh_expires_in_days", 8)
            status["needs_reauth_soon"] = refresh_days <= 2
            status["urgent_reauth"] = refresh_days <= 1
            status["status_class"] = "danger" if refresh_days <= 1 else "warning" if refresh_days <= 2 else "success"
        
        return jsonify(status)
    except Exception as e:
        logger.error(f"Token status API error: {e}")
        return jsonify({"valid": False, "message": "Status check failed"})

@app.route("/force_token_refresh", methods=["POST"])
def force_token_refresh():
    """Manually trigger a token refresh (for testing/maintenance)"""
    try:
        if not is_authenticated():
            return jsonify({"success": False, "message": "Not authenticated"})
        
        # Import here to avoid circular imports
        from schwab_api import refresh_tokens
        
        result = refresh_tokens()
        if result:
            return jsonify({
                "success": True, 
                "message": "Token refreshed successfully",
                "new_status": get_token_status()
            })
        else:
            return jsonify({"success": False, "message": "Token refresh failed"})
            
    except Exception as e:
        logger.error(f"Manual token refresh error: {e}")
        return jsonify({"success": False, "message": str(e)})

@app.route("/reauth_reminder")
def reauth_reminder():
    """Show a dedicated re-authentication reminder page"""
    if not is_authenticated():
        return redirect(url_for("authorize"))
    
    status = get_token_status()
    refresh_days = status.get("refresh_expires_in_days", 8)
    
    # Only show reminder if actually needed
    if refresh_days > 2:
        flash("Re-authentication not needed yet.", "info")
        return redirect(url_for("index"))
    
    return render_template("reauth_reminder.html", 
                         token_status=status,
                         refresh_days=refresh_days,
                         urgent=refresh_days <= 1)

@app.route("/api/reset_database", methods=["POST"])
def reset_database():
    """Delete and recreate the database from scratch"""
    try:
        from market_data_collector import MarketDataCollector
        collector = MarketDataCollector()
        
        # Delete the existing database file
        if os.path.exists(collector.db_path):
            os.remove(collector.db_path)
            logger.info(f"[DATA] Database deleted: {collector.db_path}")
        
        # Reinitialize with fresh schema
        collector.init_database()
        logger.info("[DATA] Database recreated with fresh schema")
        
        return jsonify({
            "success": True,
            "message": "Database reset successfully - fresh database created"
        })
        
    except Exception as e:
        logger.error(f"[DATA] Database reset failed: {e}")
        return jsonify({
            "success": False,
            "error": str(e)
        })

@app.route("/api/optimizer/weekly_optimization", methods=["POST"])
def optimizer_weekly_optimization():
    """Run weekly optimization and save results to file"""
    try:
        logger.info("[OPTIMIZER] Starting weekly optimization job...")
        results = run_weekly_optimization_job()
        
        if "error" in results:
            return jsonify({
                "success": False,
                "error": results["error"]
            })
        
        return jsonify({
            "success": True,
            "message": "Weekly optimization completed successfully",
            "results": {
                "total_trades": results.get("total_historical_trades", 0),
                "symbol_specific_count": results.get("strategy_counts", {}).get("symbol_specific", 0),
                "universal_buy_params": results.get("universal_strategies", {}).get("BUY", {}),
                "universal_short_params": results.get("universal_strategies", {}).get("SELL_SHORT", {}),
                "results_file": "./data/weekly_optimization_results.json"
            }
        })
        
    except Exception as e:
        logger.error(f"[OPTIMIZER] Weekly optimization error: {e}")
        return jsonify({
            "success": False,
            "error": str(e)
        })

@app.route("/")
def index():
    if not session.get("logged_in"):
        return redirect(url_for("login"))

    # Use cached data instead of reading CSV every time
    df = data_cache.get_trades_data()

    # Only refresh P/L if market is open and data is stale
    if is_market_open_now():
        last_refresh_path = "./data/last_refresh.txt"
        should_refresh = True
        
        if os.path.exists(last_refresh_path):
            try:
                with open(last_refresh_path) as f:
                    last_refresh = datetime.fromisoformat(f.read().strip())
                    # Only refresh if last update was more than 2 minutes ago
                    should_refresh = (datetime.now(EDT) - last_refresh).total_seconds() > 120
            except:
                pass
        
        if should_refresh:
            async_refresh_pl()  # Non-blocking refresh

    if not df.empty:
        df["year"] = df["date"].dt.year
        df["month"] = df["date"].dt.month
        df["day"] = df["date"].dt.day

    gainer_loser_mode = request.args.get("view", "avg")
    show_tab = request.args.get("show_tab", "dashboard")
    selected_year = request.args.get("year", type=int)
    selected_month = request.args.get("month", type=int)
    selected_day = request.args.get("day", type=int)
    selected_ticker = request.args.get("ticker", type=str)

    years = sorted(df["year"].unique()) if not df.empty else []
    months = sorted(df["month"].unique()) if not df.empty else []

    total_pl = round(df["pnl_pct"].sum(), 2) if not df.empty else 0.0

    filtered_df = df.copy()
    if selected_year:
        filtered_df = filtered_df[filtered_df["year"] == selected_year]
    if selected_month:
        filtered_df = filtered_df[filtered_df["month"] == selected_month]
    if selected_day:
        filtered_df = filtered_df[filtered_df["day"] == selected_day]
    if selected_ticker:
        filtered_df = filtered_df[filtered_df["symbol"].str.upper() == selected_ticker.upper()]

    filtered_pl = round(filtered_df.groupby("date")["pnl_pct"].mean().sum(), 2) if not filtered_df.empty else 0.0

    trades_for_display = []
    if selected_year or selected_month or selected_day or selected_ticker:
        for _, row in filtered_df.sort_values("date", ascending=False).iterrows():
            trades_for_display.append({
                "date": row["date"],
                "formatted_date": row["date"].strftime("%m/%d/%Y") if pd.notna(row["date"]) else "",
                "symbol": row.get("symbol", ""),
                "action": row.get("action", ""),
                "entry": row.get("entry", ""),
                "exit": row.get("exit", ""),
                "pnl_pct": row.get("pnl_pct", 0.0)
            })

    avg_summary = df.groupby("symbol")["pnl_pct"].mean().reset_index().round({"pnl_pct": 2}).sort_values("pnl_pct", ascending=False)
    total_summary = df.groupby("symbol")["pnl_pct"].sum().reset_index().round({"pnl_pct": 2}).sort_values("pnl_pct", ascending=False)

    top_avg_winners = avg_summary.head(10).to_dict("records")
    top_avg_losers = avg_summary.tail(10).sort_values("pnl_pct").to_dict("records")
    top_total_winners = total_summary.head(10).to_dict("records")
    top_total_losers = total_summary.tail(10).sort_values("pnl_pct").to_dict("records")

    tickers = sorted(df["symbol"].dropna().unique()) if not df.empty else []

    try:
        plan_data = data_cache.get_trade_plan_data()
    except Exception as e:
        logger.error(f"Failed to load trade plan from cache: {e}")
        plan_data = {"buy": [], "short": []}

    # Get last modified time of trade_plan.json
    try:
        json_path = os.path.join(os.path.dirname(__file__), "./data/trade_plan.json")
        timestamp = os.path.getmtime(json_path)
        last_cached = datetime.fromtimestamp(timestamp, tz=UTC).astimezone(EDT).strftime("%m/%d/%Y %I:%M %p")
    except Exception:
        last_cached = "Unknown"

    # Get time of last Yahoo P/L refresh
    try:
        with open("./data/last_refresh.txt", "r") as f:
            last_refresh = f.read().strip()
    except Exception:
        last_refresh = "--"

    performance = calculate_performance(df)
    daily_avg = get_daily_avg(df)

    # ➕ Add daily P/L list for logs tab
    from collections import defaultdict
    
    daily_pl_dict = defaultdict(list)
    
    for _, row in filtered_df.iterrows():
        if pd.notna(row["date"]) and pd.notna(row["pnl_pct"]):
            date_str = row["date"].strftime("%m/%d/%Y")
            daily_pl_dict[date_str].append(row["pnl_pct"])
    
    # Convert to list of dicts
    daily_avg_pl = []
    for date_str, pl_list in sorted(daily_pl_dict.items()):
        avg = round(sum(pl_list) / len(pl_list), 2)
        daily_avg_pl.append({"date": date_str, "avg_pl": avg})

    # Use cached positions instead of direct API call
    open_positions = data_cache.get_positions_data()
    pos_lookup = {pos["symbol"]: pos for pos in open_positions}

    today_str = datetime.now(UTC).astimezone(EDT).strftime("%Y-%m-%d")
    todays_trades = []

    if not df.empty:
        today_df = df[df["date"].dt.strftime("%Y-%m-%d") == today_str]
        for _, row in today_df.iterrows():
            symbol = row["symbol"]
            entry_price = row["entry"]
            exit_price = row["exit"]
            is_closed = not pd.isna(exit_price)

            if is_closed:
                current_price = exit_price
                status = "Closed"
                pl = row["pnl_pct"]
            else:
                current_price = row.get("current", entry_price)
                pl = row.get("pnl_pct", 0.0)
                status = "Open"

            todays_trades.append({
                "symbol": symbol,
                "action": row["action"],
                "entry": entry_price,
                "current": round(current_price, 2),
                "status": status,
                "pl": round(pl, 2)
            })

    if not is_after_market_close_cutoff():
        todays_trades = simulate_theoretical_hits(todays_trades)

    # Enhanced token status with re-auth countdown
    token_status_enhanced = get_token_status() if is_authenticated() else {"valid": False, "message": "Not authenticated"}
    
    # Add re-auth warning flags
    if token_status_enhanced.get("valid"):
        refresh_days = token_status_enhanced.get("refresh_expires_in_days", 8)
        token_status_enhanced["needs_reauth_soon"] = refresh_days <= 2
        token_status_enhanced["urgent_reauth"] = refresh_days <= 1
        token_status_enhanced["refresh_warning"] = refresh_days <= 2  # For template compatibility

    pl_values = [t["pl"] for t in todays_trades if "pl" in t]
    today_pl = round(sum(pl_values) / len(pl_values), 2) if pl_values else 0.0
    open_time = "09:30"
    close_time = get_market_close_time().strftime("%H:%M")

    # Load the last refresh timestamp
    refresh_txt_path = "./data/last_refresh.txt"
    last_refresh_display = ""
    if os.path.exists(refresh_txt_path):
        with open(refresh_txt_path) as f:
            try:
                ts = f.read().strip()
                dt = datetime.fromisoformat(ts)
                last_refresh_display = dt.isoformat()  # Store in full for JS
            except Exception:
                last_refresh_display = ""

    return render_template(
        "index.html",
        scheduler_running=scheduler_running_flag,
        scheduled_jobs=get_scheduled_jobs(),
        positions=calculate_live_open_positions() if is_authenticated() else [],
        allocation=get_allocation(),
        demo_mode=get_demo_mode(),
        db_status=get_database_status_for_template(),
        collection_history=get_collection_history_for_template(),
        optimizer_status=get_optimizer_status_for_template(),
        token_status=token_status_enhanced,
        years=years,
        months=months,
        selected_year=selected_year,
        selected_month=selected_month,
        selected_day=selected_day,
        total_pl=total_pl,
        filtered_pl=filtered_pl,
        show_tab=show_tab,
        trades=trades_for_display,
        performance=performance,
        daily_avg=daily_avg,
        top_avg_winners=top_avg_winners,
        top_avg_losers=top_avg_losers,
        top_total_winners=top_total_winners,
        top_total_losers=top_total_losers,
        selected_ticker=selected_ticker,
        tickers=tickers,
        view_mode=gainer_loser_mode,
        todays_trades=todays_trades,
        open_time=open_time,
        close_time=close_time,
        plan=plan_data,
        sheet=plan_data,
        last_cached=last_cached,
        last_refresh=last_refresh_display,
        today_pl=today_pl,
        daily_avg_pl=daily_avg_pl,
        account_id=get_hashed_account_id()
    )

@app.route("/toggle_scheduler", methods=["POST"])
def toggle_scheduler():
    """Toggle scheduler without blocking the web request"""
    global scheduler_running_flag
    
    try:
        current_state = scheduler_running_flag
        
        if current_state:
            success = stop_scheduler()
            if success:
                flash("Scheduler stopped.", "success")
            else:
                flash("Failed to stop scheduler - it may be busy.", "warning")
        else:
            success = start_scheduler_once()
            if success:
                flash("Scheduler started.", "success")
            else:
                flash("Failed to start scheduler - it may be busy.", "warning")
                
    except Exception as e:
        logger.error(f"Error toggling scheduler: {e}")
        flash("Error toggling scheduler.", "danger")
    
    return redirect(url_for("index"))

@app.route("/manual_place_order", methods=["POST"])
def manual_place_order():
    def async_trade():
        try:
            run_place_order()
        except Exception as e:
            logger.error(f"Trade placement failed in thread: {e}")

    Thread(target=async_trade).start()
    flash("Trade placement started in background.", "info")
    return redirect(url_for("index"))

@app.route("/manual_close_positions", methods=["POST"])
def manual_close_positions():
    def async_close():
        try:
            run_close_positions()
        except Exception as e:
            logger.error(f"Position closing failed in thread: {e}")

    Thread(target=async_close).start()
    flash("Position closing started in background.", "info")
    return redirect(url_for("index"))

@app.route("/refresh_sheet", methods=["POST"])
def refresh_sheet():
    """Start trade plan refresh and return status immediately"""
    def async_refresh_sheet():
        try:
            success, msg = refresh_trade_plan()
            logger.info(f"Trade plan refresh: {msg}")
            # Store the result in a simple file for status checking
            with open("./data/refresh_status.txt", "w") as f:
                f.write(f"{'success' if success else 'error'}|{msg}")
        except Exception as e:
            logger.error(f"Trade plan refresh failed: {e}")
            with open("./data/refresh_status.txt", "w") as f:
                f.write(f"error|{str(e)}")

    # Mark refresh as in progress
    with open("./data/refresh_status.txt", "w") as f:
        f.write("in_progress|Refreshing trade plan...")
    
    Thread(target=async_refresh_sheet).start()
    return jsonify({"status": "started", "message": "Trade plan refresh started"})

@app.route("/refresh_sheet_status")
def refresh_sheet_status():
    """Check the status of trade plan refresh"""
    try:
        if not os.path.exists("./data/refresh_status.txt"):
            return jsonify({"status": "unknown", "message": "No refresh in progress"})
        
        with open("./data/refresh_status.txt", "r") as f:
            content = f.read().strip()
            if "|" in content:
                status, message = content.split("|", 1)
                return jsonify({"status": status, "message": message})
            else:
                return jsonify({"status": "unknown", "message": content})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)})

@app.route("/get_trade_plan")
def get_trade_plan():
    """Get current trade plan data"""
    try:
        # Use cached version instead of load_trade_plan_from_cache
        plan_data = data_cache.get_trade_plan_data()
        
        # Get last modified time
        json_path = os.path.join(os.path.dirname(__file__), "./data/trade_plan.json")
        try:
            timestamp = os.path.getmtime(json_path)
            last_cached = datetime.fromtimestamp(timestamp, tz=UTC).astimezone(EDT).strftime("%m/%d/%Y %I:%M %p")
        except Exception:
            last_cached = "Unknown"
        
        return jsonify({
            "plan": plan_data,
            "last_cached": last_cached
        })
    except Exception as e:
        return jsonify({"error": str(e)})
        
@app.route("/refresh_timestamp")
def refresh_timestamp():
    refresh_txt_path = "./data/last_refresh.txt"
    if os.path.exists(refresh_txt_path):
        with open(refresh_txt_path) as f:
            return f.read().strip(), 200
    return "", 204

@app.route("/update_allocation", methods=["POST"])
def update_allocation():
    try:
        raw_val = float(request.form["allocation"])  # e.g. 25 from the form
        new_val = round(raw_val / 100.0, 4)  # Convert to 0.25
        if 0.01 <= new_val <= 1:
            set_allocation(new_val)
            logger.info(f"[TRADE] Allocation updated to {raw_val:.0f}%")
        else:
            logger.warning(f"[TRADE] Allocation {raw_val}% out of range.")
    except Exception as e:
        logger.error(f"[TRADE] Error updating allocation: {e}")
    return redirect(url_for("index", show_tab="dashboard"))

@app.before_request
def ensure_authenticated():
    allowed = {"authorize", "submit_auth_url", "static", "login"}
    if request.endpoint not in allowed:
        if not session.get("logged_in"):
            return redirect(url_for("login"))

# === Dashboard time helpers ===
try:
    LOCAL_TZ = datetime.now().astimezone().tzinfo  # system local timezone (UTC on Render, EDT on Pi)
except Exception:
    LOCAL_TZ = ZoneInfo("UTC")

def as_edt(dt_naive_or_aware):
    """Convert a schedule job's next_run to EDT correctly."""
    if getattr(dt_naive_or_aware, "tzinfo", None) is None:
        dt_local = dt_naive_or_aware.replace(tzinfo=LOCAL_TZ)
    else:
        dt_local = dt_naive_or_aware
    return dt_local.astimezone(EDT)

def get_scheduled_jobs():
    """Get jobs for dashboard - SIMPLE LOGIC"""
    now_edt = datetime.now(EDT)
    today = now_edt.date()
    
    # Job name mapping for dashboard
    job_labels = {
        "get_morning_data_for_picks": "Collect Morning Data",
        "run_place_order": "Place Morning Trades", 
        "schedule_close": "Schedule Close Positions",
        "scheduled_refresh": "Token Refresh",
        "scheduled_refresh_trade_plan": "Update Trade Plan",
        "run_end_of_day_collection": "End-of-Day Data Collection",
        "health_check": "Health Check",
        "proactive_token_maintenance": "Token Maintenance"
    }
    
    jobs = []
    
    for job in schedule.jobs:
        try:
            # Get job details
            func_name = getattr(job.job_func, "__name__", "unknown")
            
            # Handle wrapped functions (weekday_only)
            if hasattr(job.job_func, "__wrapped__"):
                func_name = getattr(job.job_func.__wrapped__, "__name__", func_name)
            
            # Skip internal/hidden jobs
            if func_name in ["wrapper"]:
                continue
            
            # Convert next_run to EDT - handle timezone properly
            if job.next_run.tzinfo is None:
                # Assume UTC if no timezone
                next_run_edt = job.next_run.replace(tzinfo=ZoneInfo("UTC")).astimezone(EDT)
            else:
                next_run_edt = job.next_run.astimezone(EDT)
            
            # SIMPLE LOGIC: Show job if it's scheduled for today and hasn't happened yet
            if next_run_edt.date() == today and next_run_edt > now_edt:
                label = job_labels.get(func_name, func_name)
                
                jobs.append({
                    "label": label,
                    "next_run": next_run_edt.strftime("%I:%M %p EDT"),
                    "function": func_name
                })
        
        except Exception as e:
            logger.error(f"[SCHEDULER] Error processing job for dashboard: {e}")
            continue
    
    # Sort by next run time
    jobs.sort(key=lambda x: datetime.strptime(x["next_run"], "%I:%M %p EDT"))
    
    return jobs

def get_hashed_account_id():
    return os.getenv("ACCOUNT_ID")

def simulate_theoretical_hits(trades):
    import yfinance as yf
    if is_after_market_close_cutoff():
        return trades  # Skip updating after cutoff

    # Batch process symbols for efficiency
    symbols_to_check = [trade["symbol"] for trade in trades if trade["status"] == "Open"]
    
    if not symbols_to_check:
        return trades

    # Get historical data for all symbols at once
    symbol_data = {}
    for symbol in set(symbols_to_check):  # Remove duplicates
        try:
            data = yf.Ticker(symbol).history(period="1d", interval="1m")
            if not data.empty:
                symbol_data[symbol] = data
        except Exception as e:
            logger.error(f"Sim hit error for {symbol}: {e}")

    for trade in trades:
        if trade["status"] != "Open":
            continue

        symbol = trade["symbol"]
        entry_price = trade["entry"]
        action = trade["action"].lower()

        if symbol not in symbol_data:
            trade["status"] = "Error"
            continue

        data = symbol_data[symbol]

        try:
            if action == "buy":
                stop_price = entry_price * 0.99
                profit_price = entry_price * 1.03
                for _, candle in data.iterrows():
                    if candle["Low"] <= stop_price:
                        trade["status"] = "Hit Stop Loss"
                        trade["pl"] = -1.0
                        break
                    elif candle["High"] >= profit_price:
                        trade["status"] = "Hit Take Profit"
                        trade["pl"] = 3.0
                        break

            elif action == "sell":
                stop_price = entry_price * 1.01
                profit_price = entry_price * 0.97
                for _, candle in data.iterrows():
                    if candle["High"] >= stop_price:  # Stop loss condition
                        trade["status"] = "Hit Stop Loss"
                        trade["pl"] = -1.0
                        break
                    elif candle["Low"] <= profit_price:  # Take profit condition
                        trade["status"] = "Hit Take Profit"
                        trade["pl"] = 3.0
                        break

        except Exception as e:
            trade["status"] = "Error"
            logger.error(f"Sim hit error for {symbol}: {e}")

    return trades

@app.after_request
def add_no_cache_headers(resp):
    try:
        if resp.is_json:
            resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
            resp.headers["Pragma"] = "no-cache"
    except Exception:
        pass
    return resp

if __name__ == "__main__":
    auto_fill_account_id_if_blank()
    if os.path.exists("./data/token.json"):
        refresh_tokens()
        # Initialize scheduler once at startup
        initialize_scheduler_once()
        schedule_close()
    else:
        logger.warning("No token.json found. Skipping until authenticated.")
    
    app.run(debug=False, use_reloader=False)
else:
    # For production deployment (Render, etc.)
    auto_fill_account_id_if_blank()
    if os.path.exists("./data/token.json"):
        refresh_tokens()
    
    # Initialize scheduler once when module is loaded in production
    initialize_scheduler_once()
    schedule_close()


