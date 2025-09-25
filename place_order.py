import requests
from bs4 import BeautifulSoup
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
import json
import math
import time
import os
import sys
from loguru import logger
from config import get_allocation, get_demo_mode
from csv_logger import append_trade_log
from schwab_api import make_schwab_request, is_shortable
from concurrent.futures import ThreadPoolExecutor, as_completed
from trade_plan_fetcher import get_trade_plan_from_google_sheet, convert_site_data, get_trade_plan_from_cache
from demo_mode import DemoModeSimulator

# Ensure logs directory exists
os.makedirs(os.path.join(os.path.dirname(__file__), "logs"), exist_ok=True)

UTC = timezone.utc
EDT = ZoneInfo("America/New_York")
logger._core.timestamp = lambda: datetime.now(UTC).astimezone(EDT)

# Remove any existing loggers
logger.remove()

# File logger (Eastern Time patched globally)
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

ACCOUNT_ID = os.getenv("ACCOUNT_ID")
DRY_RUN = os.getenv("DRY_RUN", "False").lower() == "true"

demo_simulator = DemoModeSimulator()

def get_trade_plan_from_site():
    from playwright.sync_api import sync_playwright

    login_url = "https://www.phinetix.com/trader/login.php"
    data_url = "https://www.phinetix.com/trader/SavantLD/ui/index.php"

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context()
            page = context.new_page()

            page.goto(login_url)
            page.fill('input[name="email"]', os.getenv("PICKS_SITE_USER"))
            page.fill('input[name="password"]', os.getenv("PICKS_SITE_PASS"))
            page.click('input[name="submit"]')
            page.wait_for_load_state("networkidle")
            page.goto(data_url)

            header_text = page.inner_text("h4.card-title")
            if "/" not in header_text:
                raise Exception("Could not find direction/date in page header.")
            direction, date_str = header_text.strip().split(" / ")
            plan_date = datetime.strptime(date_str, "%m.%d.%Y").date()

            today_et = datetime.now(UTC).astimezone(EDT).date()
            if plan_date != today_et:
                logger.warning(f"Site plan date {plan_date} does not match today ({today_et}) — skipping")
                return {"direction": None, "tickers": []}

            buttons = page.query_selector_all('button.btn.btn-primary.btn-block')
            tickers = [btn.inner_text().strip().upper() for btn in buttons if btn.inner_text().strip()]

            logger.info("===== FINAL TRADE PLAN =====")
            logger.info(f"Direction: {direction}")
            logger.info(f"Tickers: {tickers}")

            return {"direction": direction, "tickers": tickers}

    except Exception as e:
        logger.error(f"Error in get_trade_plan_from_site: {e}")
        return {"direction": None, "tickers": []}

def convert_site_data(site_data: dict) -> dict:
    direction = site_data.get("direction")
    tickers = site_data.get("tickers", [])
    if direction == "LONG":
        return {"buy": tickers, "short": []}
    elif direction == "SHORT":
        return {"buy": [], "short": tickers}
    return {"buy": [], "short": []}

def get_latest_price(ticker):
    quote = make_schwab_request("GET", f"/marketdata/v1/quotes?symbols={ticker}")
    if ticker not in quote or 'quote' not in quote[ticker]:
        logger.error(f"No quote found for {ticker}")
        raise ValueError(f"Quote unavailable: {ticker}")
    return float(quote[ticker]['quote']['lastPrice'])

def cancel_order(order_id, ticker):
    """Cancel a specific order by ID."""
    try:
        response = make_schwab_request("DELETE", f"/trader/v1/accounts/{ACCOUNT_ID}/orders/{order_id}")
        logger.warning(f"[TRADE] {ticker}: Order {order_id} canceled due to timeout")
        return True
    except Exception as e:
        logger.error(f"[TRADE] {ticker}: Failed to cancel order {order_id} - {e}")
        return False

def wait_for_fill(order_id, ticker):
    """Poll Schwab until the order is filled, or cancel and return None after timeout."""
    for attempt in range(300):  # Try for ~5m
        try:
            order = make_schwab_request("GET", f"/trader/v1/accounts/{ACCOUNT_ID}/orders/{order_id}")
            if isinstance(order, str):
                order = json.loads(order)

            status = order.get("status")
            if status == "FILLED":
                # Try to extract from execution legs
                try:
                    activities = order.get("orderActivityCollection", [])
                    if activities:
                        legs = activities[0].get("executionLegs", [])
                        if legs and "price" in legs[0]:
                            price = float(legs[0]["price"])
                            logger.success(f"[TRADE] {ticker}: Filled via legs @ ${price}")
                            return price
                except Exception as e:
                    logger.warning(f"[TRADE] {ticker}: Could not extract price from legs — {e}")

                # Fallback to filledPrice or price field
                fallback_price = float(order.get("filledPrice") or order.get("price", 0))
                if fallback_price:
                    logger.success(f"[TRADE] {ticker}: Filled via fallback @ ${fallback_price}")
                    return fallback_price
            elif status == "CANCELED":
                logger.warning(f"[TRADE] {ticker}: Order was canceled")
                return None
        except Exception as e:
            logger.warning(f"{ticker}: Fill check failed (attempt {attempt + 1}) - {e}")
        
        if attempt % 30 == 0 and attempt > 0:
            logger.info(f"[TRADE] {ticker}: Still waiting for fill... {attempt} seconds elapsed")
        time.sleep(1)
    
    # Timeout reached - cancel the order
    logger.error(f"[TRADE] {ticker}: Entry order did not fill in 5 minutes, canceling order {order_id}")
    cancel_order(order_id, ticker)
    return None

def get_bid_ask_prices(ticker):
    """Get current bid/ask prices for more accurate stop-loss placement."""
    try:
        quote = make_schwab_request("GET", f"/marketdata/v1/quotes?symbols={ticker}")
        if ticker not in quote or 'quote' not in quote[ticker]:
            return None, None
        
        quote_data = quote[ticker]['quote']
        bid = float(quote_data.get('bidPrice', 0))
        ask = float(quote_data.get('askPrice', 0))
        return bid, ask
    except Exception as e:
        logger.warning(f"[TRADE] {ticker}: Could not get bid/ask prices - {e}")
        return None, None

def place_trade(ticker, side, cash_value, allocation):
    """Enhanced place_trade function with demo mode support"""
    
    # Check if we're in demo mode
    demo_mode = get_demo_mode()
    if demo_mode:
        logger.info(f"[DEMO] 🎭 Demo mode active - simulating trade for {ticker}")
    
    price = get_latest_price(ticker)
    max_position = cash_value * allocation
    quantity = math.floor(max_position / price)
    if quantity < 1:
        logger.warning(f"[TRADE] Skipping {ticker}, too expensive for allocation")
        return

    logger.info(f"[TRADE] Placing entry order: {side} {quantity}x {ticker} @ market")

    if DRY_RUN:
        # Original dry run logic
        stop_loss = round(price * (1.01 if side == "SELL_SHORT" else 0.99), 2)
        take_profit = round(price * (0.97 if side == "SELL_SHORT" else 1.03), 2)
        logger.info(f"[TRADE] DRY RUN → {ticker}: {side}, qty={quantity}, stop={stop_loss}, limit={take_profit}")
        today_str = datetime.now(EDT).strftime("%Y-%m-%d")
        trade_row = {
            "date": today_str,
            "symbol": ticker,
            "action": side,
            "entry": price,
            "current": price,
            "exit": "",
            "pnl_pct": ""
        }
        append_trade_log([trade_row])
        return

    if demo_mode:
        # Demo mode simulation
        try:
            # Simulate realistic entry
            entry_result = demo_simulator.simulate_realistic_entry(ticker, side, quantity)
            
            if not entry_result["success"]:
                logger.error(f"[DEMO] Trade simulation failed: {entry_result.get('rejection_reason', 'Unknown error')}")
                return
            
            entry_price = entry_result["fill_price"]
            slippage = entry_result["slippage_pct"]
            
            logger.success(f"[DEMO] ✅ Simulated entry: {side} {quantity}x {ticker} @ ${entry_price:.4f} (slippage: {slippage:+.3f}%)")
            
            # Calculate stop loss and take profit with realistic pricing
            if side == "SELL_SHORT":
                stop_loss = round(entry_price * 1.01, 2)
                take_profit = round(entry_price * 0.97, 2)
            else:  # BUY
                stop_loss = round(entry_price * 0.99, 2)
                take_profit = round(entry_price * 1.03, 2)
            
            # Simulate bracket order placement
            bracket_result = demo_simulator.simulate_bracket_orders(
                entry_result, side, 0.01, 0.03  # 1% stop, 3% target
            )
            
            # Create trade log entry compatible with existing CSV format
            today_str = datetime.now(EDT).strftime("%Y-%m-%d")
            trade_row = {
                "date": today_str,
                "symbol": ticker,
                "action": side,
                "entry": entry_price,
                "current": entry_price,
                "exit": "",
                "pnl_pct": ""
            }
            
            append_trade_log([trade_row])
            
            logger.info(f"[DEMO] 🎯 Simulated bracket orders: Stop=${stop_loss}, Target=${take_profit}")
            return
            
        except Exception as e:
            logger.error(f"[DEMO] Demo simulation failed: {e}")
            # Fall back to dry run behavior
            logger.info("[DEMO] Falling back to dry run simulation")

    # STEP 1: Place entry market order
    entry_order = {
        "orderType": "MARKET",
        "session": "NORMAL",
        "duration": "DAY",
        "orderStrategyType": "SINGLE",
        "orderLegCollection": [{
            "instruction": side,
            "quantity": quantity,
            "instrument": {"symbol": ticker, "assetType": "EQUITY"}
        }]
    }

    entry_response = make_schwab_request("POST", f"/trader/v1/accounts/{ACCOUNT_ID}/orders", json=entry_order)
    if isinstance(entry_response, dict):
        order_id = entry_response.get("orderId") or entry_response.get("order_id")
    else:
        order_id = None

    if not order_id:
        logger.warning(f"[TRADE] {ticker}: No order ID returned. Trying fallback order fetch...")
        try:
            time.sleep(5)  # allow Schwab to register the order
            now_utc = datetime.now(timezone.utc).replace(microsecond=0)
            from_time = now_utc - timedelta(minutes=1)

            from_time_str = from_time.strftime("%Y-%m-%dT%H:%M:%SZ")
            to_time_str = now_utc.strftime("%Y-%m-%dT%H:%M:%SZ")

            orders = make_schwab_request(
                "GET",
                f"/trader/v1/accounts/{ACCOUNT_ID}/orders?fromEnteredTime={from_time_str}&toEnteredTime={to_time_str}"
            )

            logger.debug(f"[TRADE] {ticker}: Raw fallback Schwab response type: {type(orders)}")
            logger.debug(f"[TRADE] {ticker}: Raw fallback Schwab response:\n{orders}")

            with open(f"logs/{ticker}_fallback.json", "w") as f:
                json.dump(orders, f, indent=2, default=str)

            # Log raw fallback response
            logger.debug(f"[TRADE] {ticker}: Raw fallback Schwab orders response:\n{json.dumps(orders, indent=2)}")

            if isinstance(orders, str):
                try:
                    orders = json.loads(orders)
                except Exception as e:
                    logger.error(f"[TRADE] {ticker}: Fallback order fetch returned invalid JSON → {e}")
                    orders = []

            if not isinstance(orders, list):
                logger.error(f"[TRADE] {ticker}: Fallback orders response not a list → {orders}")
                return

            for order in sorted(orders, key=lambda x: x.get("enteredTime", ""), reverse=True):
                leg = order.get("orderLegCollection", [{}])[0]
                symbol = leg.get("instrument", {}).get("symbol", "")
                status = order.get("status")
                otype = order.get("orderType")

                logger.debug(f"[TRADE] {ticker}: Evaluating fallback order → symbol: {symbol}, status: {status}, type: {otype}")

                if symbol == ticker and status in ["QUEUED", "WORKING", "FILLED", "PENDING_ACTIVATION"] and otype == "MARKET":
                    order_id = order.get("orderId")
                    logger.success(f"[TRADE] {ticker}: Matched fallback Schwab order ID: {order_id}")
                    break

        except Exception as e:
            logger.error(f"[TRADE] {ticker}: Fallback order fetch failed: {e}")

    if not order_id:
        logger.error(f"[TRADE] {ticker}: Could not determine order ID. Entry failed.")
        return

    # STEP 2: Wait for fill (now with cancellation on timeout)
    entry_price = wait_for_fill(order_id, ticker)
    if not entry_price:
        logger.error(f"[TRADE] {ticker}: Order was not filled (either timeout or canceled).")
        return

    # STEP 3: Get current bid/ask and compute SL/TP targets with validation
    bid, ask = get_bid_ask_prices(ticker)
    
    # Calculate initial stop-loss and take-profit
    if side == "SELL_SHORT":
        initial_stop_loss = round(entry_price * 1.01, 2)
        take_profit = round(entry_price * 0.97, 2)
    else:  # BUY
        initial_stop_loss = round(entry_price * 0.99, 2)
        take_profit = round(entry_price * 1.03, 2)
    
    # FIXED: Validate and adjust stop-loss based on current bid/ask
    stop_loss = initial_stop_loss
    logger.info(f"[TRADE] {ticker}: Entry: ${entry_price}, Initial SL: ${initial_stop_loss}, Bid: ${bid}, Ask: ${ask}")
    
    if bid and ask:
        if side == "BUY":
            # For long positions with SELL STOP orders: stop price must be BELOW current bid
            # BUT also below current market price to be a proper stop-loss
            current_price = get_latest_price(ticker)  # Get fresh price after fill
            max_stop_price = min(bid - 0.01, current_price - 0.01)  # Must be below both bid and current price
            
            if initial_stop_loss >= max_stop_price:
                stop_loss = round(max_stop_price, 2)
                logger.warning(f"[TRADE] {ticker}: Adjusted stop-loss from {initial_stop_loss} to {stop_loss} (below bid {bid} and current price {current_price})")
            
            # Safety check: ensure stop-loss is actually below entry price
            if stop_loss >= entry_price:
                stop_loss = round(entry_price - 0.05, 2)  # Force it at least 5 cents below entry
                logger.warning(f"[TRADE] {ticker}: Stop-loss was above entry price, forced to {stop_loss}")
                
        else:  # SELL_SHORT
            # For short positions with BUY_TO_COVER STOP orders: stop price must be ABOVE current ask
            current_price = get_latest_price(ticker)  # Get fresh price after fill
            min_stop_price = max(ask + 0.01, current_price + 0.01)  # Must be above both ask and current price
            
            if initial_stop_loss <= min_stop_price:
                stop_loss = round(min_stop_price, 2)
                logger.warning(f"[TRADE] {ticker}: Adjusted stop-loss from {initial_stop_loss} to {stop_loss} (above ask {ask} and current price {current_price})")
            
            # Safety check: ensure stop-loss is actually above entry price
            if stop_loss <= entry_price:
                stop_loss = round(entry_price + 0.05, 2)  # Force it at least 5 cents above entry
                logger.warning(f"[TRADE] {ticker}: Stop-loss was below entry price, forced to {stop_loss}")
    else:
        logger.warning(f"[TRADE] {ticker}: Could not get bid/ask prices for stop-loss validation")
        # Fallback: use current market price for validation
        try:
            current_price = get_latest_price(ticker)
            if side == "BUY" and initial_stop_loss >= current_price:
                stop_loss = round(current_price - 0.05, 2)
                logger.warning(f"[TRADE] {ticker}: Fallback stop-loss adjustment to {stop_loss}")
            elif side == "SELL_SHORT" and initial_stop_loss <= current_price:
                stop_loss = round(current_price + 0.05, 2)
                logger.warning(f"[TRADE] {ticker}: Fallback stop-loss adjustment to {stop_loss}")
        except Exception as e:
            logger.error(f"[TRADE] {ticker}: Could not validate stop-loss: {e}")

    # STEP 4: Submit SL/TP as OCO exit order
    exit_instruction = "SELL" if side == "BUY" else "BUY_TO_COVER"
    # Try to place OCO order with retry logic for stop price issues
    max_retries = 3
    retry_delay = 2  # seconds
    
    for attempt in range(max_retries):
        try:
            oco_order = {
                "orderStrategyType": "OCO",
                "childOrderStrategies": [
                    {
                        "orderType": "LIMIT",
                        "price": str(take_profit),
                        "session": "NORMAL",
                        "duration": "DAY",
                        "orderStrategyType": "SINGLE",
                        "orderLegCollection": [{
                            "instruction": exit_instruction,
                            "quantity": quantity,
                            "instrument": {"symbol": ticker, "assetType": "EQUITY"}
                        }]
                    },
                    {
                        "orderType": "STOP",
                        "stopPrice": str(stop_loss),
                        "session": "NORMAL",
                        "duration": "DAY",
                        "orderStrategyType": "SINGLE",
                        "orderLegCollection": [{
                            "instruction": exit_instruction,
                            "quantity": quantity,
                            "instrument": {"symbol": ticker, "assetType": "EQUITY"}
                        }]
                    }
                ]
            }

            res = make_schwab_request("POST", f"/trader/v1/accounts/{ACCOUNT_ID}/orders", json=oco_order)

            if res and (res.get("orderId") or res.get("order_id") or res.get("status_code") in [200, 201]):
                logger.success(f"[TRADE] {ticker}: OCO SL/TP submitted ✅ SL: {stop_loss}, TP: {take_profit}")
                break
            else:
                # Check if it's a stop price validation error
                error_msg = str(res) if res else "Unknown error"
                if "stop price" in error_msg.lower() and attempt < max_retries - 1:
                    logger.warning(f"[TRADE] {ticker}: Stop price validation failed, retrying with updated prices (attempt {attempt + 1})")
                    time.sleep(retry_delay)
                    
                    # Get fresh bid/ask prices and recalculate stop-loss
                    bid, ask = get_bid_ask_prices(ticker)
                    if bid and ask:
                        if side == "BUY":
                            stop_loss = round(bid - 0.02, 2)  # More conservative buffer
                        else:  # SELL_SHORT
                            stop_loss = round(ask + 0.02, 2)  # More conservative buffer
                        logger.info(f"[TRADE] {ticker}: Updated stop-loss to {stop_loss} based on fresh bid/ask")
                    continue
                else:
                    logger.error(f"[TRADE] {ticker}: OCO SL/TP failed → {res}")
                    break
        except Exception as e:
            logger.error(f"[TRADE] {ticker}: OCO order placement exception → {e}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
                continue
            break

    today_str = datetime.now(EDT).strftime("%Y-%m-%d")
    trade_row = {
        "date": today_str,
        "symbol": ticker,
        "action": side,
        "entry": entry_price,
        "current": entry_price,
        "exit": "",
        "pnl_pct": ""
    }
    append_trade_log([trade_row])

def main():
    """Simplified main function - only reads from cache, doesn't fetch"""
    
    # Simply load from cache - if it doesn't exist, that's an error condition
    try:
        data = get_trade_plan_from_cache()
        
        if not data.get("buy") and not data.get("short"):
            logger.error("No trade plan found in cache. Run trade plan refresh first.")
            logger.error("Either:")
            logger.error("  1. Visit dashboard and click 'Refresh Trade Plan'")
            logger.error("  2. Run trade plan fetch manually")
            logger.error("  3. Ensure trade_plan.json exists in ./data/ folder")
            return
            
        logger.info(f"Loaded trade plan from cache: {len(data.get('buy', []))} BUY, {len(data.get('short', []))} SHORT")
        
    except Exception as e:
        logger.error(f"Failed to load trade plan from cache: {e}")
        logger.error("Trade plan cache is missing or corrupted.")
        logger.error("Please refresh the trade plan from the dashboard first.")
        return

    # Rest of your existing main() function stays exactly the same...
    account_info = make_schwab_request("GET", "/trader/v1/accounts")
    if not isinstance(account_info, list) or "securitiesAccount" not in account_info[0]:
        logger.error("Invalid account response from Schwab")
        return

    balances = account_info[0]['securitiesAccount'].get('currentBalances', {})
    cash = (
        balances.get('availableFundsNonMarginableTrade') or
        balances.get('cashAvailableForTrading') or
        balances.get('cashBalance')
    )

    if cash is None:
        logger.error("Unable to determine cash balance. Account info:\n" + json.dumps(account_info, indent=2))
        logger.error("Aborting trade placement due to missing cash balance.")
        return

    allocation = get_allocation()
    logger.info(f"Cash Available: {cash}, Allocation: {allocation}")

    all_trades = []
    if "buy" in data:
        all_trades += [("BUY", t) for t in data["buy"]]
    if "short" in data:
        all_trades += [("SELL_SHORT", t) for t in data["short"]]

    logger.info(f"Launching {len(all_trades)} trade threads...")

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(place_trade, ticker, side, cash, allocation) for side, ticker in all_trades]
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logger.error(f"Threaded trade failed: {e}")

if __name__ == "__main__":
    main()
