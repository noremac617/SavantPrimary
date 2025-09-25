import os
import sys
import time
import csv
import json
import pandas as pd 
from loguru import logger
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from schwab_api import make_schwab_request
from config import get_demo_mode
from demo_mode import DemoModeSimulator

# Consistent path for trades CSV
TRADES_CSV_PATH = "./data/trades.csv"
if not os.path.exists("./data"):
    TRADES_CSV_PATH = "trades.csv"

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
    level="INFO",
    format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}"
)

# Console logger (also Eastern Time)
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

def get_open_positions():
    endpoint = f"/trader/v1/accounts/{ACCOUNT_ID}?fields=positions"
    response = make_schwab_request("GET", endpoint)
    if not isinstance(response, dict) or "securitiesAccount" not in response:
        logger.error("Could not retrieve positions.")
        return []
    return response["securitiesAccount"].get("positions", [])

def cancel_open_orders():
    logger.info("[TRADE] Canceling open orders before closing positions...")
    now = datetime.now(timezone.utc)
    from_time = (now - timedelta(days=1)).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    to_time = now.replace(microsecond=0).isoformat().replace("+00:00", "Z")
    endpoint = f"/trader/v1/accounts/{ACCOUNT_ID}/orders?fromEnteredTime={from_time}&toEnteredTime={to_time}"
    response = make_schwab_request("GET", endpoint)

    if not isinstance(response, list):
        logger.warning("No open orders retrieved.")
        return

    for order in response:
        order_id = order.get("orderId")
        status = order.get("status")
        if status in ("WORKING", "PENDING_ACTIVATION"):
            logger.info(f"[TRADE] Attempting to cancel order {order_id} with status {status}...")
            cancel_endpoint = f"/trader/v1/accounts/{ACCOUNT_ID}/orders/{order_id}"
            cancel_response = make_schwab_request("DELETE", cancel_endpoint)

            if isinstance(cancel_response, dict) and cancel_response.get("error"):
                logger.warning(f"[TRADE] Cancel error for order {order_id}: {cancel_response['error']}")
            else:
                logger.info(f"[TRADE] Canceled order {order_id} → {cancel_response}")

def update_trade_csv(symbol, exit_price, trade_type="manual"):
    """Centralized function to update exit prices in CSV"""
    try:
        if not os.path.exists(TRADES_CSV_PATH):
            logger.warning(f"CSV file not found: {TRADES_CSV_PATH}")
            return False
            
        df = pd.read_csv(TRADES_CSV_PATH)
        original_columns = df.columns.tolist()
        
        # Normalize column names for processing
        df.columns = df.columns.str.strip().str.lower()
        
        # Debug: Show what we're looking for
        logger.debug(f"Looking for symbol: {symbol}, exit_price: {exit_price}")
        logger.debug(f"DataFrame columns: {df.columns.tolist()}")
        
        # More flexible exit condition checking - handles empty CSV cells
        def is_exit_missing(val):
            # Handle pandas NaN
            if pd.isna(val):
                return True
            # Handle empty strings
            if isinstance(val, str) and val.strip() == "":
                return True
            # Handle actual empty values that pandas might read as various types
            if val == "" or val is None:
                return True
            try:
                float_val = float(val)
                return float_val == 0.0
            except (ValueError, TypeError):
                return True
        
        # Find rows to update (symbol matches and exit is empty/null)
        mask = (df["symbol"] == symbol) & df["exit"].apply(is_exit_missing)
        
        if not mask.any():
            logger.warning(f"No matching rows found for {symbol} with missing exit price")
            logger.debug(f"Available symbols: {df['symbol'].unique()}")
            logger.debug(f"Exit values for {symbol}: {df[df['symbol'] == symbol]['exit'].values}")
            return False
        
        matching_rows = df[mask]
        logger.info(f"Found {len(matching_rows)} rows to update for {symbol}")
        
        # Update exit price
        df.loc[mask, "exit"] = exit_price
        df.loc[mask, "current"] = exit_price
        
        # Calculate PnL
        for idx in df[mask].index:
            try:
                entry = float(df.loc[idx, "entry"])
                action = str(df.loc[idx, "action"]).strip().upper()
                
                if action in ("BUY", "BUY_TO_COVER"):
                    pnl = ((exit_price - entry) / entry) * 100
                elif action in ("SELL", "SELL_SHORT"):
                    pnl = ((entry - exit_price) / entry) * 100
                else:
                    logger.warning(f"Unknown action type: {action}")
                    pnl = 0
                
                df.loc[idx, "pnl_pct"] = round(pnl, 2)
                logger.debug(f"Updated row {idx}: {symbol} {action} entry={entry} exit={exit_price} pnl={pnl:.2f}%")
                
            except Exception as e:
                logger.error(f"Error calculating PnL for row {idx}: {e}")
        
        # Restore original column names
        df.columns = original_columns
        
        # Save back to CSV
        df.to_csv(TRADES_CSV_PATH, index=False)
        logger.success(f"✅ Updated exit price for {symbol}: ${exit_price:.2f} ({trade_type})")
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to update CSV for {symbol}: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False

def close_position(symbol, quantity, action, market_value):
    if quantity <= 0:
        return

    if DRY_RUN:
        logger.info("[TRADE] DRY RUN: No order placed.")
        return

    logger.info(f"[TRADE] Submitting MARKET {action} order to close {quantity}x {symbol}...")

    payload = {
        "orderType": "MARKET",
        "session": "NORMAL",
        "duration": "DAY",
        "orderStrategyType": "SINGLE",
        "orderLegCollection": [{
            "instruction": action,
            "quantity": quantity,
            "instrument": {
                "symbol": symbol,
                "assetType": "EQUITY"
            }
        }]
    }

    response = make_schwab_request("POST", f"/trader/v1/accounts/{ACCOUNT_ID}/orders", json=payload)
    if not isinstance(response, dict) or response.get("error"):
        logger.error(f"[TRADE] Failed to place close order for {symbol}")
        return

    # Wait a bit for order to process
    time.sleep(3)

    # Look for the filled order
    filled_price = get_recent_fill_price(symbol, action)
    
    if filled_price:
        logger.success(f"[TRADE] TRADE CLOSED: {action} {quantity}x {symbol} @ ${filled_price:.2f}")
        update_trade_csv(symbol, filled_price, "manual_close")
    else:
        logger.warning(f"[TRADE] Could not find fill price for {symbol}, using market value estimate")
        estimate_price = market_value / quantity if quantity > 0 else market_value
        update_trade_csv(symbol, estimate_price, "manual_close_estimate")

def get_recent_fill_price(symbol, action, lookback_minutes=5):
    """Get the most recent fill price for a symbol/action combination"""
    try:
        now = datetime.now(timezone.utc).replace(microsecond=0)
        from_time = (now - timedelta(minutes=lookback_minutes)).isoformat().replace("+00:00", "Z")
        to_time = (now + timedelta(minutes=2)).isoformat().replace("+00:00", "Z")
        
        endpoint = f"/trader/v1/accounts/{ACCOUNT_ID}/orders?fromEnteredTime={from_time}&toEnteredTime={to_time}"
        orders_response = make_schwab_request("GET", endpoint)

        if not isinstance(orders_response, list):
            logger.warning(f"No recent orders found for {symbol}")
            return None

        # Sort by entered time (most recent first)
        orders_response.sort(key=lambda x: x.get("enteredTime", ""), reverse=True)

        for order in orders_response:
            if order.get("status") != "FILLED":
                continue
                
            legs = order.get("orderLegCollection", [])
            if not legs:
                continue

            leg = legs[0]
            order_symbol = leg.get("instrument", {}).get("symbol")
            order_action = leg.get("instruction")
            
            if order_symbol == symbol and order_action == action:
                # Try to get fill price from execution legs
                try:
                    activities = order.get("orderActivityCollection", [])
                    if activities:
                        exec_legs = activities[0].get("executionLegs", [])
                        if exec_legs and "price" in exec_legs[0]:
                            return float(exec_legs[0]["price"])
                except Exception as e:
                    logger.debug(f"Could not extract price from execution legs: {e}")
                
                # Fallback to order level price
                if order.get("filledPrice"):
                    return float(order["filledPrice"])
                elif order.get("price"):
                    return float(order["price"])
        
        return None
        
    except Exception as e:
        logger.error(f"Error getting recent fill price for {symbol}: {e}")
        return None

def get_all_filled_exit_orders(lookback_days=1):
    """Get ALL filled exit orders from recent days - both manual closes and bracket orders"""
    EDT = ZoneInfo("America/New_York")
    today = datetime.now(EDT)
    
    # Extend lookback to catch orders from previous days
    from_time = today - timedelta(days=lookback_days)
    to_time = today + timedelta(hours=1)  # Add buffer for timezone issues
    
    # Convert to UTC for API
    from_time_utc = from_time.astimezone(UTC)
    to_time_utc = to_time.astimezone(UTC)

    endpoint = f"/trader/v1/accounts/{ACCOUNT_ID}/orders"
    params = {
        "fromEnteredTime": from_time_utc.isoformat().replace("+00:00", "Z"),
        "toEnteredTime": to_time_utc.isoformat().replace("+00:00", "Z")
    }

    logger.info(f"🔍 Fetching orders from {from_time_utc} to {to_time_utc}")
    response = make_schwab_request("GET", endpoint, params=params)
    
    if not response or not isinstance(response, list):
        logger.error(f"Failed to fetch orders: {response}")
        return []

    logger.info(f"🔍 Fetched {len(response)} total orders")
    filled_exits = []

    for order in response:
        if not isinstance(order, dict):
            continue

        try:
            status = order.get("status")
            if status != "FILLED":
                continue

            # Handle OCO bracket orders
            if order.get("orderStrategyType") == "OCO":
                children = order.get("childOrderStrategies", [])
                for child in children:
                    if child.get("status") == "FILLED":
                        leg = child.get("orderLegCollection", [{}])[0]
                        symbol = leg.get("instrument", {}).get("symbol")
                        action = leg.get("instruction")
                        
                        if action in ("SELL", "BUY_TO_COVER"):
                            price = extract_fill_price(child)
                            if symbol and price:
                                filled_exits.append({
                                    "symbol": symbol,
                                    "action": action,
                                    "price": price,
                                    "time": child.get("closeTime") or child.get("enteredTime"),
                                    "type": "bracket"
                                })
                                logger.info(f"📊 Found bracket exit: {symbol} {action} @ ${price}")

            # Handle single orders (manual closes)
            elif order.get("orderStrategyType") == "SINGLE":
                leg = order.get("orderLegCollection", [{}])[0]
                symbol = leg.get("instrument", {}).get("symbol")
                action = leg.get("instruction")
                
                # Only process exit actions
                if action in ("SELL", "BUY_TO_COVER"):
                    price = extract_fill_price(order)
                    if symbol and price:
                        filled_exits.append({
                            "symbol": symbol,
                            "action": action,
                            "price": price,
                            "time": order.get("closeTime") or order.get("enteredTime"),
                            "type": "manual"
                        })
                        logger.info(f"📊 Found manual exit: {symbol} {action} @ ${price}")

        except Exception as e:
            logger.warning(f"⚠️ Error processing order: {e}")
            continue

    logger.info(f"✅ Found {len(filled_exits)} total exit orders")
    return filled_exits

def extract_fill_price(order):
    """Extract fill price from order with multiple fallback methods"""
    try:
        # Method 1: Try execution legs first (most accurate)
        activities = order.get("orderActivityCollection", [])
        if activities:
            exec_legs = activities[0].get("executionLegs", [])
            if exec_legs and "price" in exec_legs[0]:
                return float(exec_legs[0]["price"])
        
        # Method 2: Try filledPrice
        if order.get("filledPrice"):
            return float(order["filledPrice"])
        
        # Method 3: Try price field
        if order.get("price"):
            return float(order["price"])
        
        # Method 4: Try averageFillPrice
        if order.get("averageFillPrice"):
            return float(order["averageFillPrice"])
        
        return None
        
    except Exception as e:
        logger.debug(f"Error extracting fill price: {e}")
        return None

def patch_exit_prices():
    """Patch all missing exit prices from recent filled orders"""
    if not os.path.exists(TRADES_CSV_PATH):
        logger.warning(f"CSV file not found: {TRADES_CSV_PATH}")
        return

    try:
        df = pd.read_csv(TRADES_CSV_PATH)
        df.columns = df.columns.str.strip().str.lower()
        
        # More robust check for missing exit prices - handles empty CSV cells
        def is_exit_missing(val):
            # Handle pandas NaN
            if pd.isna(val):
                return True
            # Handle empty strings
            if isinstance(val, str) and val.strip() == "":
                return True
            # Handle actual empty values that pandas might read as various types
            if val == "" or val is None:
                return True
            try:
                float_val = float(val)
                return float_val == 0.0
            except (ValueError, TypeError):
                return True
        
        missing_exits = df[df["exit"].apply(is_exit_missing)]
        
        if missing_exits.empty:
            logger.info("ℹ️ No missing exit prices found")
            return
        
        logger.info(f"🔍 Found {len(missing_exits)} trades missing exit prices:")
        for _, row in missing_exits.iterrows():
            logger.info(f"  - {row['symbol']} ({row['action']}) entered @ ${row['entry']}")
        
        # Get all filled exit orders (look back further)
        exit_orders = get_all_filled_exit_orders(lookback_days=2)
        
        if not exit_orders:
            logger.warning("⚠️ No exit orders found")
            return
        
        updated_count = 0
        
        # Group exit orders by symbol for better matching
        exit_by_symbol = {}
        for order in exit_orders:
            symbol = order["symbol"]
            if symbol not in exit_by_symbol:
                exit_by_symbol[symbol] = []
            exit_by_symbol[symbol].append(order)
        
        # Match exit orders to missing trades
        for symbol, orders in exit_by_symbol.items():
            # Sort orders by time (most recent first)
            orders.sort(key=lambda x: x.get("time", ""), reverse=True)
            
            for order in orders:
                price = order["price"]
                order_type = order["type"]
                
                # Check if this symbol still has missing exit prices
                current_df = pd.read_csv(TRADES_CSV_PATH)
                current_df.columns = current_df.columns.str.strip().str.lower()
                
                mask = (current_df["symbol"] == symbol) & current_df["exit"].apply(is_exit_missing)
                
                if mask.any():
                    success = update_trade_csv(symbol, price, order_type)
                    if success:
                        updated_count += 1
                        break  # Only use the most recent exit for each symbol
        
        if updated_count > 0:
            logger.success(f"✅ Successfully patched {updated_count} exit prices")
        else:
            logger.info("ℹ️ No exit prices were patched")
            
    except Exception as e:
        logger.error(f"❌ Error in patch_exit_prices: {e}")
        import traceback
        logger.error(traceback.format_exc())

def debug_csv_and_orders():
    """Debug function to inspect CSV and orders"""
    logger.info("🔍 DEBUG: Inspecting CSV and orders...")
    
    # Check CSV
    if os.path.exists(TRADES_CSV_PATH):
        df = pd.read_csv(TRADES_CSV_PATH)
        logger.info(f"📊 CSV has {len(df)} rows")
        logger.info(f"📊 Columns: {df.columns.tolist()}")
        
        df.columns = df.columns.str.strip().str.lower()
        missing_exits = df[df["exit"].isna() | (df["exit"] == "") | (df["exit"] == 0.0)]
        logger.info(f"📊 Rows with missing exits: {len(missing_exits)}")
        
        if not missing_exits.empty:
            logger.info("📊 Missing exit details:")
            for _, row in missing_exits.iterrows():
                logger.info(f"  - {row['symbol']} | Action: {row['action']} | Entry: ${row['entry']} | Exit: '{row['exit']}'")
    
    # Check orders
    exit_orders = get_all_filled_exit_orders(lookback_days=2)
    logger.info(f"📊 Found {len(exit_orders)} exit orders")
    
    for order in exit_orders:
        logger.info(f"📊 Order: {order['symbol']} {order['action']} @ ${order['price']} ({order['type']})")

def main():
    """Enhanced main function with demo mode support"""
    demo_mode = get_demo_mode()
    
    if demo_mode:
        logger.info("[DEMO] 🎭 Demo mode active - simulating position closure")
        
        # Initialize demo simulator
        demo_simulator = DemoModeSimulator()
        
        # Simulate market close exits for demo trades
        exit_results = demo_simulator.simulate_market_close_exits()
        
        if exit_results["exits"]:
            logger.info(f"[DEMO] ✅ Simulated closing {len(exit_results['exits'])} demo positions at market close")
            for exit in exit_results["exits"]:
                logger.info(f"[DEMO]   - {exit['symbol']}: ${exit['exit_price']:.4f} ({exit['pnl_pct']:+.2f}%)")
        else:
            logger.info("[DEMO] ℹ️ No open demo positions to close")
        
        return
    
    # Original live trading logic continues below...
    logger.info("[TRADE] Starting position closure process...")
    cancel_open_orders()
    time.sleep(1)

    positions = get_open_positions()
    if not positions:
        logger.info("[TRADE] No open positions found to close.")
        
        # Even if no positions to close, try to patch exit prices
        logger.info("[TRADE] Attempting to patch any missing exit prices...")
        patch_exit_prices()
        return

    logger.info(f"[TRADE] Found {len(positions)} open positions to close.")

    for pos in positions:
        symbol = pos.get("instrument", {}).get("symbol")
        qty = int(pos.get("longQuantity") or pos.get("shortQuantity", 0))
        mv = float(pos.get("marketValue", 0))
        action = "SELL" if pos.get("longQuantity") else "BUY_TO_COVER"

        logger.info(f"[TRADE] Preparing to close {qty}x {symbol} ({action}) with est. value ${mv:.2f}")
        close_position(symbol, qty, action, mv)

    logger.info("[TRADE] Completed position closure process.")
    
    # Wait a bit for orders to settle, then patch
    time.sleep(5)
    logger.info("[TRADE] Attempting to patch any remaining missing exit prices...")
    patch_exit_prices()

if __name__ == "__main__":
    # Add debug flag for troubleshooting
    if len(sys.argv) > 1 and sys.argv[1] == "--debug":
        debug_csv_and_orders()
    else:
        main()
        # Always try to patch any missing exit prices
        patch_exit_prices()
