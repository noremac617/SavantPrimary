import os
import json
import requests
import pandas as pd
from datetime import datetime
from zoneinfo import ZoneInfo
from loguru import logger
from io import StringIO

EDT = ZoneInfo("America/New_York")

def get_trade_plan_from_google_sheet():
    """
    Fetch trade plan from Google Sheets and save to trade_plan.json
    
    NEW Sheet layout:
    C1: date
    D1: direction1, E1: ticker1
    F1: direction2, G1: ticker2  
    H1: direction3, I1: ticker3
    etc.
    
    Row 2: actual values
    
    Returns: dict with "buy" and "short" keys containing lists of tickers
    """
    sheet_id = os.getenv("GOOGLE_SHEET_ID")
    
    if not sheet_id:
        logger.error("GOOGLE_SHEET_ID not found in environment variables")
        return {"buy": [], "short": []}
    
    try:
        # Get CSV export of the sheet
        url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv"
        logger.info(f"Fetching trade plan from Google Sheets...")
        
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        
        # Read CSV data
        df = pd.read_csv(StringIO(response.text))
        
        # Get today's date in the same format as your sheet
        today = datetime.now(EDT).date()
        
        # Find today's data - check multiple possible date formats
        todays_data = None
        
        # Look through rows to find today's date
        for index, row in df.iterrows():
            try:
                # Get the date from column C (index 2)
                date_value = row.iloc[2] if len(row) > 2 else None
                
                if pd.isna(date_value):
                    continue
                
                # Try to parse the date - handle various formats
                if isinstance(date_value, str):
                    # Common formats: "2025-01-08", "1/8/2025", "01/08/2025"
                    try:
                        if "/" in date_value:
                            # Handle M/D/YYYY or MM/DD/YYYY
                            parsed_date = pd.to_datetime(date_value).date()
                        else:
                            # Handle YYYY-MM-DD
                            parsed_date = pd.to_datetime(date_value).date()
                    except:
                        continue
                else:
                    # Already a date/datetime object
                    parsed_date = pd.to_datetime(date_value).date()
                
                if parsed_date == today:
                    todays_data = row
                    break
                    
            except Exception as e:
                logger.debug(f"Error parsing date in row {index}: {e}")
                continue
        
        if todays_data is None:
            logger.warning(f"No data found for today ({today}) in Google Sheet")
            return {"buy": [], "short": []}
        
        # NEW: Parse alternating direction/ticker pairs starting from column D (index 3)
        buy_tickers = []
        short_tickers = []
        
        # Start from column D (index 3) and process in pairs
        for i in range(3, len(todays_data), 2):  # Step by 2 to get direction columns
            if i >= len(todays_data):
                break
                
            # Get direction from current column
            direction = todays_data.iloc[i] if i < len(todays_data) else None
            # Get ticker from next column  
            ticker = todays_data.iloc[i + 1] if (i + 1) < len(todays_data) else None
            
            # Skip if either is missing
            if pd.isna(direction) or pd.isna(ticker):
                continue
                
            direction = str(direction).strip().upper()
            ticker = str(ticker).strip().upper()
            
            # Skip if either is empty
            if not direction or not ticker:
                continue
            
            # Categorize by direction
            if direction in ["LONG", "BUY"]:
                buy_tickers.append(ticker)
                logger.info(f"Found BUY: {ticker}")
            elif direction in ["SHORT", "SELL", "SELL_SHORT"]:
                short_tickers.append(ticker)
                logger.info(f"Found SHORT: {ticker}")
            else:
                logger.warning(f"Unknown direction '{direction}' for ticker {ticker}, skipping")
        
        logger.info("===== TRADE PLAN FROM GOOGLE SHEETS =====")
        logger.info(f"Date: {today}")
        logger.info(f"BUY tickers: {buy_tickers}")
        logger.info(f"SHORT tickers: {short_tickers}")
        
        # Create the trade plan data in the expected format
        trade_plan_data = {
            "buy": buy_tickers,
            "short": short_tickers
        }
        
        # Save to trade_plan.json for compatibility with existing system
        save_trade_plan_to_json(trade_plan_data)
        
        return trade_plan_data
        
    except Exception as e:
        logger.error(f"Error fetching trade plan from Google Sheets: {e}")
        return {"buy": [], "short": []}

def convert_site_data(site_data: dict) -> dict:
    """
    Convert OLD format trade plan data to the format used by your existing system
    This is for backward compatibility with the old single-direction format
    """
    # If already in new format, return as-is
    if "buy" in site_data and "short" in site_data:
        return site_data
        
    # Handle old format
    direction = site_data.get("direction")
    tickers = site_data.get("tickers", [])
    
    if direction in ["LONG", "BUY"]:
        return {"buy": tickers, "short": []}
    elif direction in ["SHORT", "SELL", "SELL_SHORT"]:
        return {"buy": [], "short": tickers}
    else:
        return {"buy": [], "short": []}

def save_trade_plan_to_json(trade_plan_data: dict):
    """
    Save trade plan to ./data/trade_plan.json in the format expected by the rest of the app
    """
    try:
        # Ensure data directory exists
        os.makedirs("./data", exist_ok=True)
        
        # The trade_plan_data should already be in the correct format
        # but ensure it has the required keys
        formatted_data = {
            "buy": trade_plan_data.get("buy", []),
            "short": trade_plan_data.get("short", [])
        }
        
        # ALWAYS save to ./data/trade_plan.json (consistent path)
        json_path = "./data/trade_plan.json"
        with open(json_path, "w") as f:
            json.dump(formatted_data, f, indent=2)
        
        logger.info(f"Trade plan saved to {json_path}: {formatted_data}")
        
    except Exception as e:
        logger.error(f"Failed to save trade_plan.json: {e}")

def get_trade_plan_from_cache():
    """
    Load trade plan from trade_plan.json cache
    """
    try:
        with open("./data/trade_plan.json", "r") as f:
            return json.load(f)
    except Exception as e:
        logger.warning(f"Failed to load trade plan from cache: {e}")
        return {"buy": [], "short": []}

# For backward compatibility, provide the original function name as well
def get_trade_plan_from_site():
    """
    Backward compatibility function - now uses Google Sheets instead of website
    """
    return get_trade_plan_from_google_sheet()