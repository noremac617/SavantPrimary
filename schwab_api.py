import os
import json
import tempfile
import time
import base64
import requests
import yfinance as yf
from datetime import datetime, timezone
from loguru import logger
from dotenv import load_dotenv
from threading import Lock
from zoneinfo import ZoneInfo

load_dotenv()

APP_KEY = os.getenv('APP_KEY')
APP_SECRET = os.getenv('APP_SECRET')
ACCOUNT_ID = os.getenv('ACCOUNT_ID')
TOKEN_FILE = os.getenv('TOKEN_FILE', './data/token.json')
_refresh_lock = Lock()

UTC = timezone.utc
EDT = ZoneInfo("America/New_York")

def load_tokens():
    with open(TOKEN_FILE, 'r') as f:
        return json.load(f)

def safe_save_tokens(tokens: dict):
    """Atomically write tokens to TOKEN_FILE to avoid corruption across workers."""
    dirpath = os.path.dirname(TOKEN_FILE) or "."
    os.makedirs(dirpath, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", dir=dirpath, delete=False) as tf:
        json.dump(tokens, tf, indent=4)
        tf.flush()
        os.fsync(tf.fileno())
        tmpname = tf.name
    os.replace(tmpname, TOKEN_FILE)

def save_tokens(tokens):
    safe_save_tokens(tokens)

def is_token_expired(tokens, skew=120):
    """Check if access token is expired or about to expire"""
    issued = tokens.get('issued_at', time.time())
    ttl = int(tokens.get('expires_in', 1800))
    return (time.time() - issued) >= max(0, ttl - skew)

def is_refresh_token_near_expiry(tokens, days_before_expiry=1.5):
    """
    Check if refresh token is close to the 7-day expiry.
    
    Since Schwab doesn't extend the window reliably, we need to force 
    re-authentication before the 7-day limit.
    """
    # Use the ORIGINAL authentication time, not last refresh time
    # because Schwab isn't extending the window
    original_auth_time = tokens.get('original_auth_time', tokens.get('issued_at', time.time()))
    age_seconds = time.time() - original_auth_time
    age_days = age_seconds / (24 * 60 * 60)
    
    expires_in_days = 7 - age_days
    needs_reauth = expires_in_days <= days_before_expiry
    
    logger.debug(f"🔍 REFRESH TOKEN EXPIRY CHECK:")
    logger.debug(f"  Original auth time: {datetime.fromtimestamp(original_auth_time, EDT)}")
    logger.debug(f"  Age since original auth: {age_days:.2f} days")
    logger.debug(f"  Expires in: {expires_in_days:.2f} days")
    logger.debug(f"  Needs re-auth (threshold {days_before_expiry} days): {needs_reauth}")
    
    return needs_reauth

def mark_tokens_invalid():
    """Mark tokens as invalid and require re-authentication"""
    try:
        # Create a backup of the invalid tokens for debugging
        tokens = load_tokens()
        backup_file = TOKEN_FILE + ".invalid_backup"
        with open(backup_file, 'w') as f:
            json.dump(tokens, f, indent=4)
        logger.info(f"Backed up invalid tokens to {backup_file}")
        
        # Remove the token file to force re-authentication
        if os.path.exists(TOKEN_FILE):
            os.remove(TOKEN_FILE)
            logger.warning("🔴 Removed invalid token file - manual re-authentication required")
        
    except Exception as e:
        logger.error(f"Error marking tokens as invalid: {e}")

def refresh_tokens():
    """
    Refresh access token using refresh token.
    
    NOTE: Schwab typically does NOT provide new refresh tokens that extend 
    the 7-day window, so we track the original authentication time.
    """
    try:
        if not os.path.exists(TOKEN_FILE):
            logger.error("No token file found - authentication required")
            return None
            
        tokens = load_tokens()
        old_refresh = tokens.get("refresh_token")
        
        if not old_refresh:
            logger.error("No refresh token available in token file")
            mark_tokens_invalid()
            return None

        # Check if we're close to the 7-day limit
        if is_refresh_token_near_expiry(tokens, days_before_expiry=1.5):
            logger.warning("🔴 Refresh token is near expiry (within 1.5 days)")
            logger.warning("🔴 Schwab's API doesn't reliably extend the 7-day window")
            logger.warning("🔴 Manual re-authentication will be required soon")
            # Continue with refresh attempt, but warn user

        auth_header = base64.b64encode(f"{APP_KEY}:{APP_SECRET}".encode()).decode()
        headers = {
            "Authorization": f"Basic {auth_header}",
            "Content-Type": "application/x-www-form-urlencoded"
        }
        data = {
            "grant_type": "refresh_token",
            "refresh_token": old_refresh
        }

        logger.info("🔄 Attempting to refresh Schwab tokens...")
        response = requests.post("https://api.schwabapi.com/v1/oauth/token", 
                               headers=headers, data=data, timeout=30)
        
        if not response.ok:
            error_text = response.text
            logger.error(f"Token refresh failed: {response.status_code} {error_text}")
            
            # Parse specific error conditions
            if response.status_code == 400:
                # Common 400 errors that require re-authentication
                invalid_token_errors = [
                    "unsupported_token_type",
                    "invalid_grant", 
                    "refresh_token_authentication_error",
                    "invalid_request",
                    "invalid_client"
                ]
                
                if any(error in error_text for error in invalid_token_errors):
                    logger.error("🔴 Refresh token has expired after 7 days")
                    logger.error("💡 This is expected behavior - Schwab doesn't extend the window")
                    mark_tokens_invalid()
                    return None
            
            elif response.status_code == 401:
                logger.error("🔴 Unauthorized - refresh token rejected")
                mark_tokens_invalid()
                return None
            
            # For other errors, don't delete tokens but still fail
            logger.error(f"🔴 Token refresh failed with HTTP {response.status_code}")
            return None

        refreshed = response.json()
        now = time.time()
        
        # Check if Schwab provided a new refresh token
        new_refresh = refreshed.get("refresh_token")
        if not new_refresh:
            # Fallback to old refresh token if Schwab doesn't provide a new one
            new_refresh = old_refresh
            logger.warning("⚠️ Schwab didn't provide new refresh token")
        
        # CRITICAL: Preserve the original authentication time
        # Since Schwab doesn't extend the window, we need to track when 
        # the user originally authenticated to know when re-auth is needed
        original_auth_time = tokens.get('original_auth_time', tokens.get('issued_at', now))
        
        expires_in = int(refreshed.get("expires_in", 1800))

        # Create new token data with updated timestamps
        merged = {
            **tokens,  # Keep existing data
            **refreshed,  # Update with new token data
            "refresh_token": new_refresh,
            "issued_at": now,
            "last_refresh_time": now,
            "original_auth_time": original_auth_time,  # PRESERVE ORIGINAL TIME
            "expires_in": expires_in,
        }
        
        save_tokens(merged)
        
        # Log the refresh token status
        old_suffix = (old_refresh or "")[-8:]
        new_suffix = (new_refresh or "")[-8:]
        
        if old_suffix != new_suffix:
            logger.success(f"✅ Token refreshed with NEW refresh token (...{old_suffix} → ...{new_suffix})")
            logger.success("🎯 7-day window has been EXTENDED!")
        else:
            logger.success(f"✅ Token refreshed with same refresh token (...{new_suffix})")
            logger.warning("⚠️ 7-day window NOT extended (typical Schwab behavior)")
            
            # Calculate remaining time until re-auth needed
            age_days = (now - original_auth_time) / (24 * 60 * 60)
            remaining_days = 7 - age_days
            logger.info(f"📅 Days until re-authentication needed: {remaining_days:.1f}")
            
        return merged
        
    except FileNotFoundError:
        logger.error("Token file not found - authentication required")
        return None
    except json.JSONDecodeError as e:
        logger.error(f"Token file is corrupted: {e}")
        mark_tokens_invalid()
        return None
    except Exception as e:
        logger.error(f"Token refresh failed with exception: {e}")
        return None

def get_access_token():
    """Get valid access token, refreshing if necessary"""
    try:
        if not os.path.exists(TOKEN_FILE):
            logger.error("No token file found - authentication required")
            return None
            
        tokens = load_tokens()
        
        # Check if refresh token is near expiry and warn user
        if is_refresh_token_near_expiry(tokens, days_before_expiry=1.0):
            logger.warning("🔴 URGENT: Refresh token expires within 1 day!")
            logger.warning("🔴 Manual re-authentication will be required soon!")
            logger.warning("🔴 Visit /authorize in your browser to re-authenticate")
        
        # Check if we need to refresh access token
        needs_access_refresh = is_token_expired(tokens)
        
        if needs_access_refresh:
            with _refresh_lock:
                # Double-check after acquiring lock
                tokens = load_tokens()
                needs_access_refresh = is_token_expired(tokens)
                
                if needs_access_refresh:
                    logger.info("⏰ Access token expired, refreshing...")
                    tokens = refresh_tokens()
                    
                    if not tokens:
                        logger.error("Failed to refresh tokens")
                        return None
        
        return tokens.get('access_token') if tokens else None
        
    except FileNotFoundError:
        logger.error("Token file not found - authentication required")
        return None
    except json.JSONDecodeError as e:
        logger.error(f"Token file is corrupted: {e}")
        mark_tokens_invalid()
        return None
    except Exception as e:
        logger.error(f"Failed to get access token: {e}")
        return None

def make_schwab_request(method, endpoint, **kwargs):
    """Make authenticated request to Schwab API with retry logic"""
    max_retries = 2
    
    for attempt in range(max_retries):
        access_token = get_access_token()
        if not access_token:
            logger.error("No access token available — aborting request.")
            return {"error": "Authentication failed", "status_code": 401}

        headers = kwargs.pop('headers', {})
        headers.update({
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json"
        })
        if method.upper() in ["POST", "PUT", "PATCH"]:
            headers["Content-Type"] = "application/json"

        url = f"https://api.schwabapi.com{endpoint}"
        logger.debug(f"{method} {url}")

        try:
            response = requests.request(method, url, headers=headers, timeout=30, **kwargs)

            # If we get 401, try refreshing token once
            if response.status_code == 401 and attempt < max_retries - 1:
                logger.warning(f"Got 401, attempting token refresh (attempt {attempt + 1})")
                with _refresh_lock:
                    refresh_result = refresh_tokens()
                    if not refresh_result:
                        logger.error("Token refresh failed, cannot retry request")
                        break
                continue

            if not response.ok:
                logger.error(f"{method} {url} failed: {response.status_code} - {response.text}")

            if response.status_code == 201 and not response.text.strip():
                return {"message": "Order placed successfully", "status_code": 201}

            if response.content and response.content.strip():
                return response.json()
            else:
                return {"status_code": response.status_code, "message": "No content"}

        except Exception as e:
            logger.error(f"Request failed: {e}")
            if attempt < max_retries - 1:
                continue
            return {"error": "Request failed", "status_code": 500}
    
    return {"error": "Max retries exceeded", "status_code": 500}

def get_token_status():
    """Get detailed token status information"""
    try:
        if not os.path.exists(TOKEN_FILE):
            return {"valid": False, "message": "Token file not found - authentication required"}
            
        tokens = load_tokens()
        issued_at = tokens.get("issued_at")
        expires_in = tokens.get("expires_in", 1800)
        original_auth_time = tokens.get("original_auth_time", issued_at)

        if not issued_at:
            return {"valid": False, "message": "Token missing 'issued_at'"}

        now = time.time()
        expires_at = issued_at + expires_in
        valid = now < expires_at
        
        # Calculate based on ORIGINAL authentication time (not last refresh)
        auth_age_days = (now - original_auth_time) / (24 * 60 * 60)
        refresh_expires_in_days = 7 - auth_age_days
        refresh_warning = refresh_expires_in_days <= 1.5  # Warn when 1.5 days left

        # Convert timestamps to Eastern
        issued_dt = datetime.fromtimestamp(issued_at, tz=UTC).astimezone(EDT)
        expires_dt = datetime.fromtimestamp(expires_at, tz=UTC).astimezone(EDT)
        original_auth_dt = datetime.fromtimestamp(original_auth_time, tz=UTC).astimezone(EDT)

        status = {
            "valid": valid,
            "message": "Token is valid" if valid else "Token expired",
            "issued_at": issued_dt.strftime("%m-%d %I:%M:%S %p"),
            "expires_at": expires_dt.strftime("%m-%d %I:%M:%S %p"),
            "original_auth_at": original_auth_dt.strftime("%m-%d %I:%M:%S %p"),
            "time_remaining": f"{int((expires_at - now) // 60)}m" if valid else "Expired",
            "auth_age_days": round(auth_age_days, 1),
            "refresh_expires_in_days": round(refresh_expires_in_days, 1),
            "refresh_warning": refresh_warning
        }
        
        if refresh_warning:
            status["message"] += f" (Re-auth needed in {refresh_expires_in_days:.1f} days)"
        else:
            status["message"] += f" (Re-auth needed in {refresh_expires_in_days:.1f} days)"

        return status

    except FileNotFoundError:
        return {"valid": False, "message": "Token file not found - authentication required"}
    except json.JSONDecodeError:
        return {"valid": False, "message": "Token file corrupted - re-authentication required"}
    except Exception as e:
        logger.error(f"Token status error: {e}")
        return {"valid": False, "message": "Token check failed"}

def proactive_token_maintenance():
    """
    Check token status and warn if re-authentication is needed soon.
    
    Since Schwab doesn't extend the 7-day window reliably, we need to 
    monitor the original authentication time and warn users.
    """
    try:
        if not os.path.exists(TOKEN_FILE):
            logger.info("No token file found, skipping maintenance")
            return
            
        tokens = load_tokens()
        
        # Check if we're approaching the 7-day limit
        if is_refresh_token_near_expiry(tokens, days_before_expiry=2.0):
            logger.warning("🔴 ATTENTION: Refresh token expires within 2 days!")
            logger.warning("🔴 Schwab's API does not extend the 7-day window reliably")
            logger.warning("🔴 Manual re-authentication will be required soon")
            logger.warning("🔴 Visit your app's /authorize URL to re-authenticate")
            
            # Also check if we need to refresh the access token
            if is_token_expired(tokens):
                logger.info("🔄 Refreshing access token...")
                refresh_tokens()
        else:
            # Normal token refresh if access token is expired
            if is_token_expired(tokens):
                logger.info("🔄 Refreshing access token...")
                refresh_tokens()
            else:
                logger.info("✅ Tokens are healthy - no maintenance needed")
                
    except Exception as e:
        logger.error(f"Proactive token maintenance failed: {e}")

def is_shortable(ticker):
    resp = make_schwab_request("GET", f"/trader/v1/marketdata/{ticker}/borrowable")
    return resp.get("isShortable", False) if isinstance(resp, dict) else False

def get_yahoo_price(symbol):
    try:
        ticker = yf.Ticker(symbol)
        price = ticker.info.get("regularMarketPrice")
        if price is not None:
            return round(price, 2)
        else:
            logger.warning(f"Price for {symbol} not found on Yahoo.")
    except Exception as e:
        logger.error(f"Error fetching Yahoo price for {symbol}: {e}")
    return None

def get_account_hash(account_number: str):
    accounts = make_schwab_request("GET", "/trader/v1/accounts/accountNumbers")
    if not isinstance(accounts, list):
        logger.error(f"Unexpected hash response: {accounts}")
        return None
    for acct in accounts:
        if acct.get("accountNumber") == account_number:
            return acct.get("hashValue")
    logger.error(f"Account number {account_number} not found.")
    return None

def get_open_positions():
    if not ACCOUNT_ID:
        logger.error("ACCOUNT_ID is not set.")
        return []

    endpoint = f"/trader/v1/accounts/{ACCOUNT_ID}?fields=positions"
    response = make_schwab_request("GET", endpoint)
    if not isinstance(response, dict):
        return []

    raw_positions = response.get("securitiesAccount", {}).get("positions", [])
    positions = []

    for pos in raw_positions:
        symbol = pos.get("instrument", {}).get("symbol")
        long_qty = pos.get("longQuantity", 0)
        short_qty = pos.get("shortQuantity", 0)
        qty = long_qty or short_qty
        if not symbol or qty <= 0:
            continue

        mv = pos.get("marketValue", 0)
        avg = pos.get("averagePrice", 0)
        cost_basis = qty * avg
        pnl_percent = ((mv - cost_basis) / cost_basis) * 100 if cost_basis > 0 else 0.0
        position_type = "Long" if long_qty > 0 else "Short"

        positions.append({
            "symbol": symbol,
            "quantity": qty,
            "position_type": position_type,
            "entry_price": round(avg, 2),
            "last_price": round(mv / qty, 2) if qty else 0.0,
            "pl": round(pnl_percent, 2)
        })

    return positions