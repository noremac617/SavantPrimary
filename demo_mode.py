import os
import json
import random
import time
import math
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from loguru import logger
import yfinance as yf
from typing import Dict, Optional, Tuple, List

EDT = ZoneInfo("America/New_York")

class DemoModeSimulator:
    """
    Sophisticated demo mode that simulates realistic trading conditions
    for testing neural net builds without live trading
    """
    
    def __init__(self):
        self.config_file = "./data/demo_config.json"
        self.load_config()
    
    def load_config(self):
        """Load demo mode configuration"""
        default_config = {
            "enabled": False,
            "simulation_params": {
                "base_spread_pct": 0.05,  # Base bid-ask spread %
                "max_slippage_pct": 0.15,  # Maximum slippage %
                "volatility_multiplier": 1.5,  # How much volatility affects spread
                "time_decay_factor": 0.3,  # How much spread tightens over time
                "volume_impact_factor": 0.2,  # How volume affects spread
                "fill_delay_min": 1.0,  # Minimum fill delay seconds
                "fill_delay_max": 3.0,  # Maximum fill delay seconds
                "realistic_rejection_rate": 0.02  # 2% chance of order rejection
            },
            "visual_indicators": {
                "show_demo_banner": True,
                "demo_color_scheme": True,
                "add_demo_prefix": True
            }
        }
        
        try:
            if os.path.exists(self.config_file):
                with open(self.config_file, 'r') as f:
                    self.config = json.load(f)
            else:
                self.config = default_config
                self.save_config()
        except Exception as e:
            logger.error(f"[DEMO] Error loading config: {e}")
            self.config = default_config
    
    def save_config(self):
        """Save demo mode configuration"""
        try:
            os.makedirs(os.path.dirname(self.config_file), exist_ok=True)
            with open(self.config_file, 'w') as f:
                json.dump(self.config, f, indent=2)
        except Exception as e:
            logger.error(f"[DEMO] Error saving config: {e}")
    
    def is_enabled(self) -> bool:
        """Check if demo mode is enabled"""
        return self.config.get("enabled", False)
    
    def toggle_demo_mode(self) -> bool:
        """Toggle demo mode on/off"""
        self.config["enabled"] = not self.config.get("enabled", False)
        self.save_config()
        status = "enabled" if self.config["enabled"] else "disabled"
        logger.info(f"[DEMO] Demo mode {status}")
        return self.config["enabled"]
    
    def set_demo_mode(self, enabled: bool):
        """Explicitly set demo mode state"""
        self.config["enabled"] = enabled
        self.save_config()
        status = "enabled" if enabled else "disabled"
        logger.info(f"[DEMO] Demo mode {status}")
    
    def get_market_conditions(self, symbol: str) -> Dict:
        """
        Analyze current market conditions for realistic simulation
        """
        try:
            ticker = yf.Ticker(symbol)
            
            # Get recent data for volatility calculation
            hist = ticker.history(period="5d", interval="1m")
            if hist.empty:
                # Fallback to daily data
                hist = ticker.history(period="10d")
            
            if hist.empty:
                logger.warning(f"[DEMO] No market data for {symbol}, using defaults")
                return self._get_default_conditions()
            
            # Calculate volatility (standard deviation of returns)
            returns = hist['Close'].pct_change().dropna()
            volatility = returns.std() * 100 if len(returns) > 1 else 1.0
            
            # Get volume characteristics
            avg_volume = hist['Volume'].mean() if 'Volume' in hist.columns else 1000000
            recent_volume = hist['Volume'].iloc[-1] if 'Volume' in hist.columns else avg_volume
            volume_ratio = recent_volume / avg_volume if avg_volume > 0 else 1.0
            
            # Calculate current spread estimate (simplified)
            current_price = hist['Close'].iloc[-1]
            price_range = hist['High'].iloc[-1] - hist['Low'].iloc[-1]
            estimated_spread_pct = (price_range / current_price) * 100 * 0.1  # Rough estimate
            
            # Time since market open (affects liquidity)
            now = datetime.now(EDT)
            market_open = now.replace(hour=9, minute=30, second=0, microsecond=0)
            if now.date() == market_open.date() and now >= market_open:
                minutes_since_open = (now - market_open).total_seconds() / 60
            else:
                minutes_since_open = 60  # Default to 1 hour if not during trading
            
            return {
                "symbol": symbol,
                "current_price": current_price,
                "volatility_pct": min(volatility, 10.0),  # Cap at 10%
                "volume_ratio": min(volume_ratio, 3.0),  # Cap at 3x
                "estimated_spread_pct": min(estimated_spread_pct, 0.5),  # Cap at 0.5%
                "minutes_since_open": minutes_since_open,
                "avg_volume": avg_volume
            }
            
        except Exception as e:
            logger.error(f"[DEMO] Error getting market conditions for {symbol}: {e}")
            return self._get_default_conditions(symbol)
    
    def _get_default_conditions(self, symbol: str = "DEFAULT") -> Dict:
        """Fallback market conditions"""
        return {
            "symbol": symbol,
            "current_price": 100.0,
            "volatility_pct": 2.0,
            "volume_ratio": 1.0,
            "estimated_spread_pct": 0.1,
            "minutes_since_open": 30,
            "avg_volume": 1000000
        }
    
    def simulate_realistic_entry(self, symbol: str, side: str, target_quantity: int) -> Dict:
        """
        Simulate a realistic order entry with sophisticated market modeling
        
        Returns:
        {
            "success": bool,
            "filled_quantity": int,
            "fill_price": float,
            "fill_time": datetime,
            "slippage_pct": float,
            "total_slippage_cost": float,
            "rejection_reason": str (if rejected)
        }
        """
        if not self.is_enabled():
            raise ValueError("Demo mode not enabled")
        
        logger.info(f"[DEMO] Simulating {side} {target_quantity}x {symbol}")
        
        # Get market conditions
        conditions = self.get_market_conditions(symbol)
        current_price = conditions["current_price"]
        
        # Calculate dynamic spread based on market conditions
        spread_pct = self._calculate_dynamic_spread(conditions)
        
        # Simulate order rejection (rare but realistic)
        if random.random() < self.config["simulation_params"]["realistic_rejection_rate"]:
            return {
                "success": False,
                "rejection_reason": random.choice([
                    "Insufficient liquidity",
                    "Halt pending",
                    "Outside market hours",
                    "Symbol not available"
                ])
            }
        
        # Calculate realistic fill price
        fill_price = self._calculate_fill_price(current_price, side, spread_pct, conditions)
        
        # Simulate realistic fill delay
        fill_delay = random.uniform(
            self.config["simulation_params"]["fill_delay_min"],
            self.config["simulation_params"]["fill_delay_max"]
        )
        
        # For demo, we don't actually wait, but we log the intended delay
        fill_time = datetime.now(timezone.utc)
        
        # Calculate slippage
        if side.upper() in ["BUY"]:
            slippage_pct = ((fill_price - current_price) / current_price) * 100
        else:  # SELL, SELL_SHORT
            slippage_pct = ((current_price - fill_price) / current_price) * 100
        
        total_slippage_cost = abs(slippage_pct * current_price * target_quantity / 100)
        
        logger.info(f"[DEMO] ✅ Simulated fill: {symbol} @ ${fill_price:.4f} "
                   f"(slippage: {slippage_pct:+.3f}%, delay: {fill_delay:.1f}s)")
        
        return {
            "success": True,
            "filled_quantity": target_quantity,
            "fill_price": round(fill_price, 4),
            "fill_time": fill_time,
            "slippage_pct": round(slippage_pct, 4),
            "total_slippage_cost": round(total_slippage_cost, 2),
            "simulated_delay": fill_delay,
            "market_conditions": conditions
        }
    
    def _calculate_dynamic_spread(self, conditions: Dict) -> float:
        """Calculate dynamic bid-ask spread based on market conditions"""
        params = self.config["simulation_params"]
        base_spread = params["base_spread_pct"]
        
        # Volatility impact (higher volatility = wider spreads)
        volatility_impact = conditions["volatility_pct"] * params["volatility_multiplier"] / 100
        
        # Volume impact (higher volume = tighter spreads)
        volume_impact = max(0, (1.5 - conditions["volume_ratio"]) * params["volume_impact_factor"])
        
        # Time decay (spreads tighten as market matures during the day)
        time_factor = max(0.3, 1 - (conditions["minutes_since_open"] / 390) * params["time_decay_factor"])
        
        # Combine factors
        dynamic_spread = (base_spread + volatility_impact + volume_impact) * time_factor
        
        # Cap the spread at reasonable levels
        return min(max(dynamic_spread, 0.01), params["max_slippage_pct"])
    
    def _calculate_fill_price(self, current_price: float, side: str, spread_pct: float, conditions: Dict) -> float:
        """Calculate realistic fill price including spread and slippage"""
        half_spread = spread_pct / 200  # Convert to decimal and divide by 2
        
        # Base bid/ask
        if side.upper() in ["BUY"]:
            # Buying at the ask (higher price)
            base_price = current_price * (1 + half_spread)
        else:  # SELL, SELL_SHORT
            # Selling at the bid (lower price)
            base_price = current_price * (1 - half_spread)
        
        # Add some randomness for realistic variation
        # Small additional slippage based on market impact
        market_impact = random.uniform(-0.02, 0.02) * (spread_pct / 100) * current_price
        
        return base_price + market_impact
    
    def simulate_bracket_orders(self, entry_result: Dict, side: str, stop_pct: float, target_pct: float) -> Dict:
        """
        Simulate the placement of stop-loss and take-profit bracket orders
        """
        if not entry_result["success"]:
            return {"bracket_orders": None, "error": "Entry order failed"}
        
        fill_price = entry_result["fill_price"]
        
        # Calculate bracket order prices
        if side.upper() in ["BUY"]:
            stop_price = fill_price * (1 - stop_pct)
            target_price = fill_price * (1 + target_pct)
        else:  # SELL_SHORT
            stop_price = fill_price * (1 + stop_pct)
            target_price = fill_price * (1 - target_pct)
        
        # Simulate small delay for bracket order placement
        bracket_delay = random.uniform(0.5, 1.5)
        
        logger.info(f"[DEMO] 🎯 Bracket orders placed: Stop=${stop_price:.4f}, Target=${target_price:.4f}")
        
        return {
            "bracket_orders": {
                "stop_loss": {
                    "price": round(stop_price, 4),
                    "order_type": "STOP"
                },
                "take_profit": {
                    "price": round(target_price, 4),
                    "order_type": "LIMIT"
                }
            },
            "bracket_delay": bracket_delay
        }
    
    def get_demo_status(self) -> Dict:
        """Get comprehensive demo mode status"""
        return {
            "enabled": self.is_enabled(),
            "config": self.config,
            "session_stats": {
                "demo_trades_today": self._count_demo_trades_today(),
                "last_demo_trade": self._get_last_demo_trade_time()
            }
        }
    
    def _count_demo_trades_today(self) -> int:
        """Count demo trades made today (placeholder - could read from logs)"""
        # This could be enhanced to actually count from logs or a tracking file
        return 0
    
    def _get_last_demo_trade_time(self) -> Optional[str]:
        """Get timestamp of last demo trade (placeholder)"""
        # This could be enhanced to track actual demo trade times
        return None
    
    def get_open_demo_trades(self) -> List[Dict]:
        """Get currently open demo trades from CSV"""
        import pandas as pd
        
        trades_path = "./data/trades.csv"
        if not os.path.exists(trades_path):
            return []
        
        try:
            df = pd.read_csv(trades_path)
            df['date'] = pd.to_datetime(df['date'], errors='coerce')
            
            # Get today's trades that don't have exit prices
            today = datetime.now(EDT).date()
            today_trades = df[df['date'].dt.date == today]
            
            # Filter for open positions (no exit price)
            open_trades = today_trades[
                (pd.isna(today_trades['exit'])) | 
                (today_trades['exit'] == '') | 
                (today_trades['exit'] == 0)
            ]
            
            demo_trades = []
            for _, trade in open_trades.iterrows():
                demo_trades.append({
                    'symbol': trade['symbol'],
                    'action': trade['action'],
                    'entry_price': float(trade['entry']),
                    'current_price': float(trade.get('current', trade['entry'])),
                    'date': trade['date'],
                    'quantity': 100,  # Estimated - we don't store quantity in CSV
                })
            
            return demo_trades
            
        except Exception as e:
            logger.error(f"[DEMO] Error loading open demo trades: {e}")
            return []
    
    def simulate_exit_monitoring(self) -> Dict:
        """
        Monitor demo trades and simulate exits based on real market data
        Returns summary of any exits that occurred
        """
        if not self.is_enabled():
            return {"exits": []}
        
        open_trades = self.get_open_demo_trades()
        if not open_trades:
            return {"exits": []}
        
        logger.debug(f"[DEMO] Monitoring {len(open_trades)} open demo positions")
        
        exits_simulated = []
        
        for trade in open_trades:
            try:
                symbol = trade['symbol']
                
                # Get current market price
                current_price = self._get_current_price(symbol)
                if not current_price:
                    continue
                
                # Calculate stop loss and take profit levels
                entry_price = trade['entry_price']
                action = trade['action']
                
                if action.upper() in ['BUY']:
                    stop_loss_price = entry_price * 0.99  # 1% stop loss
                    take_profit_price = entry_price * 1.03  # 3% take profit
                    
                    # Check for exit conditions
                    if current_price <= stop_loss_price:
                        exit_result = self._simulate_stop_loss_exit(trade, current_price, stop_loss_price)
                        exits_simulated.append(exit_result)
                    elif current_price >= take_profit_price:
                        exit_result = self._simulate_take_profit_exit(trade, current_price, take_profit_price)
                        exits_simulated.append(exit_result)
                
                elif action.upper() in ['SELL_SHORT']:
                    stop_loss_price = entry_price * 1.01  # 1% stop loss (price goes up)
                    take_profit_price = entry_price * 0.97  # 3% take profit (price goes down)
                    
                    # Check for exit conditions  
                    if current_price >= stop_loss_price:
                        exit_result = self._simulate_stop_loss_exit(trade, current_price, stop_loss_price)
                        exits_simulated.append(exit_result)
                    elif current_price <= take_profit_price:
                        exit_result = self._simulate_take_profit_exit(trade, current_price, take_profit_price)
                        exits_simulated.append(exit_result)
                        
            except Exception as e:
                logger.error(f"[DEMO] Error monitoring {trade.get('symbol', 'unknown')}: {e}")
                continue
        
        if exits_simulated:
            logger.info(f"[DEMO] 🎯 Simulated {len(exits_simulated)} exits during monitoring")
        
        return {"exits": exits_simulated}
    
    def _get_current_price(self, symbol: str) -> Optional[float]:
        """Get current market price for a symbol"""
        try:
            import yfinance as yf
            ticker = yf.Ticker(symbol)
            data = ticker.history(period="1d", interval="1m")
            
            if not data.empty:
                return float(data['Close'].iloc[-1])
            else:
                # Fallback to daily data
                data = ticker.history(period="1d")
                if not data.empty:
                    return float(data['Close'].iloc[-1])
                    
        except Exception as e:
            logger.error(f"[DEMO] Error getting price for {symbol}: {e}")
        
        return None
    
    def _simulate_stop_loss_exit(self, trade: Dict, current_price: float, stop_price: float) -> Dict:
        """Simulate a stop loss exit with realistic slippage"""
        symbol = trade['symbol']
        action = trade['action']
        entry_price = trade['entry_price']
        
        # Calculate realistic exit price with stop loss slippage
        # Stop losses typically get worse fills than limit orders
        slippage_factor = random.uniform(0.001, 0.003)  # 0.1% to 0.3% additional slippage
        
        if action.upper() in ['BUY']:
            # Selling at stop - might get slightly worse price
            exit_price = stop_price * (1 - slippage_factor)
            exit_instruction = "SELL"
            pnl_pct = ((exit_price - entry_price) / entry_price) * 100
        else:  # SELL_SHORT
            # Covering at stop - might get slightly worse price  
            exit_price = stop_price * (1 + slippage_factor)
            exit_instruction = "BUY_TO_COVER"
            pnl_pct = ((entry_price - exit_price) / entry_price) * 100
        
        # Update the CSV with exit data
        self._update_csv_exit(symbol, exit_price, pnl_pct)
        
        logger.info(f"[DEMO] 🛑 Stop Loss Hit: {symbol} {exit_instruction} @ ${exit_price:.4f} "
                   f"(P/L: {pnl_pct:+.2f}%, slippage: {slippage_factor*100:.2f}%)")
        
        return {
            "symbol": symbol,
            "exit_type": "stop_loss",
            "exit_price": exit_price,
            "pnl_pct": pnl_pct,
            "exit_time": datetime.now(timezone.utc),
            "slippage_pct": slippage_factor * 100
        }
    
    def _simulate_take_profit_exit(self, trade: Dict, current_price: float, target_price: float) -> Dict:
        """Simulate a take profit exit with realistic slippage"""
        symbol = trade['symbol']
        action = trade['action']
        entry_price = trade['entry_price']
        
        # Take profit orders get better fills but still some slippage
        slippage_factor = random.uniform(0.0005, 0.0015)  # 0.05% to 0.15% slippage
        
        if action.upper() in ['BUY']:
            # Selling at limit - might get slightly better price
            exit_price = target_price * (1 + slippage_factor)
            exit_instruction = "SELL"
            pnl_pct = ((exit_price - entry_price) / entry_price) * 100
        else:  # SELL_SHORT
            # Covering at limit - might get slightly better price
            exit_price = target_price * (1 - slippage_factor)
            exit_instruction = "BUY_TO_COVER" 
            pnl_pct = ((entry_price - exit_price) / entry_price) * 100
        
        # Update the CSV with exit data
        self._update_csv_exit(symbol, exit_price, pnl_pct)
        
        logger.info(f"[DEMO] 🎯 Take Profit Hit: {symbol} {exit_instruction} @ ${exit_price:.4f} "
                   f"(P/L: {pnl_pct:+.2f}%, bonus: {slippage_factor*100:.2f}%)")
        
        return {
            "symbol": symbol,
            "exit_type": "take_profit",
            "exit_price": exit_price,
            "pnl_pct": pnl_pct,
            "exit_time": datetime.now(timezone.utc),
            "slippage_pct": slippage_factor * 100
        }
    
    def simulate_market_close_exits(self) -> Dict:
        """Simulate closing all open demo positions at market close (3:55 PM)"""
        if not self.is_enabled():
            return {"exits": []}
        
        open_trades = self.get_open_demo_trades()
        if not open_trades:
            return {"exits": []}
        
        logger.info(f"[DEMO] 🔔 Market close - simulating exits for {len(open_trades)} open demo positions")
        
        exits_simulated = []
        
        for trade in open_trades:
            try:
                symbol = trade['symbol']
                action = trade['action']
                entry_price = trade['entry_price']
                
                # Get current market price for close
                current_price = self._get_current_price(symbol)
                if not current_price:
                    # Fallback to last known price
                    current_price = trade.get('current_price', entry_price)
                
                # Simulate market order exit with higher slippage (market close rush)
                slippage_factor = random.uniform(0.002, 0.005)  # 0.2% to 0.5% slippage
                
                if action.upper() in ['BUY']:
                    # Market sell at close
                    exit_price = current_price * (1 - slippage_factor)
                    exit_instruction = "SELL"
                    pnl_pct = ((exit_price - entry_price) / entry_price) * 100
                else:  # SELL_SHORT
                    # Market cover at close
                    exit_price = current_price * (1 + slippage_factor)
                    exit_instruction = "BUY_TO_COVER"
                    pnl_pct = ((entry_price - exit_price) / entry_price) * 100
                
                # Update the CSV with exit data
                self._update_csv_exit(symbol, exit_price, pnl_pct)
                
                logger.info(f"[DEMO] 🔔 Market Close: {symbol} {exit_instruction} @ ${exit_price:.4f} "
                           f"(P/L: {pnl_pct:+.2f}%)")
                
                exits_simulated.append({
                    "symbol": symbol,
                    "exit_type": "market_close",
                    "exit_price": exit_price,
                    "pnl_pct": pnl_pct,
                    "exit_time": datetime.now(timezone.utc),
                    "slippage_pct": slippage_factor * 100
                })
                
            except Exception as e:
                logger.error(f"[DEMO] Error closing {trade.get('symbol', 'unknown')} at market close: {e}")
                continue
        
        return {"exits": exits_simulated}
    
    def _update_csv_exit(self, symbol: str, exit_price: float, pnl_pct: float):
        """Update the trades CSV with exit price and P/L"""
        import pandas as pd
        
        trades_path = "./data/trades.csv"
        if not os.path.exists(trades_path):
            logger.error(f"[DEMO] CSV file not found: {trades_path}")
            return
        
        try:
            df = pd.read_csv(trades_path)
            df['date'] = pd.to_datetime(df['date'], errors='coerce')
            
            # Find today's open trade for this symbol
            today = datetime.now(EDT).date()
            mask = (
                (df['date'].dt.date == today) & 
                (df['symbol'] == symbol) & 
                ((pd.isna(df['exit'])) | (df['exit'] == '') | (df['exit'] == 0))
            )
            
            if mask.any():
                # Update the first matching trade
                df.loc[mask, 'exit'] = round(exit_price, 4)
                df.loc[mask, 'current'] = round(exit_price, 4)
                df.loc[mask, 'pnl_pct'] = round(pnl_pct, 2)
                
                # Save back to CSV
                df.to_csv(trades_path, index=False)
                logger.debug(f"[DEMO] Updated CSV: {symbol} exit @ ${exit_price:.4f}")
            else:
                logger.warning(f"[DEMO] No open trade found for {symbol} to update")
                
        except Exception as e:
            logger.error(f"[DEMO] Error updating CSV for {symbol}: {e}")
    
    def is_market_hours(self) -> bool:
        """Check if we're currently in market hours (9:30 AM - 4:00 PM ET)"""
        now = datetime.now(EDT)
        
        # Skip weekends
        if now.weekday() >= 5:  # Saturday = 5, Sunday = 6
            return False
        
        # Market hours: 9:30 AM - 4:00 PM ET
        market_open = now.replace(hour=9, minute=30, second=0, microsecond=0)
        market_close = now.replace(hour=16, minute=0, second=0, microsecond=0)
        
        return market_open <= now <= market_close