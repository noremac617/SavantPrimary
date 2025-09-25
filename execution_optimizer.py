import sqlite3
import pandas as pd
import numpy as np
import json
import logging
import os
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
import pytz
from dataclasses import dataclass
import argparse

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Eastern Timezone
EDT = pytz.timezone('US/Eastern')

@dataclass
class OptimizationParameters:
    """Container for optimization parameters"""
    stop_loss_pct: float
    take_profit_pct: float
    entry_minute_offset: int
    expected_pnl: float
    confidence: float
    strategy_type: str  # 'default', 'universal', 'symbol_specific'

class ExecutionOptimizer:
    def __init__(self, db_path="./data/market_data.db"):
        self.db_path = db_path
        self.min_trades_for_symbol_specific = 10        # Symbol-specific threshold
        self.min_trades_for_universal_direction = 50    # Per-direction universal threshold
        
        # Default strategy parameters
        self.default_params = OptimizationParameters(
            stop_loss_pct=0.01,
            take_profit_pct=0.03,
            entry_minute_offset=0,  # Market open
            expected_pnl=0.0,
            confidence=0.0,
            strategy_type='default'
        )
        
        logger.debug(f"[OPTIMIZER] Initialized with thresholds: Symbol-specific={self.min_trades_for_symbol_specific}, Universal={self.min_trades_for_universal_direction}")
    
    def get_historical_picks(self) -> pd.DataFrame:
        """Get all historical picks (without P&L data) for status reporting"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                query = """
                    SELECT symbol, direction, date, pnl_percent as pnl_pct
                    FROM trade_outcomes
                    WHERE pnl_percent IS NOT NULL
                    ORDER BY date DESC
                """
                
                picks = pd.read_sql_query(query, conn)
                
            logger.debug(f"[OPTIMIZER] Loaded {len(picks)} historical picks")
            return picks
            
        except Exception as e:
            logger.error(f"[OPTIMIZER] Error loading historical picks: {e}")
            return pd.DataFrame()
    
    def get_actual_trade_outcome(self, symbol: str, date: str) -> Optional[float]:
        """Get actual trade outcome for a symbol on a specific date"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                query = """
                    SELECT pnl_percent FROM trade_outcomes 
                    WHERE symbol = ? AND date = ?
                """
                result = pd.read_sql_query(query, conn, params=[symbol, date])
                
                if result.empty:
                    return None
                
                return result['pnl_percent'].iloc[0] / 100.0  # Convert percentage to decimal
                
        except Exception as e:
            logger.error(f"[OPTIMIZER] Error getting trade outcome for {symbol} {date}: {e}")
            return None
    
    def get_all_historical_trades(self) -> pd.DataFrame:
        """Get ALL historical trades for universal optimization (no date limits)"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                query = """
                    SELECT symbol, direction, date, pnl_percent as pnl_pct
                    FROM trade_outcomes
                    WHERE pnl_percent IS NOT NULL
                    ORDER BY date DESC
                """
                
                trades = pd.read_sql_query(query, conn)
                
                # Convert pnl_percent to decimal
                if not trades.empty:
                    trades['pnl_pct'] = trades['pnl_pct'] / 100.0
                
            logger.debug(f"[OPTIMIZER] Loaded {len(trades)} total historical trades for optimization")
            return trades
            
        except Exception as e:
            logger.error(f"[OPTIMIZER] Error loading historical trades: {e}")
            return pd.DataFrame()
    
    def get_all_historical_trades_by_direction(self, direction: str) -> pd.DataFrame:
        """Get ALL historical trades for a specific direction (BUY or SELL_SHORT) - no date limits"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                query = """
                    SELECT symbol, direction, date, pnl_percent as pnl_pct
                    FROM trade_outcomes
                    WHERE direction = ?
                    AND pnl_percent IS NOT NULL
                    ORDER BY date DESC
                """
                
                trades = pd.read_sql_query(query, conn, params=[direction])
                
                # Convert pnl_percent to decimal
                if not trades.empty:
                    trades['pnl_pct'] = trades['pnl_pct'] / 100.0
                
            logger.info(f"[OPTIMIZER] Loaded {len(trades)} total {direction} trades for universal optimization")
            return trades
            
        except Exception as e:
            logger.error(f"[OPTIMIZER] Error loading {direction} trades: {e}")
            return pd.DataFrame()
    
    def get_symbol_specific_trades(self, symbol: str, direction: str) -> pd.DataFrame:
        """Get ALL trades for specific symbol/direction combination - no date limits"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                query = """
                    SELECT symbol, direction, date, pnl_percent as pnl_pct
                    FROM trade_outcomes
                    WHERE symbol = ? AND direction = ?
                    AND pnl_percent IS NOT NULL
                    ORDER BY date DESC
                """
                
                trades = pd.read_sql_query(query, conn, params=[symbol, direction])
                
                # Convert pnl_percent to decimal
                if not trades.empty:
                    trades['pnl_pct'] = trades['pnl_pct'] / 100.0
                
            logger.info(f"[OPTIMIZER] Loaded {len(trades)} total trades for {symbol} {direction}")
            return trades
            
        except Exception as e:
            logger.error(f"[OPTIMIZER] Error loading symbol-specific trades: {e}")
            return pd.DataFrame()
    
    def optimize_parameters_for_trades(self, trades: pd.DataFrame) -> OptimizationParameters:
        """
        Optimize entry timing, stop loss, and take profit for given trades
        Uses grid search across all historical data (no artificial restrictions)
        """
        if trades.empty:
            return self.default_params
        
        best_params = self.default_params
        best_avg_pnl = -999999
        
        # Reduced parameter ranges for faster execution
        stop_loss_range = [0.005, 0.0075, 0.01, 0.0125, 0.015, 0.0175, 0.02, 0.0225, 0.025, 0.0275, 0.03, 0.035, 0.04]
        take_profit_range = [0.01, 0.0125, 0.015, 0.0175, 0.02, 0.0225, 0.025, 0.0275, 0.03, 0.035, 0.04, 0.045, 0.05, 0.055, 0.06]
        entry_minute_range = list(range(0, 61))
        
        total_combinations = len(stop_loss_range) * len(take_profit_range) * len(entry_minute_range)
        logger.debug(f"[OPTIMIZER] Testing {total_combinations} parameter combinations on {len(trades)} trades")
        
        tested_combinations = 0
        
        for stop_pct in stop_loss_range:
            for tp_pct in take_profit_range:
                for entry_minute in entry_minute_range:
                    tested_combinations += 1
                    
                    # Skip invalid combinations
                    if tp_pct <= stop_pct:
                        continue
                    
                    # Calculate average PnL for this parameter set
                    total_pnl = 0
                    valid_trades = 0
                    
                    for _, trade in trades.iterrows():
                        simulated_pnl = self.simulate_trade_outcome(
                            trade['symbol'], trade['direction'], trade['date'],
                            stop_pct, tp_pct, entry_minute
                        )
                        
                        if simulated_pnl is not None:
                            total_pnl += simulated_pnl
                            valid_trades += 1
                    
                    if valid_trades > 0:
                        avg_pnl = total_pnl / valid_trades
                        
                        if avg_pnl > best_avg_pnl:
                            best_avg_pnl = avg_pnl
                            best_params = OptimizationParameters(
                                stop_loss_pct=stop_pct,
                                take_profit_pct=tp_pct,
                                entry_minute_offset=entry_minute,
                                expected_pnl=avg_pnl,
                                confidence=min(valid_trades / 10.0, 1.0),  # Confidence based on number of trades
                                strategy_type='optimized'
                            )
                    
                    # Progress logging every 1000 combinations
                    if tested_combinations % 1000 == 0:
                        progress = (tested_combinations / total_combinations) * 100
                        logger.debug(f"[OPTIMIZER] Progress: {progress:.1f}% ({tested_combinations}/{total_combinations})")
        
        logger.info(f"[OPTIMIZER] Best parameters: Stop={best_params.stop_loss_pct*100:.1f}%, Target={best_params.take_profit_pct*100:.1f}%, Entry=+{best_params.entry_minute_offset}min, Expected PnL={best_params.expected_pnl:.3f}")
        return best_params
    
    def get_symbol_specific_optimized_parameters(self, symbol: str, direction: str) -> OptimizationParameters:
        """Get optimized parameters for specific symbol/direction with all available data"""
        logger.info(f"[OPTIMIZER] Getting symbol-specific parameters for {symbol} {direction}")
        
        # Get all trades for this symbol/direction (no date limits)
        symbol_trades = self.get_symbol_specific_trades(symbol, direction)
        
        if len(symbol_trades) >= self.min_trades_for_symbol_specific:
            logger.info(f"[OPTIMIZER] ✅ Using SYMBOL-SPECIFIC strategy for {symbol} {direction} ({len(symbol_trades)} trades)")
            optimized_params = self.optimize_parameters_for_trades(symbol_trades)
            optimized_params.strategy_type = 'symbol_specific'
            return optimized_params
        else:
            logger.info(f"[OPTIMIZER] ⚠️ Insufficient symbol-specific data for {symbol} {direction} ({len(symbol_trades)} < {self.min_trades_for_symbol_specific})")
            return self._fallback_to_universal_direction(symbol, direction)
    
    def get_universal_optimized_parameters(self, direction: str) -> OptimizationParameters:
        """Get universally optimized parameters for direction (BUY/SELL_SHORT) using all available data"""
        logger.info(f"[OPTIMIZER] Getting universal parameters for {direction} direction")
        
        # Get all trades for this direction (no date limits)
        direction_trades = self.get_all_historical_trades_by_direction(direction)
        
        if len(direction_trades) >= self.min_trades_for_universal_direction:
            logger.info(f"[OPTIMIZER] ✅ Using UNIVERSAL {direction} strategy ({len(direction_trades)} trades)")
            optimized_params = self.optimize_parameters_for_trades(direction_trades)
            optimized_params.strategy_type = 'universal'
            return optimized_params
        else:
            logger.info(f"[OPTIMIZER] ⚠️ Insufficient {direction} data for universal strategy ({len(direction_trades)} < {self.min_trades_for_universal_direction})")
            return self.default_params
    
    def get_optimized_parameters(self, symbol: str, direction: str) -> OptimizationParameters:
        """
        Get best optimization strategy for given symbol/direction using all available data
        
        Strategy hierarchy (using ALL database data):
        1. Symbol-specific (10+ trades for symbol+direction)
        2. Universal direction (50+ trades for direction)  
        3. Default parameters
        """
        logger.info(f"[OPTIMIZER] 🎯 Getting optimized parameters for {symbol} {direction}")
        
        # STEP 1: Try symbol-specific optimization (ALL data)
        symbol_trades = self.get_symbol_specific_trades(symbol, direction)
        symbol_trade_count = len(symbol_trades)
        
        logger.info(f"[OPTIMIZER] 📊 Total {symbol} {direction} historical trades: {symbol_trade_count}")
        
        if symbol_trade_count >= self.min_trades_for_symbol_specific:
            logger.info(f"[OPTIMIZER] ✅ Using SYMBOL-SPECIFIC strategy ({symbol_trade_count} >= {self.min_trades_for_symbol_specific})")
            symbol_params = self.optimize_parameters_for_trades(symbol_trades)
            symbol_params.strategy_type = 'symbol_specific'
            return symbol_params
        
        # STEP 2: Try universal direction optimization (ALL data)
        direction_trades = self.get_all_historical_trades_by_direction(direction)
        direction_trade_count = len(direction_trades)
        
        logger.info(f"[OPTIMIZER] 📈 Total {direction} historical trades: {direction_trade_count}")
        
        if direction_trade_count >= self.min_trades_for_universal_direction:
            logger.info(f"[OPTIMIZER] ✅ Using UNIVERSAL {direction} strategy ({direction_trade_count} >= {self.min_trades_for_universal_direction})")
            universal_params = self.get_universal_optimized_parameters(direction)
            return universal_params
        
        # STEP 3: Default strategy
        logger.info(f"[OPTIMIZER] ⚡ Using DEFAULT strategy (insufficient {direction} data: {direction_trade_count} < {self.min_trades_for_universal_direction})")
        return self.default_params
    
    def _fallback_to_universal_direction(self, symbol: str, direction: str) -> OptimizationParameters:
        """Fallback when symbol-specific optimization fails"""
        logger.warning(f"[OPTIMIZER] Symbol-specific optimization failed for {symbol} {direction}, falling back to universal {direction}")
        
        direction_trades = self.get_all_historical_trades_by_direction(direction)
        if len(direction_trades) >= self.min_trades_for_universal_direction:
            return self.get_universal_optimized_parameters(direction)
        else:
            logger.warning(f"[OPTIMIZER] Insufficient {direction} trades for universal fallback, using default")
            return self.default_params
    
    def simulate_trade_outcome(self, symbol: str, direction: str, date: str, 
                             stop_pct: float, tp_pct: float, entry_minute: int) -> Optional[float]:
        """
        Simulate trade outcome with given parameters
        This is a simplified simulation - in reality you'd use actual price data
        """
        try:
            # Get actual trade outcome from database
            with sqlite3.connect(self.db_path) as conn:
                query = """
                    SELECT pnl_percent FROM trade_outcomes 
                    WHERE symbol = ? AND direction = ? AND date = ?
                """
                result = pd.read_sql_query(query, conn, params=[symbol, direction, date])
                
                if result.empty:
                    return None
                
                actual_pnl = result['pnl_percent'].iloc[0] / 100.0  # Convert percentage to decimal
                
                # Simple simulation logic:
                # If actual trade was positive and would have hit our target, cap at target
                # If actual trade was negative and would have hit our stop, cap at stop loss
                if actual_pnl > 0:
                    return min(actual_pnl, tp_pct)  # Cap gains at take profit
                else:
                    return max(actual_pnl, -stop_pct)  # Cap losses at stop loss
                    
        except Exception as e:
            logger.error(f"[OPTIMIZER] Error simulating trade for {symbol} {direction} {date}: {e}")
            return None
    
    def generate_strategy_comparison(self) -> Dict:
        """
        Compare performance of all three strategies across ALL historical data
        """
        logger.debug("[OPTIMIZER] Generating three-strategy performance comparison using ALL available data...")
        
        all_trades = self.get_all_historical_trades()
        
        if all_trades.empty:
            return {"error": "No historical trades found"}
        
        logger.info(f"[OPTIMIZER] Analyzing {len(all_trades)} total historical trades")
        
        # Get optimized parameters for both directions
        universal_buy_params = self.get_universal_optimized_parameters("BUY")
        universal_short_params = self.get_universal_optimized_parameters("SELL_SHORT")
        
        # Test strategies on historical trades
        default_results = []
        universal_results = []
        symbol_specific_results = []
        
        strategy_assignments = {'default': 0, 'universal': 0, 'symbol_specific': 0}
        
        for _, trade in all_trades.iterrows():
            symbol = trade['symbol']
            direction = trade['direction']
            date = trade['date']
            
            # Determine which strategy would be used
            symbol_trades_count = len(self.get_symbol_specific_trades(symbol, direction))
            direction_trades_count = len(self.get_all_historical_trades_by_direction(direction))
            
            if symbol_trades_count >= self.min_trades_for_symbol_specific:
                strategy_used = 'symbol_specific'
                strategy_assignments['symbol_specific'] += 1
            elif direction_trades_count >= self.min_trades_for_universal_direction:
                strategy_used = 'universal'
                strategy_assignments['universal'] += 1
            else:
                strategy_used = 'default'
                strategy_assignments['default'] += 1
            
            # Test default strategy
            default_result = self.simulate_trade_outcome(
                symbol, direction, date,
                self.default_params.stop_loss_pct,
                self.default_params.take_profit_pct,
                self.default_params.entry_minute_offset
            )
            if default_result is not None:
                default_results.append(default_result)
            
            # Test universal strategy
            universal_params = universal_buy_params if direction == "BUY" else universal_short_params
            universal_result = self.simulate_trade_outcome(
                symbol, direction, date,
                universal_params.stop_loss_pct,
                universal_params.take_profit_pct,
                universal_params.entry_minute_offset
            )
            if universal_result is not None:
                universal_results.append(universal_result)
            
            # Test symbol-specific strategy (if applicable)
            if symbol_trades_count >= self.min_trades_for_symbol_specific:
                symbol_params = self.get_symbol_specific_optimized_parameters(symbol, direction)
                symbol_result = self.simulate_trade_outcome(
                    symbol, direction, date,
                    symbol_params.stop_loss_pct,
                    symbol_params.take_profit_pct,
                    symbol_params.entry_minute_offset
                )
                if symbol_result is not None:
                    symbol_specific_results.append(symbol_result)
        
        return {
            "total_trades_analyzed": len(all_trades),
            "data_period": "All available data (no date limits)",
            "strategy_assignments": strategy_assignments,
            "default_strategy": {
                "avg_pnl": np.mean(default_results) if default_results else 0,
                "total_pnl": sum(default_results) if default_results else 0,
                "trade_count": len(default_results),
                "win_rate": len([r for r in default_results if r > 0]) / len(default_results) if default_results else 0
            },
            "universal_strategy": {
                "avg_pnl": np.mean(universal_results) if universal_results else 0,
                "total_pnl": sum(universal_results) if universal_results else 0,
                "trade_count": len(universal_results),
                "win_rate": len([r for r in universal_results if r > 0]) / len(universal_results) if universal_results else 0
            },
            "symbol_specific_strategy": {
                "avg_pnl": np.mean(symbol_specific_results) if symbol_specific_results else 0,
                "total_pnl": sum(symbol_specific_results) if symbol_specific_results else 0,
                "trade_count": len(symbol_specific_results),
                "win_rate": len([r for r in symbol_specific_results if r > 0]) / len(symbol_specific_results) if symbol_specific_results else 0
            }
        }
    
    def analyze_strategy_distribution(self) -> Dict:
        """
        Analyze how trades would be distributed across the three strategies using ALL data
        """
        logger.debug("[OPTIMIZER] Analyzing strategy distribution across ALL historical data...")
        
        try:
            all_trades = self.get_all_historical_trades()
            
            if all_trades.empty:
                return {"error": "No historical trades found"}
            
            results = []
            
            # Group by symbol/direction to get counts
            grouped = all_trades.groupby(['symbol', 'direction']).size().reset_index(name='trade_count')
            
            for _, row in grouped.iterrows():
                symbol = row['symbol']
                direction = row['direction']
                trade_count = row['trade_count']
                
                # Determine strategy assignment
                if trade_count >= self.min_trades_for_symbol_specific:
                    strategy = 'symbol_specific'
                else:
                    direction_count = len(self.get_all_historical_trades_by_direction(direction))
                    if direction_count >= self.min_trades_for_universal_direction:
                        strategy = 'universal'
                    else:
                        strategy = 'default'
                
                results.append({
                    'symbol': symbol,
                    'direction': direction,
                    'trade_count': trade_count,
                    'strategy': strategy
                })
            
            # Calculate distribution
            total_combinations = len(results)
            strategy_counts = {
                'symbol_specific': sum(1 for r in results if r['strategy'] == 'symbol_specific'),
                'universal': sum(1 for r in results if r['strategy'] == 'universal'),
                'default': sum(1 for r in results if r['strategy'] == 'default')
            }
            
            trade_counts = {
                'symbol_specific': sum(r['trade_count'] for r in results if r['strategy'] == 'symbol_specific'),
                'universal': sum(r['trade_count'] for r in results if r['strategy'] == 'universal'),
                'default': sum(r['trade_count'] for r in results if r['strategy'] == 'default')
            }
            
            return {
                'total_symbol_direction_combinations': total_combinations,
                'total_trades_analyzed': len(all_trades),
                'data_period': 'All available data (no date limits)',
                'strategy_distribution': {
                    'symbol_specific': {
                        'combinations': strategy_counts['symbol_specific'],
                        'percentage': (strategy_counts['symbol_specific'] / total_combinations) * 100 if total_combinations > 0 else 0,
                        'total_trades': trade_counts['symbol_specific']
                    },
                    'universal': {
                        'combinations': strategy_counts['universal'],
                        'percentage': (strategy_counts['universal'] / total_combinations) * 100 if total_combinations > 0 else 0,
                        'total_trades': trade_counts['universal']
                    },
                    'default': {
                        'combinations': strategy_counts['default'],
                        'percentage': (strategy_counts['default'] / total_combinations) * 100 if total_combinations > 0 else 0,
                        'total_trades': trade_counts['default']
                    }
                },
                'details': results,
                'trade_distribution_by_strategy': {
                    'symbol_specific_pct': (trade_counts['symbol_specific'] / len(all_trades)) * 100 if len(all_trades) > 0 else 0,
                    'universal_pct': (trade_counts['universal'] / len(all_trades)) * 100 if len(all_trades) > 0 else 0,
                    'default_pct': (trade_counts['default'] / len(all_trades)) * 100 if len(all_trades) > 0 else 0
                }
            }
            
        except Exception as e:
            logger.error(f"[OPTIMIZER] Error analyzing strategy distribution: {e}")
            return {}
    
    def run_out_of_sample_validation(self, symbol: str, direction: str) -> Dict:
        """
        Run out-of-sample validation for symbol-specific optimization using ALL available data
        """
        try:
            # Get all trades for this symbol/direction (no date limits)
            all_trades = self.get_symbol_specific_trades(symbol, direction)
            
            if len(all_trades) < 20:  # Need minimum trades for validation
                return {"error": f"Insufficient trades for validation ({len(all_trades)} < 20)"}
            
            logger.info(f"[OPTIMIZER] Running out-of-sample validation for {symbol} {direction} using {len(all_trades)} total trades")
            
            # Split into training (first 80%) and test (last 20%)
            split_point = int(len(all_trades) * 0.8)
            training_trades = all_trades.iloc[split_point:].copy()  # Older trades for training
            test_trades = all_trades.iloc[:split_point].copy()      # Newer trades for testing
            
            # Optimize on training data
            training_params = self.optimize_parameters_for_trades(training_trades)
            
            # Test on out-of-sample data
            current_results = []
            optimized_results = []
            
            for _, test_trade in test_trades.iterrows():
                # Current strategy results
                current_pnl = self.simulate_trade_outcome(
                    test_trade['symbol'], test_trade['direction'], test_trade['date'],
                    stop_pct=self.default_params.stop_loss_pct,
                    tp_pct=self.default_params.take_profit_pct,
                    entry_minute=self.default_params.entry_minute_offset
                )
                
                # Optimized strategy results
                optimized_pnl = self.simulate_trade_outcome(
                    test_trade['symbol'], test_trade['direction'], test_trade['date'],
                    stop_pct=training_params.stop_loss_pct,
                    tp_pct=training_params.take_profit_pct,
                    entry_minute=training_params.entry_minute_offset
                )
                
                if current_pnl is not None:
                    current_results.append(current_pnl)
                if optimized_pnl is not None:
                    optimized_results.append(optimized_pnl)
            
            # Calculate validation metrics
            current_avg = np.mean(current_results) if current_results else 0
            optimized_avg = np.mean(optimized_results) if optimized_results else 0
            improvement = ((optimized_avg - current_avg) / abs(current_avg)) * 100 if current_avg != 0 else 0
            
            return {
                "symbol": symbol,
                "direction": direction,
                "total_trades": len(all_trades),
                "training_trades": len(training_trades),
                "test_trades": len(test_trades),
                "training_params": {
                    "stop_loss_pct": training_params.stop_loss_pct,
                    "take_profit_pct": training_params.take_profit_pct,
                    "entry_minute_offset": training_params.entry_minute_offset
                },
                "validation_results": {
                    "current_avg_pnl": current_avg,
                    "optimized_avg_pnl": optimized_avg,
                    "improvement_pct": improvement,
                    "validation_passed": optimized_avg > current_avg
                }
            }
            
        except Exception as e:
            logger.error(f"[OPTIMIZER] Error in out-of-sample validation: {e}")
            return {"error": str(e)}
    
    def format_time_offset(self, minutes=0):
        """Convert minute offset to HH:MM:SS format"""
        market_open = datetime.strptime("09:30:00", "%H:%M:%S")
        new_time = market_open + timedelta(minutes=minutes)
        return new_time.strftime("%H:%M:%S")
    
    def save_optimization_results(self, results: Dict, filepath: str = "three_strategy_optimization_results.json"):
        """Save optimization results to file"""
        try:
            with open(filepath, 'w') as f:
                json.dump(results, f, indent=2, default=str)
            logger.info(f"[OPTIMIZER] Results saved to {filepath}")
        except Exception as e:
            logger.error(f"[OPTIMIZER] Failed to save results: {e}")

    def run_weekly_optimization(self) -> Dict:
        """
        Run weekly optimization analysis and save results to data folder
        Now uses ALL available data instead of 250-day lookback
        """
        logger.info("[OPTIMIZER] Starting weekly optimization analysis using ALL available database data")
        
        try:
            # Get all trades from database (no date limits)
            all_trades = self.get_all_historical_trades()
            
            if len(all_trades) < self.min_trades_for_universal_direction:
                logger.warning(f"[OPTIMIZER] Insufficient trades for optimization ({len(all_trades)} < {self.min_trades_for_universal_direction})")
                return {"error": "Insufficient historical data"}
            
            logger.info(f"[OPTIMIZER] Analyzing {len(all_trades)} total historical trades from entire database")
            
            # Get universal strategy parameters for both directions
            universal_buy_params = self.get_universal_optimized_parameters("BUY")
            universal_short_params = self.get_universal_optimized_parameters("SELL_SHORT")
            
            # Find all symbol/direction combinations with 10+ trades (using ALL data)
            symbol_specific_strategies = {}
            
            with sqlite3.connect(self.db_path) as conn:
                query = """
                    SELECT symbol, direction, COUNT(*) as trade_count
                    FROM trade_outcomes 
                    WHERE pnl_percent IS NOT NULL
                    GROUP BY symbol, direction
                    HAVING COUNT(*) >= ?
                    ORDER BY trade_count DESC
                """
                results = pd.read_sql_query(query, conn, params=[self.min_trades_for_symbol_specific])
            
            logger.info(f"[OPTIMIZER] Found {len(results)} symbol/direction combinations with {self.min_trades_for_symbol_specific}+ trades")
            
            # Optimize parameters for each qualifying symbol/direction
            for _, row in results.iterrows():
                symbol = row['symbol']
                direction = row['direction']
                trade_count = row['trade_count']
                
                logger.info(f"[OPTIMIZER] Optimizing {symbol} {direction} ({trade_count} trades)")
                
                symbol_trades = self.get_symbol_specific_trades(symbol, direction)
                if not symbol_trades.empty:
                    symbol_params = self.optimize_parameters_for_trades(symbol_trades)
                    symbol_params.strategy_type = 'symbol_specific'
                    
                    symbol_specific_strategies[f"{symbol}_{direction}"] = {
                        'symbol': symbol,
                        'direction': direction,
                        'trade_count': trade_count,
                        'stop_loss_pct': symbol_params.stop_loss_pct,
                        'take_profit_pct': symbol_params.take_profit_pct,
                        'entry_minute_offset': symbol_params.entry_minute_offset,
                        'expected_pnl': symbol_params.expected_pnl,
                        'confidence': symbol_params.confidence
                    }
            
            # Get database statistics for analysis period info
            with sqlite3.connect(self.db_path) as conn:
                date_query = """
                    SELECT MIN(date) as oldest_date, MAX(date) as newest_date, COUNT(DISTINCT date) as trading_days
                    FROM trade_outcomes
                    WHERE pnl_percent IS NOT NULL
                """
                date_result = pd.read_sql_query(date_query, conn)
                
                oldest_date = date_result['oldest_date'].iloc[0] if not date_result.empty else "N/A"
                newest_date = date_result['newest_date'].iloc[0] if not date_result.empty else "N/A"
                trading_days = date_result['trading_days'].iloc[0] if not date_result.empty else 0
            
            # Create optimization results
            optimization_results = {
                'last_updated': datetime.now(EDT).isoformat(),
                'analysis_period': {
                    'description': 'All available database data (no date limits)',
                    'oldest_date': oldest_date,
                    'newest_date': newest_date,
                    'total_trading_days': trading_days
                },
                'total_historical_trades': len(all_trades),
                'universal_strategies': {
                    'BUY': {
                        'stop_loss_pct': universal_buy_params.stop_loss_pct,
                        'take_profit_pct': universal_buy_params.take_profit_pct,
                        'entry_minute_offset': universal_buy_params.entry_minute_offset,
                        'expected_pnl': universal_buy_params.expected_pnl,
                        'confidence': universal_buy_params.confidence,
                        'description': 'Applied to BUY trades with <10 symbol trades when BUY trades >= 50'
                    },
                    'SELL_SHORT': {
                        'stop_loss_pct': universal_short_params.stop_loss_pct,
                        'take_profit_pct': universal_short_params.take_profit_pct,
                        'entry_minute_offset': universal_short_params.entry_minute_offset,
                        'expected_pnl': universal_short_params.expected_pnl,
                        'confidence': universal_short_params.confidence,
                        'description': 'Applied to SELL_SHORT trades with <10 symbol trades when SELL_SHORT trades >= 50'
                    }
                },
                'symbol_specific_strategies': symbol_specific_strategies,
                'strategy_counts': {
                    'symbol_specific': len(symbol_specific_strategies),
                    'universal_buy_eligible': len(self.get_all_historical_trades_by_direction("BUY")) >= self.min_trades_for_universal_direction,
                    'universal_short_eligible': len(self.get_all_historical_trades_by_direction("SELL_SHORT")) >= self.min_trades_for_universal_direction
                },
                'optimization_config': {
                    'min_trades_for_symbol_specific': self.min_trades_for_symbol_specific,
                    'min_trades_for_universal_direction': self.min_trades_for_universal_direction,
                    'data_retention_policy': 'Database automatically maintains optimal size via trading day retention'
                }
            }
            
            # Save results to data folder
            data_dir = os.path.dirname(self.db_path)
            results_path = os.path.join(data_dir, "weekly_optimization_results.json")
            self.save_optimization_results(optimization_results, results_path)
            
            logger.info(f"[OPTIMIZER] Weekly optimization completed using {len(all_trades)} trades spanning {trading_days} trading days")
            logger.info(f"[OPTIMIZER] Generated {len(symbol_specific_strategies)} symbol-specific strategies")
            
            return optimization_results
            
        except Exception as e:
            logger.error(f"[OPTIMIZER] Error in weekly optimization: {e}")
            return {"error": str(e)}

def main():
    parser = argparse.ArgumentParser(description="Simplified Three-Strategy Trade Execution Optimizer")
    parser.add_argument("--mode", choices=["analyze", "optimize", "plan", "validate", "distribution", "weekly"], 
                       default="analyze", help="Operation mode")
    parser.add_argument("--symbol", type=str, help="Symbol for analysis")
    parser.add_argument("--direction", choices=["BUY", "SELL_SHORT"], help="Trade direction")
    parser.add_argument("--date", type=str, help="Date for analysis (YYYY-MM-DD)")
    
    args = parser.parse_args()
    
    optimizer = ExecutionOptimizer()
    
    if args.mode == "analyze":
        if not args.symbol or not args.direction:
            print("Error: --symbol and --direction required for analyze mode")
            return
            
        logger.info(f"[OPTIMIZER] Analyzing optimization for {args.symbol} {args.direction}")
        params = optimizer.get_optimized_parameters(args.symbol, args.direction)
        
        print(f"\n=== OPTIMIZATION RESULTS ===")
        print(f"Symbol: {args.symbol}")
        print(f"Direction: {args.direction}")
        print(f"Strategy: {params.strategy_type.upper()}")
        print(f"Stop Loss: {params.stop_loss_pct*100:.1f}%")
        print(f"Take Profit: {params.take_profit_pct*100:.1f}%")
        print(f"Entry Time: {optimizer.format_time_offset(params.entry_minute_offset)}")
        print(f"Expected P&L: {params.expected_pnl:.3f}")
        print(f"Confidence: {params.confidence:.2f}")
        
    elif args.mode == "optimize":
        if not args.symbol or not args.direction:
            print("Error: --symbol and --direction required for optimize mode")
            return
            
        logger.info(f"[OPTIMIZER] Running detailed optimization for {args.symbol} {args.direction}")
        params = optimizer.get_symbol_specific_optimized_parameters(args.symbol, args.direction)
        
        print(f"\n=== SYMBOL-SPECIFIC OPTIMIZATION ===")
        print(f"Symbol: {args.symbol}")
        print(f"Direction: {args.direction}")
        print(f"Strategy: {params.strategy_type.upper()}")
        print(f"Stop Loss: {params.stop_loss_pct*100:.1f}%")
        print(f"Take Profit: {params.take_profit_pct*100:.1f}%")
        print(f"Entry Time: {optimizer.format_time_offset(params.entry_minute_offset)}")
        print(f"Expected P&L: {params.expected_pnl:.3f}")
        print(f"Confidence: {params.confidence:.2f}")
        
    elif args.mode == "plan":
        logger.info("[OPTIMIZER] Generating execution plan for today's picks")
        
        # This would integrate with trade_plan.json to generate optimized execution parameters
        print("Execution plan generation not yet implemented")
        
    elif args.mode == "validate":
        if not args.symbol or not args.direction:
            print("Error: --symbol and --direction required for validate mode")
            return
            
        logger.info(f"[OPTIMIZER] Running out-of-sample validation for {args.symbol} {args.direction}")
        validation = optimizer.run_out_of_sample_validation(args.symbol, args.direction)
        
        if "error" in validation:
            print(f"Validation Error: {validation['error']}")
        else:
            oos = validation['validation_results']
            print(f"\n=== OUT-OF-SAMPLE VALIDATION ===")
            print(f"Symbol: {args.symbol} {args.direction}")
            print(f"Total Trades: {validation['total_trades']}")
            print(f"Training Trades: {validation['training_trades']}")
            print(f"Test Trades: {validation['test_trades']}")
            print(f"Current Avg P/L: {oos['current_avg_pnl']:.2%}")
            print(f"Optimized Avg P/L: {oos['optimized_avg_pnl']:.2%}")
            print(f"Improvement: {oos['improvement_pct']:+.1f}%")
            print(f"Validation Passed: {oos['validation_passed']}")
    
    elif args.mode == "distribution":
        logger.info("[OPTIMIZER] Analyzing strategy distribution across all historical data")
        distribution = optimizer.analyze_strategy_distribution()
        
        if "error" in distribution:
            print(f"Error: {distribution['error']}")
        else:
            print(f"\n=== STRATEGY DISTRIBUTION ANALYSIS ===")
            print(f"Data Period: {distribution['data_period']}")
            print(f"Total Trades: {distribution['total_trades_analyzed']}")
            print(f"Symbol/Direction Combinations: {distribution['total_symbol_direction_combinations']}")
            
            dist = distribution['strategy_distribution']
            print(f"\nStrategy Assignment by Combinations:")
            print(f"  Symbol-Specific: {dist['symbol_specific']['combinations']} ({dist['symbol_specific']['percentage']:.1f}%)")
            print(f"  Universal: {dist['universal']['combinations']} ({dist['universal']['percentage']:.1f}%)")
            print(f"  Default: {dist['default']['combinations']} ({dist['default']['percentage']:.1f}%)")
            
            trade_dist = distribution['trade_distribution_by_strategy']
            print(f"\nStrategy Assignment by Trade Volume:")
            print(f"  Symbol-Specific: {trade_dist['symbol_specific_pct']:.1f}% of all trades")
            print(f"  Universal: {trade_dist['universal_pct']:.1f}% of all trades")
            print(f"  Default: {trade_dist['default_pct']:.1f}% of all trades")

    elif args.mode == "weekly":
        logger.info("[OPTIMIZER] Running weekly optimization using ALL available data...")
        results = run_weekly_optimization_job()

def run_weekly_optimization_job():
    """
    Weekly optimization job - run this on weekends
    Now uses ALL available data instead of 250-day limit
    """
    logger.info("[OPTIMIZER] Running weekly optimization job using ALL database data...")
    
    optimizer = ExecutionOptimizer()
    results = optimizer.run_weekly_optimization()
    
    if "error" not in results:
        logger.info("[OPTIMIZER] Weekly optimization completed successfully")
        
        # Print summary
        period_info = results['analysis_period']
        universal_buy = results['universal_strategies']['BUY']
        universal_short = results['universal_strategies']['SELL_SHORT']
        symbol_count = results['strategy_counts']['symbol_specific']
        total_trades = results['total_historical_trades']
        
        print(f"\n=== WEEKLY OPTIMIZATION SUMMARY ===")
        print(f"Analysis Period: {period_info['description']}")
        print(f"Date Range: {period_info['oldest_date']} to {period_info['newest_date']}")
        print(f"Trading Days: {period_info['total_trading_days']}")
        print(f"Total Trades Analyzed: {total_trades}")
        print(f"\nUniversal BUY Strategy:")
        print(f"  Stop={universal_buy['stop_loss_pct']*100:.1f}%, Target={universal_buy['take_profit_pct']*100:.1f}%, Entry=+{universal_buy['entry_minute_offset']}min")
        print(f"Universal SHORT Strategy:")
        print(f"  Stop={universal_short['stop_loss_pct']*100:.1f}%, Target={universal_short['take_profit_pct']*100:.1f}%, Entry=+{universal_short['entry_minute_offset']}min")
        print(f"Symbol-Specific Strategies: {symbol_count} qualified")
        print(f"Results saved to: ./data/weekly_optimization_results.json")
        
        if symbol_count > 0:
            print(f"\nTop Symbol-Specific Strategies:")
            sorted_strategies = sorted(
                results['symbol_specific_strategies'].items(),
                key=lambda x: x[1]['trade_count'],
                reverse=True
            )[:10]  # Show top 10
            
            for symbol_key, strategy in sorted_strategies:
                print(f"  {strategy['symbol']} {strategy['direction']}: "
                      f"Stop={strategy['stop_loss_pct']*100:.1f}%, "
                      f"Target={strategy['take_profit_pct']*100:.1f}%, "
                      f"Entry=+{strategy['entry_minute_offset']}min "
                      f"({strategy['trade_count']} trades)")
    else:
        logger.error(f"[OPTIMIZER] Weekly optimization failed: {results['error']}")
    
    return results

if __name__ == "__main__":
    main()