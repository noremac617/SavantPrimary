#!/usr/bin/env python3
"""
Simple Optimizer Performance Test

Tests how long it takes to analyze one trade across all parameter combinations:
- Stop Loss: 13 values (0.5% to 4%)  
- Take Profit: 15 values (1% to 6%)
- Entry Minutes: 61 values (0 to 60 minutes)
Total: 13 × 15 × 61 = 11,895 combinations per trade
"""

import os
import sys
import time
import sqlite3
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from loguru import logger
import tempfile

# Add the current directory to Python path so we can import our modules
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Set up timezone
UTC = timezone.utc
EDT = ZoneInfo("America/New_York")

# Configure logging
logger.remove()
logger.add(sys.stdout, 
          level="INFO", 
          format="<green>{time:HH:mm:ss}</green> | <level>{level}</level> | {message}")

class SingleTradeOptimizerTest:
    """Test optimizer performance on a single trade with full parameter space"""
    
    def __init__(self):
        # Create a temporary database for testing
        self.temp_db = tempfile.NamedTemporaryFile(suffix='.db', delete=False)
        self.db_path = self.temp_db.name
        self.temp_db.close()
        
        self.symbol = "NVDA"
        self.test_date = "2025-09-18"  # Use a recent trading day
        
        # Parameter ranges from your optimizer
        self.stop_loss_range = [0.005, 0.0075, 0.01, 0.0125, 0.015, 0.0175, 0.02, 0.0225, 0.025, 0.0275, 0.03, 0.035, 0.04]
        self.take_profit_range = [0.01, 0.0125, 0.015, 0.0175, 0.02, 0.0225, 0.025, 0.0275, 0.03, 0.035, 0.04, 0.045, 0.05, 0.055, 0.06]
        self.entry_minute_range = list(range(0, 61))
        
        self.total_combinations = len(self.stop_loss_range) * len(self.take_profit_range) * len(self.entry_minute_range)
        
        logger.info(f"🎯 Testing {self.symbol} on {self.test_date}")
        logger.info(f"📊 Parameter combinations: {self.total_combinations:,}")
        logger.info(f"   Stop Loss: {len(self.stop_loss_range)} values")
        logger.info(f"   Take Profit: {len(self.take_profit_range)} values") 
        logger.info(f"   Entry Minutes: {len(self.entry_minute_range)} values")
    
    def setup_database(self):
        """Setup minimal database with just minute OHLCV data"""
        logger.info("🏗️ Setting up database...")
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # Create minute OHLCV table
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
                    UNIQUE(date, symbol, minute_timestamp)
                )
            """)
            
            # Index for fast lookups
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_ohlcv_date_symbol ON minute_ohlcv(date, symbol)")
            
            conn.commit()
        
        logger.success("✅ Database setup complete")
    
    def download_minute_data(self):
        """Download one day of minute data for NVDA"""
        logger.info(f"📥 Downloading minute data for {self.symbol} on {self.test_date}...")
        
        ticker = yf.Ticker(self.symbol)
        
        # Get minute data for the specific test date
        test_date_obj = datetime.strptime(self.test_date, "%Y-%m-%d")
        data = ticker.history(
            start=test_date_obj,
            end=test_date_obj + timedelta(days=1),
            interval="1m",
            prepost=False
        )
        
        if data.empty:
            logger.error(f"❌ No data available for {self.test_date}")
            return 0
        
        records_inserted = 0
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            for timestamp, row in data.iterrows():
                ts_str = timestamp.strftime("%H:%M:%S")
                
                try:
                    cursor.execute("""
                        INSERT OR IGNORE INTO minute_ohlcv 
                        (date, symbol, minute_timestamp, open, high, low, close, volume)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        self.test_date, self.symbol, ts_str,
                        round(float(row['Open']), 4),
                        round(float(row['High']), 4),
                        round(float(row['Low']), 4),
                        round(float(row['Close']), 4),
                        int(row['Volume'])
                    ))
                    records_inserted += 1
                except Exception as e:
                    logger.debug(f"Insert error: {e}")
            
            conn.commit()
        
        logger.success(f"✅ Downloaded {records_inserted} minute bars")
        return records_inserted
    
    def simulate_single_trade_optimization(self):
        """Simulate analyzing one trade across all parameter combinations"""
        logger.info("🚀 Starting single trade optimization simulation...")
        
        # Get the minute data to work with
        with sqlite3.connect(self.db_path) as conn:
            df = pd.read_sql_query("""
                SELECT minute_timestamp, open, high, low, close, volume
                FROM minute_ohlcv 
                WHERE symbol = ? AND date = ?
                ORDER BY minute_timestamp
            """, conn, params=(self.symbol, self.test_date))
        
        if df.empty:
            logger.error("❌ No minute data found")
            return None
        
        logger.info(f"📊 Working with {len(df)} minute bars")
        
        # Simulate the parameter optimization process
        start_time = time.time()
        
        best_pnl = -999
        best_params = None
        combinations_tested = 0
        
        # This simulates what your optimizer would do:
        # Test every combination of stop_loss, take_profit, and entry_minute
        for stop_loss in self.stop_loss_range:
            for take_profit in self.take_profit_range:
                for entry_minute in self.entry_minute_range:
                    
                    # Simulate trade analysis for this parameter combination
                    pnl = self.simulate_trade_outcome(df, stop_loss, take_profit, entry_minute)
                    
                    if pnl > best_pnl:
                        best_pnl = pnl
                        best_params = {
                            'stop_loss': stop_loss,
                            'take_profit': take_profit, 
                            'entry_minute': entry_minute,
                            'expected_pnl': pnl
                        }
                    
                    combinations_tested += 1
                    
                    # Progress update every 1000 combinations
                    if combinations_tested % 1000 == 0:
                        elapsed = time.time() - start_time
                        rate = combinations_tested / elapsed
                        eta = (self.total_combinations - combinations_tested) / rate
                        logger.info(f"⏱️ Tested {combinations_tested:,}/{self.total_combinations:,} "
                                  f"({rate:.0f}/sec, ETA: {eta:.1f}s)")
        
        total_time = time.time() - start_time
        
        logger.success(f"✅ Optimization complete!")
        logger.info(f"🏆 Best parameters:")
        logger.info(f"   Stop Loss: {best_params['stop_loss']:.2%}")
        logger.info(f"   Take Profit: {best_params['take_profit']:.2%}")
        logger.info(f"   Entry Minute: +{best_params['entry_minute']} min")
        logger.info(f"   Expected P&L: {best_params['expected_pnl']:.2%}")
        
        return {
            'total_combinations': combinations_tested,
            'total_time': total_time,
            'combinations_per_second': combinations_tested / total_time,
            'best_parameters': best_params
        }
    
    def simulate_trade_outcome(self, df, stop_loss_pct, take_profit_pct, entry_minute_offset):
        """
        Simulate a trade outcome for given parameters
        This mimics what your real optimizer would do
        """
        
        # Find entry point (market open + entry_minute_offset)
        if entry_minute_offset >= len(df):
            return -999  # Invalid entry time
        
        entry_bar = df.iloc[entry_minute_offset]
        entry_price = entry_bar['open']
        
        # Calculate stop and target prices
        stop_price = entry_price * (1 - stop_loss_pct)  # Assuming BUY
        target_price = entry_price * (1 + take_profit_pct)
        
        # Simulate trade progression through remaining minute bars
        for i in range(entry_minute_offset + 1, len(df)):
            bar = df.iloc[i]
            
            # Check if stop loss hit (low touches stop)
            if bar['low'] <= stop_price:
                return -stop_loss_pct
            
            # Check if take profit hit (high touches target)  
            if bar['high'] >= target_price:
                return take_profit_pct
        
        # If neither stop nor target hit, calculate P&L at market close
        final_price = df.iloc[-1]['close']
        return (final_price - entry_price) / entry_price
    
    def cleanup(self):
        """Clean up temporary files"""
        try:
            if os.path.exists(self.db_path):
                os.unlink(self.db_path)
                logger.info("🧹 Cleanup complete")
        except Exception as e:
            logger.warning(f"⚠️ Cleanup warning: {e}")
    
    def run_test(self):
        """Run the complete single trade optimization test"""
        logger.info("🎬 Starting Single Trade Optimizer Performance Test")
        logger.info("=" * 60)
        
        total_start_time = time.time()
        
        try:
            # Setup
            self.setup_database()
            
            # Download data
            records = self.download_minute_data()
            if records == 0:
                logger.error("❌ No data available, aborting test")
                return
            
            # Run optimization simulation
            results = self.simulate_single_trade_optimization()
            
            if results:
                total_test_time = time.time() - total_start_time
                
                # Print summary
                logger.info("=" * 60)
                logger.info("📋 SINGLE TRADE OPTIMIZATION PERFORMANCE")
                logger.info("=" * 60)
                logger.info(f"🎯 Symbol: {self.symbol}")
                logger.info(f"📅 Date: {self.test_date}")
                logger.info(f"📊 Minute Bars: {records}")
                logger.info(f"🔢 Parameter Combinations: {results['total_combinations']:,}")
                logger.info(f"⏱️ Optimization Time: {results['total_time']:.2f}s")
                logger.info(f"⚡ Processing Speed: {results['combinations_per_second']:,.0f} combinations/sec")
                logger.info(f"🏁 Total Test Time: {total_test_time:.2f}s")
                logger.info("")
                logger.info("🏆 BEST PARAMETERS:")
                best = results['best_parameters']
                logger.info(f"   Stop Loss: {best['stop_loss']:.2%}")
                logger.info(f"   Take Profit: {best['take_profit']:.2%}")
                logger.info(f"   Entry Delay: +{best['entry_minute']} minutes")
                logger.info(f"   Expected P&L: {best['expected_pnl']:.2%}")
                logger.info("=" * 60)
                
                # Performance assessment
                if results['total_time'] < 10:
                    logger.success("🚀 EXCELLENT: Optimization is fast enough for real-time use!")
                elif results['total_time'] < 30:
                    logger.info("👍 GOOD: Acceptable for pre-market optimization")
                else:
                    logger.warning("⚠️ SLOW: May need optimization for real-time use")
            
            return results
            
        except KeyboardInterrupt:
            logger.warning("⚠️ Test interrupted by user")
        except Exception as e:
            logger.error(f"❌ Test failed: {e}")
        finally:
            self.cleanup()


def main():
    """Main function to run the single trade optimizer test"""
    print("🧪 Single Trade Optimizer Performance Test")
    print("==========================================")
    print("This test will:")
    print("1. Download 1 day of minute data for NVDA")
    print("2. Test all 11,895 parameter combinations on one trade")
    print("3. Time how long the optimization takes")
    print("")
    print("Parameter ranges:")
    print("• Stop Loss: 0.5% to 4% (13 values)")
    print("• Take Profit: 1% to 6% (15 values)")  
    print("• Entry Minutes: 0 to 60 minutes (61 values)")
    print("• Total combinations: 13 × 15 × 61 = 11,895")
    print("")
    
    # Confirm execution
    response = input("Ready to start the test? (y/N): ").lower()
    if response != 'y':
        print("Test cancelled.")
        return
    
    # Run the test
    tester = SingleTradeOptimizerTest()
    results = tester.run_test()
    
    if results:
        print(f"\n✅ Test completed!")
        print(f"⚡ Processed {results['combinations_per_second']:,.0f} combinations/second")
        print(f"⏱️ Total optimization time: {results['total_time']:.2f} seconds")
    else:
        print("\n❌ Test failed or was interrupted")


if __name__ == "__main__":
    main()