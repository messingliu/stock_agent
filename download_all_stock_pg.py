import os
import yfinance as yf
import akshare as ak
import pandas as pd
import time
import asyncio
import aiohttp
import nest_asyncio
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
from dotenv import load_dotenv
from tqdm import tqdm
import requests
from concurrent.futures import ThreadPoolExecutor
from functools import partial
import sys

# Enable nested asyncio for Jupyter compatibility
nest_asyncio.apply()

# Load environment variables
load_dotenv()

# Database configuration
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432'),
    'database': os.getenv('DB_NAME', 'stock_db'),
    'user': os.getenv('DB_USER', 'mengliu'),
    'password': os.getenv('DB_PASSWORD', 'password')
}

# Rate limiting settings
YAHOO_CALLS_PER_SECOND = 1
AKSHARE_CALLS_PER_SECOND = 2
MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds
BATCH_SIZE = 100  # number of stocks to process in parallel

# Semaphores for rate limiting
yahoo_semaphore = asyncio.Semaphore(YAHOO_CALLS_PER_SECOND)
akshare_semaphore = asyncio.Semaphore(AKSHARE_CALLS_PER_SECOND)
backfill = len(sys.argv) > 1 and sys.argv[1] == '--backfill'

async def retry_with_backoff(func, *args, **kwargs):
    """Retry a function with exponential backoff."""
    max_retries = kwargs.pop('max_retries', MAX_RETRIES)
    base_delay = kwargs.pop('base_delay', RETRY_DELAY)
    
    for attempt in range(max_retries):
        try:
            return await func(*args, **kwargs)
        except Exception as e:
            if attempt == max_retries - 1:  # Last attempt
                raise  # Re-raise the last exception
            
            delay = base_delay * (2 ** attempt)  # Exponential backoff
            print(f"Attempt {attempt + 1} failed with error: {str(e)}. Retrying in {delay} seconds...")
            await asyncio.sleep(delay)

def get_db_engine():
    """Create SQLAlchemy engine for PostgreSQL"""
    return create_engine(
        f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    )

def initialize_database():
    """Create tables if they don't exist"""
    engine = get_db_engine()
    
    with engine.begin() as conn:
        # Drop existing tables if they exist
        # conn.execute(text("DROP TABLE IF EXISTS us_stock_prices CASCADE"))
        # conn.execute(text("DROP TABLE IF EXISTS cn_stock_prices CASCADE"))
        # conn.execute(text("DROP TABLE IF EXISTS us_stocks CASCADE"))
        # conn.execute(text("DROP TABLE IF EXISTS cn_stocks CASCADE"))
        
        # Create price tables
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS us_stock_prices (
                symbol VARCHAR(20),
                date DATE,
                open NUMERIC(10,2),
                high NUMERIC(10,2),
                low NUMERIC(10,2),
                close NUMERIC(10,2),
                volume BIGINT,
                ma5 NUMERIC(10,2),
                ma10 NUMERIC(10,2),
                ma20 NUMERIC(10,2),
                ma60 NUMERIC(10,2),
                ma200 NUMERIC(10,2),
                PRIMARY KEY (symbol, date)
            )
        """))
        print("Created us_stock_prices table")
        
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS cn_stock_prices (
                symbol VARCHAR(20),
                date DATE,
                open NUMERIC(10,2),
                high NUMERIC(10,2),
                low NUMERIC(10,2),
                close NUMERIC(10,2),
                volume BIGINT,
                ma5 NUMERIC(10,2),
                ma10 NUMERIC(10,2),
                ma20 NUMERIC(10,2),
                ma60 NUMERIC(10,2),
                ma200 NUMERIC(10,2),
                PRIMARY KEY (symbol, date)
            )
        """))
        print("Created cn_stock_prices table")
        
        # Create indexes for better query performance
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_us_stock_prices_symbol ON us_stock_prices(symbol)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_us_stock_prices_date ON us_stock_prices(date)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_cn_stock_prices_symbol ON cn_stock_prices(symbol)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_cn_stock_prices_date ON cn_stock_prices(date)"))

# The rest of the code remains the same, just replace "INSERT OR REPLACE" with 
# "INSERT ... ON CONFLICT ... DO UPDATE" for PostgreSQL upsert syntax

# Remove get_yahoo_stock_info as it's no longer needed

# def clean_symbol(symbol):
#     """Clean symbol for Yahoo Finance API"""
#     # Handle special cases
#     if symbol == 'BF.B':
#         return 'BF-B'
#     if symbol == 'BRK.B':
#         return 'BRK-B'
#     return symbol

async def get_stocks_history(symbols, start_date):
    """Download historical data for multiple stocks in one request"""
    # Clean symbols
    # cleaned_symbols = [clean_symbol(symbol) for symbol in symbols]
    
    with ThreadPoolExecutor() as pool:
        loop = asyncio.get_event_loop()
        try:
            data = await loop.run_in_executor(
                pool,
                lambda: yf.download(
                    symbols,
                    start=start_date,
                    group_by='ticker',
                    auto_adjust=True,
                    threads=True,
                    progress=False  # Disable progress bar to avoid confusion
                )
            )
            return data
        except Exception as e:
            print(f"Error downloading data: {str(e)}")
            return {}

def calculate_moving_averages(df):
    """Calculate moving averages for a DataFrame using available data"""
    # Sort DataFrame by date
    df = df.sort_index()
    
    # Handle missing values
    if 'Close' not in df.columns:
        print(f"Warning: 'Close' column not found in DataFrame. Columns: {df.columns}")
        return df
    
    # Remove any NaN values before calculating MAs
    close_series = df['Close'].copy()
    if close_series.isna().any():
        print(f"Warning: Found {close_series.isna().sum()} NaN values in Close prices")
    
    try:
        # Calculate basic moving averages with minimum periods
        ma_windows = [5, 10, 20, 60, 200]
        available_periods = len(close_series)
        
        # Initialize all MA columns
        for window in ma_windows:
            df[f'ma{window}'] = None
        
        if available_periods > 0:
            # Calculate MA for each window
            for i, window in enumerate(ma_windows):
                # If we don't have enough data for the full window,
                # use the maximum available periods but at least 5 days
                min_periods = min(max(5, available_periods), window)
                ma = close_series.rolling(window=window, min_periods=min_periods).mean().round(2)
                
                # For shorter periods, use the shorter MA for longer windows
                if available_periods < window:
                    print(f"Only {available_periods} periods available, using MA{available_periods} for MA{window}")
                    # Calculate MA with available periods
                    shorter_ma = close_series.rolling(window=available_periods, min_periods=min_periods).mean().round(2)
                    df[f'ma{window}'] = shorter_ma
                else:
                    df[f'ma{window}'] = ma
                
    except Exception as e:
        print(f"Error calculating moving averages: {str(e)}")
        # Initialize MA columns to avoid KeyError
        for window in ma_windows:
            df[f'ma{window}'] = None
    
    return df

async def process_us_stocks_batch(symbol_infos, engine):
    """Process a batch of US stocks asynchronously"""
    try:
        symbols = [info['symbol'] for info in symbol_infos]
        default_start = datetime.strptime('2000-01-01', '%Y-%m-%d').date()
        start_date = default_start
        successful_symbols = set()

        if start_date < datetime.now().date():
            print(f"Downloading data for {len(symbols)} symbols: {symbols}")
            hist_data = await get_stocks_history(symbols, start_date)
            
            print(f"Successfully downloaded history data for {len(hist_data)} symbols")
            if symbols and symbols[0] in hist_data:
                print(f"Sample data for {symbols[0]}:")
                print(hist_data[symbols[0]].head())

            with engine.begin() as conn:
                for symbol in symbols:
                    try:
                        if symbol not in hist_data:
                            print(f"No data available for {symbol}")
                            continue
                        
                        symbol_data = hist_data[symbol]
                        if symbol_data.empty:
                            print(f"Empty data for {symbol}")
                            continue
                            
                        symbol_data = symbol_data.copy()  # Fix fragmentation warning
                        
                        # Calculate moving averages before resetting index
                        symbol_data = calculate_moving_averages(symbol_data)
                        
                        symbol_data.reset_index(inplace=True)
                        print(f"Downloaded {len(symbol_data)} records for {symbol}")
                        
                        # Insert price data with moving averages
                        for _, row in symbol_data.iterrows():
                            conn.execute(text("""
                                INSERT INTO us_stock_prices (
                                    symbol, date, open, high, low, close, volume,
                                    ma5, ma10, ma20, ma60, ma200
                                )
                                VALUES (
                                    :symbol, :date, :open, :high, :low, :close, :volume,
                                    :ma5, :ma10, :ma20, :ma60, :ma200
                                )
                                ON CONFLICT (symbol, date) DO UPDATE SET
                                    open = EXCLUDED.open,
                                    high = EXCLUDED.high,
                                    low = EXCLUDED.low,
                                    close = EXCLUDED.close,
                                    volume = EXCLUDED.volume,
                                    ma5 = EXCLUDED.ma5,
                                    ma10 = EXCLUDED.ma10,
                                    ma20 = EXCLUDED.ma20,
                                    ma60 = EXCLUDED.ma60,
                                    ma200 = EXCLUDED.ma200
                            """), {
                                'symbol': symbol,
                                'date': row['Date'].date(),
                                'open': round(float(row['Open']), 2) if pd.notna(row['Open']) else None,
                                'high': round(float(row['High']), 2) if pd.notna(row['High']) else None,
                                'low': round(float(row['Low']), 2) if pd.notna(row['Low']) else None,
                                'close': round(float(row['Close']), 2) if pd.notna(row['Close']) else None,
                                'volume': int(row['Volume']) if pd.notna(row['Volume']) else None,
                                'ma5': round(float(row['ma5']), 2) if pd.notna(row['ma5']) else None,
                                'ma10': round(float(row['ma10']), 2) if pd.notna(row['ma10']) else None,
                                'ma20': round(float(row['ma20']), 2) if pd.notna(row['ma20']) else None,
                                'ma60': round(float(row['ma60']), 2) if pd.notna(row['ma60']) else None,
                                'ma200': round(float(row['ma200']), 2) if pd.notna(row['ma200']) else None
                            })
                        successful_symbols.add(symbol)
                    except Exception as e:
                        print(f"Error processing symbol {symbol}: {str(e)}")
                        save_stock_to_file(symbol, 'US', 'failed', str(e))
                        continue
            
        return len(successful_symbols)
    except Exception as e:
        symbols_str = ', '.join(s['symbol'] for s in symbol_infos)
        print(f"Error processing US stocks batch {symbols_str}: {str(e)}")
        for s in symbol_infos:
            save_stock_to_file(s['symbol'], 'US', 'failed', str(e)) 
        raise  # Re-raise the exception for the retry mechanism

async def process_china_stock(symbol_info, engine):
    """Process a single China stock asynchronously"""
    symbol = symbol_info['symbol']
    name = symbol_info['name']
    exchange = symbol_info['exchange']
    successful_symbols = set()
    
    try:
        async with akshare_semaphore:
            with engine.begin() as conn:
                start_date = datetime.strptime('2000-01-01', '%Y-%m-%d').date()
                
                if start_date < datetime.now().date():
                    with ThreadPoolExecutor() as pool:
                        loop = asyncio.get_event_loop()
                        hist = await loop.run_in_executor(
                            pool,
                            lambda: ak.stock_zh_a_hist(
                                symbol=symbol,
                                period="daily",
                                start_date="20100101",
                                end_date=datetime.now().strftime("%Y%m%d"),
                                adjust="qfq"
                            )
                        )
                        
                        if not hist.empty:
                            # 转换列名以匹配英文格式
                            hist = hist.rename(columns={
                                '日期': 'Date',
                                '开盘': 'Open',
                                '最高': 'High',
                                '最低': 'Low',
                                '收盘': 'Close',
                                '成交量': 'Volume'
                            })
                            hist['Date'] = pd.to_datetime(hist['Date'])
                            hist.set_index('Date', inplace=True)
                            
                            # 计算移动平均线
                            hist = calculate_moving_averages(hist)
                            hist.reset_index(inplace=True)
                            
                            print(f"Downloaded {len(hist)} records for {symbol}")
                            
                            # 插入数据
                            for _, row in hist.iterrows():
                                conn.execute(text("""
                                    INSERT INTO cn_stock_prices (
                                        symbol, date, open, high, low, close, volume,
                                        ma5, ma10, ma20, ma60, ma200
                                    )
                                    VALUES (
                                        :symbol, :date, :open, :high, :low, :close, :volume,
                                        :ma5, :ma10, :ma20, :ma60, :ma200
                                    )
                                    ON CONFLICT (symbol, date) DO UPDATE SET
                                        open = EXCLUDED.open,
                                        high = EXCLUDED.high,
                                        low = EXCLUDED.low,
                                        close = EXCLUDED.close,
                                        volume = EXCLUDED.volume,
                                        ma5 = EXCLUDED.ma5,
                                        ma10 = EXCLUDED.ma10,
                                        ma20 = EXCLUDED.ma20,
                                        ma60 = EXCLUDED.ma60,
                                        ma200 = EXCLUDED.ma200
                                """), {
                                    'symbol': symbol,
                                    'date': row['Date'].date(),
                                    'open': round(float(row['Open']), 2),
                                    'high': round(float(row['High']), 2),
                                    'low': round(float(row['Low']), 2),
                                    'close': round(float(row['Close']), 2),
                                    'volume': int(row['Volume']),
                                    'ma5': round(float(row['ma5']), 2) if pd.notna(row['ma5']) else None,
                                    'ma10': round(float(row['ma10']), 2) if pd.notna(row['ma10']) else None,
                                    'ma20': round(float(row['ma20']), 2) if pd.notna(row['ma20']) else None,
                                    'ma60': round(float(row['ma60']), 2) if pd.notna(row['ma60']) else None,
                                    'ma200': round(float(row['ma200']), 2) if pd.notna(row['ma200']) else None
                                })
                            successful_symbols.add(symbol)
            await asyncio.sleep(1/AKSHARE_CALLS_PER_SECOND)  # Rate limiting
    except Exception as e:
        print(f"Error processing China stock {symbol}: {str(e)}")
        save_stock_to_file(symbol, 'CN', 'failed', str(e))
    
    return len(successful_symbols)

# The rest of the code (get_all_us_symbols, get_all_china_symbols, etc.) remains the same
class DownloadStats:
    def __init__(self):
        self.total = 0
        self.success = 0
        self.failed = set()
        self.failed_reasons = {}
    
    def add_success(self, count=1):
        self.success += count
    
    def add_failure(self, symbols, reason):
        if isinstance(symbols, str):
            symbols = [symbols]
        for symbol in symbols:
            self.failed.add(symbol)
            if reason not in self.failed_reasons:
                self.failed_reasons[reason] = set()
            self.failed_reasons[reason].add(symbol)
    
    def print_summary(self):
        print("\nDownload Summary:")
        print(f"Total symbols: {self.total}")
        print(f"Successfully downloaded: {self.success}")
        print(f"Failed downloads: {len(self.failed)}")
        if self.total > 0:
            print(f"Success rate: {(self.success / self.total * 100):.2f}%")
        else:
            print("No symbols downloaded")
        
        if self.failed:
            print("\nFailed symbols by reason:")
            for reason, symbols in self.failed_reasons.items():
                print(f"\n{reason}:")
                print(f"Count: {len(symbols)}")
                print(f"Symbols: {sorted(list(symbols))}")

def save_stock_to_file(symbol, market='CN', status='failed', reason=''):
    """Save stock to file with status (failed/pending)"""
    status_dir = 'stock_lists'
    os.makedirs(status_dir, exist_ok=True)
    
    filename = os.path.join(status_dir, f'{status}_{market.lower()}_stocks.txt')
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    # 确保文件存在
    if not os.path.exists(filename):
        open(filename, 'a').close()
    
    # 读取现有内容，避免重复
    existing_symbols = set()
    with open(filename, 'r') as f:
        for line in f:
            parts = line.strip().split('|')
            if len(parts) >= 2:
                existing_symbols.add(parts[1])
    
    # 如果股票代码不存在，则添加
    if symbol not in existing_symbols:
        with open(filename, 'a') as f:
            f.write(f"{timestamp}|{symbol}|{reason}\n")

def get_all_stock_symbols_from_file(market, status='failed'):
    """从文件读取股票代码"""
    filename = os.path.join('stock_lists', f'{status}_{market.lower()}_stocks.txt')
    symbols = []
    
    if not os.path.exists(filename):
        print(f"File not found: {filename}")
        return symbols
        
    with open(filename, 'r') as f:
        for line in f:
            print(line)
            parts = line.strip().split('|')
            print(parts)
            if len(parts) >= 2:
                symbol = parts[1]
                print(symbol)
                if market == 'cn':
                    symbols.append({
                        'symbol': symbol,
                        'name': symbol,  # 使用代码作为名称
                        'exchange': 'Unknown'
                    })
                else:  # US
                    symbols.append({
                        'symbol': symbol,
                        'exchange': 'Unknown'
                    })
    
    return symbols

async def download_us_stocks_async():
    if backfill:
        symbols = get_all_stock_symbols_from_file('us')
    else:
        symbols = get_all_us_symbols()
    engine = get_db_engine()
    total_symbols = len(symbols)
    stats = DownloadStats()
    stats.total = total_symbols
    
    with tqdm(total=total_symbols, desc="Downloading US stocks") as pbar:
        # First pass: Process all symbols in batches
        for i in range(0, total_symbols, BATCH_SIZE):
            batch = symbols[i:i + BATCH_SIZE]
            try:
                success_count = await process_us_stocks_batch(batch, engine)
                stats.add_success(success_count)
                pbar.update(len(batch))
                
                # Add a small delay between batches
                if i + BATCH_SIZE < total_symbols:
                    await asyncio.sleep(1)
            except Exception as e:
                batch_symbols = [s['symbol'] for s in batch]
                stats.add_failure(batch_symbols, str(e))
                print(f"Batch failed: {str(e)}")
        
        # Second pass: Retry failed symbols with smaller batch size
        if stats.failed:
            print("\nRetrying failed symbols with smaller batch size...")
            retry_batch_size = 10
            failed_symbols = list(stats.failed)
            failed_symbol_infos = [s for s in symbols if s['symbol'] in failed_symbols]
            
            with tqdm(total=len(failed_symbols), desc="Retrying failed symbols") as retry_pbar:
                for i in range(0, len(failed_symbol_infos), retry_batch_size):
                    retry_batch = failed_symbol_infos[i:i + retry_batch_size]
                    try:
                        success_count = await process_us_stocks_batch(retry_batch, engine)
                        stats.add_success(success_count)
                        # Remove successful symbols from failed set
                        for symbol_info in retry_batch[:success_count]:
                            stats.failed.remove(symbol_info['symbol'])
                        retry_pbar.update(len(retry_batch))
                        
                        await asyncio.sleep(2)  # Longer delay for retries
                    except Exception as e:
                        retry_symbols = [s['symbol'] for s in retry_batch]
                        stats.add_failure(retry_symbols, f"Retry failed: {str(e)}")
        
        # Print final statistics
        stats.print_summary()

async def download_china_stocks_async():
    """Download China stock data asynchronously in batches"""
    if backfill:
        symbols = get_all_stock_symbols_from_file('cn')
    else:
        symbols = get_all_china_symbols()
    engine = get_db_engine()
    total_symbols = len(symbols)
    completed = 0
    
    with tqdm(total=total_symbols, desc="Downloading China stocks") as pbar:
        # Process symbols in batches
        for i in range(0, total_symbols, BATCH_SIZE):
            batch = symbols[i:i + BATCH_SIZE]
            tasks = []
            for symbol_info in batch:
                task = asyncio.create_task(process_china_stock(symbol_info, engine))
                tasks.append(task)
            
            # Wait for all tasks in this batch to complete
            await asyncio.gather(*tasks)
            completed += len(batch)
            pbar.update(len(batch))
            
            # Add a small delay between batches
            if i + BATCH_SIZE < total_symbols:
                await asyncio.sleep(1)

def get_all_us_symbols():
    """Get symbols from multiple US exchanges"""
    symbols = []
    
    try:
        sp500_url = "https://raw.githubusercontent.com/datasets/s-and-p-500-companies/master/data/constituents.csv"
        sp500_stocks = pd.read_csv(sp500_url)
        symbols.extend([{'symbol': symbol, 'exchange': 'SPY'} for symbol in sp500_stocks['Symbol']])
    except Exception as e:
        print(f"Error getting S&P 500 symbols: {str(e)}")
    
    try:
        nasdaq_stocks = ak.stock_us_spot_em()
        nasdaq_symbols = nasdaq_stocks[nasdaq_stocks['市场'].str.contains('NASDAQ', na=False)]
        symbols.extend([{'symbol': symbol, 'exchange': 'NASDAQ'} 
                       for symbol in nasdaq_symbols['代码'].tolist()])
        
        nyse_symbols = nasdaq_stocks[nasdaq_stocks['市场'].str.contains('NYSE', na=False)]
        symbols.extend([{'symbol': symbol, 'exchange': 'NYSE'} 
                       for symbol in nyse_symbols['代码'].tolist()])
    except Exception as e:
        print(f"Error getting NASDAQ/NYSE symbols: {str(e)}")
    
    return symbols

def get_all_china_symbols():
    """Get symbols from all Chinese exchanges"""
    symbols = []
    
    try:
        sh_stocks = ak.stock_info_sh_name_code()
        symbols.extend([{'symbol': row['code'], 'name': row['name'], 'exchange': 'SH'} 
                       for _, row in sh_stocks.iterrows()])
    except Exception as e:
        print(f"Error getting Shanghai symbols: {str(e)}")
    
    try:
        sz_stocks = ak.stock_info_sz_name_code()
        symbols.extend([{'symbol': row['code'], 'name': row['name'], 'exchange': 'SZ'} 
                       for _, row in sz_stocks.iterrows()])
    except Exception as e:
        print(f"Error getting Shenzhen symbols: {str(e)}")
    
    try:
        bj_stocks = ak.stock_bj_a_spot_em()
        symbols.extend([{'symbol': row['代码'], 'name': row['名称'], 'exchange': 'BJ'} 
                       for _, row in bj_stocks.iterrows()])
    except Exception as e:
        print(f"Error getting Beijing symbols: {str(e)}")
    
    return symbols

async def main_async():
    # Initialize database
    initialize_database()
    print(f"Initialized database")
    
    # Create tasks for US and China stocks
    us_task = asyncio.create_task(download_us_stocks_async())
    china_task = asyncio.create_task(download_china_stocks_async())
    
    # Wait for both tasks to complete
    await asyncio.gather(us_task, china_task)
    #await asyncio.gather(china_task)

def main():
    # Run the async main function
    asyncio.run(main_async())

if __name__ == "__main__":
    main()
