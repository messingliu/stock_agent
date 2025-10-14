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
import tushare as ts

# Enable nested asyncio for Jupyter compatibility
nest_asyncio.apply()

# Load environment variables
load_dotenv()

# 导入配置
from config import config

# 设置Tushare token
ts.set_token(config.tushare_token)

# 从配置文件获取设置
YAHOO_CALLS_PER_SECOND = config.rate_limits['yahoo']
AKSHARE_CALLS_PER_SECOND = config.rate_limits['akshare']
MAX_RETRIES = config.retry_config['max_retries']
RETRY_DELAY = config.retry_config['base_delay']
BATCH_SIZE = config.batch_sizes['us']
BATCH_SIZE_CN = config.batch_sizes['cn']
START_DATE = config.date_range['start_date']

backfill = len(sys.argv) > 1 and sys.argv[1] == '--backfill'
force_download = len(sys.argv) > 1 and sys.argv[1] == '--download'
china_stock = len(sys.argv) > 1 and sys.argv[1] == '--china'

# Semaphores for rate limiting
yahoo_semaphore = asyncio.Semaphore(YAHOO_CALLS_PER_SECOND)
akshare_semaphore = asyncio.Semaphore(AKSHARE_CALLS_PER_SECOND)


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
    return create_engine(config.db_url)

def initialize_database():
    """Create tables if they don't exist"""
    engine = get_db_engine()
    
    with engine.begin() as conn:
        # Create stock info tables
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS us_stocks_info (
                symbol VARCHAR(20) PRIMARY KEY,
                name VARCHAR(100),
                exchange VARCHAR(20),
                market VARCHAR(10),
                update_time TIMESTAMP
            )
        """))
        print("Created us_stocks_info table")
        
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS cn_stocks_info (
                symbol VARCHAR(20) PRIMARY KEY,
                name VARCHAR(100),
                exchange VARCHAR(20),
                market VARCHAR(10),
                update_time TIMESTAMP
            )
        """))
        print("Created cn_stocks_info table")
        
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

def get_stored_symbols_count(market='CN'):
    """获取数据库中存储的股票数量"""
    engine = get_db_engine()
    table_name = f"{market.lower()}_stocks_info"
    with engine.connect() as conn:
        result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
        return result.scalar()

def update_stock_info(symbols, market='CN'):
    """更新股票信息到数据库"""
    engine = get_db_engine()
    table_name = f"{market.lower()}_stocks_info"
    
    with engine.begin() as conn:
        for symbol_info in symbols:
            conn.execute(
                text(f"""
                    INSERT INTO {table_name} (symbol, name, exchange, market, update_time)
                    VALUES (:symbol, :name, :exchange, :market, :update_time)
                    ON CONFLICT (symbol) DO UPDATE SET
                        name = EXCLUDED.name,
                        exchange = EXCLUDED.exchange,
                        market = EXCLUDED.market,
                        update_time = EXCLUDED.update_time
                """),
                {
                    'symbol': symbol_info['symbol'],
                    'name': symbol_info.get('name', symbol_info['symbol']),
                    'exchange': symbol_info['exchange'],
                    'market': market,
                    'update_time': datetime.now()
                }
            )

def get_symbols_from_db(market='CN'):
    """从数据库获取股票信息"""
    engine = get_db_engine()
    table_name = f"{market.lower()}_stocks_info"
    
    filename = os.path.join('stock_lists', f'successful_symbols_cn.txt')
    finished_symbols = []
    if not os.path.exists(filename):
        print(f"File not found: {filename}")
    else:        
        with open(filename, 'r') as f:
            for line in f:
                parts = line.strip().split('|')
                finished_symbols.append(parts[0])
    print("finished_symbols count: ", len(finished_symbols))
    with engine.connect() as conn:
        result = conn.execute(text(f"SELECT * FROM {table_name}"))
        symbols = []
        for row in result:
            if row.symbol not in finished_symbols:
                symbols.append({
                    'symbol': row.symbol,
                    'name': row.name,
                    'exchange': row.exchange,
                    'market': row.market
                })
        print("symbols count: ", len(symbols))
        return symbols


MAX_RETRY_COUNT = 3
FALLBACK_THRESHOLD = 0.5  # 如果获取到的数据少于数据库中的50%，则使用数据库数据

def get_all_us_symbols(use_db=True):
    """获取所有美股股票代码，带重试和回退机制"""
    retry_count = 0
    stored_count = get_stored_symbols_count('US') if use_db else 0
    
    while retry_count < MAX_RETRY_COUNT:
        try:
            symbols = []
            us_stocks = ak.get_us_stock_name()
            # 处理股票代码，移除前缀（例如：'AAPL.US' -> 'AAPL'）
            symbols.extend([{
                'symbol': symbol.split('.')[1].replace('_', '.') if '.' in symbol else symbol,
                'name': name,
                'exchange': 'US'
            } for symbol, name in zip(us_stocks['symbol'], us_stocks['name'])])
            print("us symbols count: ", len(symbols))
            # 检查数据质量
            if stored_count > 0 and len(symbols) < stored_count * FALLBACK_THRESHOLD:
                print(f"Warning: Only got {len(symbols)} symbols, which is less than {FALLBACK_THRESHOLD*100}% of stored {stored_count} symbols")
                if retry_count < MAX_RETRY_COUNT - 1:
                    retry_count += 1
                    print(f"Retrying... (attempt {retry_count + 1}/{MAX_RETRY_COUNT})")
                    continue
                else:
                    print("Using stored data from database")
                    symbols = get_symbols_from_db('US')
            else:
                # 更新数据库
                update_stock_info(symbols, 'US')
            
            return symbols
            
        except Exception as e:
            print(f"Error getting US symbols: {str(e)}")
            retry_count += 1
            if retry_count < MAX_RETRY_COUNT:
                print(f"Retrying... (attempt {retry_count + 1}/{MAX_RETRY_COUNT})")
            else:
                print("Using stored data from database")
                return get_symbols_from_db('US')
            delay = RETRY_DELAY * (2 ** retry_count)  # Exponential backoff
            print(f"Attempt {retry_count + 1} failed with error: {str(e)}. Retrying in {delay} seconds...")
            time.sleep(delay)

    return get_symbols_from_db('US')

def get_all_china_symbols(use_db=True):
    """获取所有A股股票代码，带重试和回退机制"""
    retry_count = 0
    stored_count = get_stored_symbols_count('CN') if use_db else 0
    
    while retry_count < MAX_RETRY_COUNT:
        try:
            symbols = []
            
            # 获取上海证券交易所股票
            sh_stocks = ak.stock_sh_a_spot_em()
            symbols.extend([{
                'symbol': row['代码'],
                'name': row['名称'],
                'exchange': 'SH'
            } for _, row in sh_stocks.iterrows()])
            
            # 获取深圳证券交易所股票
            sz_stocks = ak.stock_sz_a_spot_em()
            symbols.extend([{
                'symbol': row['代码'],
                'name': row['名称'],
                'exchange': 'SZ'
            } for _, row in sz_stocks.iterrows()])
            
            # 获取北京证券交易所股票
            bj_stocks = ak.stock_bj_a_spot_em()
            symbols.extend([{
                'symbol': row['代码'],
                'name': row['名称'],
                'exchange': 'BJ'
            } for _, row in bj_stocks.iterrows()])
            
            # 检查数据质量
            if stored_count > 0 and len(symbols) < stored_count * FALLBACK_THRESHOLD:
                print(f"Warning: Only got {len(symbols)} symbols, which is less than {FALLBACK_THRESHOLD*100}% of stored {stored_count} symbols")
                if retry_count < MAX_RETRY_COUNT - 1:
                    retry_count += 1
                    print(f"Retrying... (attempt {retry_count + 1}/{MAX_RETRY_COUNT})")
                    continue
                else:
                    print("Using stored data from database")
                    symbols = get_symbols_from_db('CN')
            else:
                # 更新数据库
                update_stock_info(symbols, 'CN')
            
            return symbols
            
        except Exception as e:
            print(f"Error getting China symbols: {str(e)}")
            retry_count += 1
            if retry_count < MAX_RETRY_COUNT:
                print(f"Retrying... (attempt {retry_count + 1}/{MAX_RETRY_COUNT})")
            else:
                print("Using stored data from database")
                return get_symbols_from_db('CN')
            delay = RETRY_DELAY * (2 ** retry_count)  # Exponential backoff
            print(f"Attempt {retry_count + 1} failed with error: {str(e)}. Retrying in {delay} seconds...")
            time.sleep(delay)
    
    return get_symbols_from_db('CN')

async def get_stocks_history(symbols, start_date):
    """Download historical data for multiple stocks in one request"""
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
        default_start = datetime.strptime(START_DATE, '%Y-%m-%d').date()
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
                        symbol_data = symbol_data[symbol_data['Close'].notna()].copy()  # Fix fragmentation warning
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
                start_date = datetime.strptime(START_DATE, '%Y-%m-%d').date()
                
                if start_date < datetime.now().date():
                    with ThreadPoolExecutor() as pool:
                        stock_code_suf = symbol
                        if symbol >= '800000':
                            stock_code_suf = str(symbol).zfill(6) + '.BJ'
                        elif symbol > '600000':
                            stock_code_suf = str(symbol).zfill(6) + '.SH'
                        else:
                            stock_code_suf = str(symbol).zfill(6) + '.SZ'

                        loop = asyncio.get_event_loop()
                        hist = await loop.run_in_executor(
                            pool,
                            lambda: ts.pro_api().daily(ts_code=stock_code_suf, 
                                    start_date=START_DATE, 
                                    end_date=datetime.now().strftime("%Y%m%d"))
                        )
                        
                        if not hist.empty:
                            # 转换列名以匹配英文格式
                            hist = hist.rename(columns={
                                'trade_date': 'Date',
                                'open': 'Open',
                                'high': 'High',
                                'low': 'Low',
                                'close': 'Close',
                                'vol': 'Volume'
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
                            
                            filename = os.path.join('stock_lists', f'successful_symbols_cn.txt')                            
                            # 确保文件存在
                            if not os.path.exists(filename):
                                open(filename, 'a').close()
                            with open(filename, 'a') as f:
                                f.write(f"{symbol}\n")

            await asyncio.sleep(1/AKSHARE_CALLS_PER_SECOND)  # Rate limiting
    except Exception as e:
        print(f"Error processing China stock {symbol}: {str(e)}")
        save_stock_to_file(symbol, 'CN', 'failed', str(e))
        await asyncio.sleep(1)  # Rate limiting
    
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
    elif force_download:
        symbols = get_all_us_symbols(True)
    else:
        symbols = get_symbols_from_db('US')

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
    elif force_download:
        symbols = get_all_china_symbols(True)
    else:
        symbols = get_symbols_from_db('CN')

    engine = get_db_engine()
    total_symbols = len(symbols)
    completed = 0
    
    with tqdm(total=total_symbols, desc="Downloading China stocks") as pbar:
        # Process symbols in batches
        for i in range(0, total_symbols, BATCH_SIZE_CN):
            batch = symbols[i:i + BATCH_SIZE_CN]
            tasks = []
            for symbol_info in batch:
                task = asyncio.create_task(process_china_stock(symbol_info, engine))
                tasks.append(task)
            
            # Wait for all tasks in this batch to complete
            await asyncio.gather(*tasks)
            completed += len(batch)
            pbar.update(len(batch))
            
            # Add a small delay between batches
            if i + BATCH_SIZE_CN < total_symbols:
                await asyncio.sleep(1)


async def main_async():
    # Initialize database
    initialize_database()
    print(f"Initialized database")
    
    if china_stock:
        china_task = asyncio.create_task(download_china_stocks_async())
        await asyncio.gather(china_task)
    else:
        us_task = asyncio.create_task(download_us_stocks_async())
        await asyncio.gather(us_task)

def main():
    # Run the async main function
    asyncio.run(main_async())

if __name__ == "__main__":
    main()
