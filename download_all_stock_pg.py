"""股票数据下载模块"""

import os
import yfinance as yf
import akshare as ak
import pandas as pd
import time
import asyncio
import aiohttp
import nest_asyncio
from datetime import datetime, timedelta
from tqdm import tqdm
import requests
from concurrent.futures import ThreadPoolExecutor
import sys
import tushare as ts
from typing import Optional, Union, List, Dict, Any

# Enable nested asyncio for Jupyter compatibility
nest_asyncio.apply()

# 导入配置和DAO
from config import config
from dao import StockDAO, TaskDAO

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

# 命令行参数
backfill = len(sys.argv) > 1 and sys.argv[1] == '--backfill'
force_download = len(sys.argv) > 1 and sys.argv[1] == '--download'
china_stock = len(sys.argv) > 1 and sys.argv[1] == '--china'

# Semaphores for rate limiting
yahoo_semaphore = asyncio.Semaphore(YAHOO_CALLS_PER_SECOND)
akshare_semaphore = asyncio.Semaphore(AKSHARE_CALLS_PER_SECOND)

async def retry_with_backoff(func, *args, **kwargs):
    """使用指数退避的重试机制"""
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

async def get_stocks_history(symbols, start_date):
    """批量下载股票历史数据"""
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
                    progress=False
                )
            )
            return data
        except Exception as e:
            print(f"Error downloading data: {str(e)}")
            return {}

def calculate_moving_averages(df):
    """计算移动平均线"""
    df = df.sort_index()
    
    if 'Close' not in df.columns:
        print(f"Warning: 'Close' column not found in DataFrame. Columns: {df.columns}")
        return df
    
    close_series = df['Close'].copy()
    if close_series.isna().any():
        print(f"Warning: Found {close_series.isna().sum()} NaN values in Close prices")
    
    try:
        ma_windows = [5, 10, 20, 60, 200]
        available_periods = len(close_series)
        
        for window in ma_windows:
            df[f'ma{window}'] = None
        
        if available_periods > 0:
            for window in ma_windows:
                min_periods = min(max(5, available_periods), window)
                ma = close_series.rolling(window=window, min_periods=min_periods).mean().round(2)
                
                if available_periods < window:
                    print(f"Only {available_periods} periods available, using MA{available_periods} for MA{window}")
                    shorter_ma = close_series.rolling(window=available_periods, min_periods=min_periods).mean().round(2)
                    df[f'ma{window}'] = shorter_ma
                else:
                    df[f'ma{window}'] = ma
                
    except Exception as e:
        print(f"Error calculating moving averages: {str(e)}")
        for window in ma_windows:
            df[f'ma{window}'] = None
    
    return df

class DownloadStats:
    """下载统计"""
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

def save_stock_to_file(symbol: str, market: str = 'CN', status: str = 'failed', reason: str = ''):
    """保存股票状态到文件"""
    status_dir = 'stock_lists'
    os.makedirs(status_dir, exist_ok=True)
    
    filename = os.path.join(status_dir, f'{status}_{market.lower()}_stocks.txt')
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    if not os.path.exists(filename):
        open(filename, 'a').close()
    
    existing_symbols = set()
    with open(filename, 'r') as f:
        for line in f:
            parts = line.strip().split('|')
            if len(parts) >= 2:
                existing_symbols.add(parts[1])
    
    if symbol not in existing_symbols:
        with open(filename, 'a') as f:
            f.write(f"{timestamp}|{symbol}|{reason}\n")

def get_all_stock_symbols_from_file(market: str, status: str = 'failed') -> List[Dict[str, Any]]:
    """从文件读取股票代码"""
    filename = os.path.join('stock_lists', f'{status}_{market.lower()}_stocks.txt')
    symbols = []
    
    if not os.path.exists(filename):
        print(f"File not found: {filename}")
        return symbols
        
    with open(filename, 'r') as f:
        for line in f:
            parts = line.strip().split('|')
            if len(parts) >= 2:
                symbol = parts[1]
                if market.lower() == 'cn':
                    symbols.append({
                        'symbol': symbol,
                        'name': symbol,
                        'exchange': 'Unknown'
                    })
                else:  # US
                    symbols.append({
                        'symbol': symbol,
                        'exchange': 'Unknown'
                    })
    
    return symbols

async def process_us_stocks_batch(symbol_infos: List[Dict[str, Any]], stock_dao: StockDAO) -> int:
    """处理一批美股数据"""
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

            for symbol in symbols:
                try:
                    if symbol not in hist_data:
                        print(f"No data available for {symbol}")
                        continue
                    
                    symbol_data = hist_data[symbol]
                    if symbol_data.empty:
                        print(f"Empty data for {symbol}")
                        continue
                    
                    symbol_data = symbol_data[symbol_data['Close'].notna()].copy()
                    symbol_data = calculate_moving_averages(symbol_data)
                    symbol_data.reset_index(inplace=True)
                    
                    print(f"Downloaded {len(symbol_data)} records for {symbol}")
                    stock_dao.upsert_stock_prices(symbol, symbol_data)
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
        raise

async def process_china_stock(symbol_info: Dict[str, Any], stock_dao: StockDAO) -> int:
    """处理单个中国股票数据"""
    symbol = symbol_info['symbol']
    exchange = symbol_info['exchange']
    successful_symbols = set()
    
    try:
        async with akshare_semaphore:
            start_date = datetime.strptime(START_DATE, '%Y-%m-%d').date()
            
            if start_date < datetime.now().date():
                with ThreadPoolExecutor() as pool:
                    loop = asyncio.get_event_loop()
                    hist = await loop.run_in_executor(
                        pool,
                        lambda: ts.pro_api().daily(
                            ts_code=symbol+'.'+exchange, 
                            start_date=START_DATE, 
                            end_date=datetime.now().strftime("%Y%m%d")
                        )
                    )

                    if not hist.empty:
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
                        
                        hist = calculate_moving_averages(hist)
                        hist.reset_index(inplace=True)
                        
                        print(f"Downloaded {len(hist)} records for {symbol}")
                        stock_dao.upsert_stock_prices(symbol, hist)
                        successful_symbols.add(symbol)
                        
                        filename = os.path.join('stock_lists', f'successful_symbols_cn.txt')
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

async def download_stocks_async(market: str, task_id: Optional[int] = None):
    """下载股票数据"""
    stock_dao = StockDAO(market)
    task_dao = TaskDAO()
    is_china = market.lower() == 'cn'
    market_upper = market.upper()
    if not task_id:
        task_id = task_dao.create_task(market, 'download')
    # 获取股票列表
    if backfill:
        symbols = get_all_stock_symbols_from_file(market)
    elif force_download:
        symbols = get_all_china_symbols(True) if is_china else get_all_us_symbols(True)
    else:
        symbols = stock_dao.get_symbols_from_db(exclude_finished=is_china)

    total_symbols = len(symbols)
    stats = DownloadStats()
    stats.total = total_symbols
    
    # 更新任务状态
    if task_id:
        task_dao.update_task_status(
            task_id,
            'running',
            total_symbols=total_symbols,
            processed_symbols=0,
            failed_symbols=0
        )
    
    batch_size = BATCH_SIZE_CN if is_china else BATCH_SIZE
    desc = f"Downloading {market_upper} stocks"
    
    with tqdm(total=total_symbols, desc=desc) as pbar:
        for i in range(0, total_symbols, batch_size):
            batch = symbols[i:i + batch_size]
            try:
                if is_china:
                    # 处理中国股票
                    tasks = []
                    for symbol_info in batch:
                        task = asyncio.create_task(process_china_stock(symbol_info, stock_dao))
                        tasks.append(task)
                    success_count = sum(await asyncio.gather(*tasks))
                else:
                    # 处理美股
                    success_count = await process_us_stocks_batch(batch, stock_dao)
                
                stats.add_success(success_count)
                failed_count = len(batch) - success_count
                if failed_count > 0:
                    stats.add_failure(
                        [s['symbol'] for s in batch[-failed_count:]],
                        "Processing failed"
                    )
                
                # 更新任务状态
                if task_id:
                    task_dao.update_task_status(
                        task_id,
                        'running',
                        processed_symbols={'increment': success_count},
                        failed_symbols={'increment': failed_count}
                    )
                
                pbar.update(len(batch))
                
                # Add a small delay between batches
                if i + batch_size < total_symbols:
                    await asyncio.sleep(1)
                    
            except Exception as e:
                batch_symbols = [s['symbol'] for s in batch]
                stats.add_failure(batch_symbols, str(e))
                print(f"Batch failed: {str(e)}")
                
                # 更新任务状态
                if task_id:
                    task_dao.update_task_status(
                        task_id,
                        'running',
                        failed_symbols={'increment': len(batch)},
                        error_message=str(e)
                    )
        
        # Print final statistics
        stats.print_summary()
        
        # 更新最终状态
        if task_id:
            status = 'completed'
            if stats.failed:
                status = 'failed' if len(stats.failed) == total_symbols else 'partial'
            
            task_dao.update_task_status(
                task_id,
                status,
                end_time=datetime.now()
            )

def get_all_us_symbols(use_db: bool = True) -> List[Dict[str, Any]]:
    """获取所有美股代码"""
    retry_count = 0
    stock_dao = StockDAO('us')
    stored_count = stock_dao.get_stored_symbols_count() if use_db else 0
    
    while retry_count < MAX_RETRY_COUNT:
        try:
            symbols = []
            us_stocks = ak.get_us_stock_name()
            symbols.extend([{
                'symbol': symbol.split('.')[1].replace('_', '.') if '.' in symbol else symbol,
                'name': name,
                'exchange': 'US'
            } for symbol, name in zip(us_stocks['symbol'], us_stocks['name'])])
            
            print("us symbols count: ", len(symbols))
            
            if stored_count > 0 and len(symbols) < stored_count * FALLBACK_THRESHOLD:
                print(f"Warning: Only got {len(symbols)} symbols, which is less than {FALLBACK_THRESHOLD*100}% of stored {stored_count} symbols")
                if retry_count < MAX_RETRY_COUNT - 1:
                    retry_count += 1
                    print(f"Retrying... (attempt {retry_count + 1}/{MAX_RETRY_COUNT})")
                    continue
                else:
                    print("Using stored data from database")
                    return stock_dao.get_symbols_from_db()
            else:
                stock_dao.update_stock_info(symbols)
                return symbols
            
        except Exception as e:
            print(f"Error getting US symbols: {str(e)}")
            retry_count += 1
            if retry_count < MAX_RETRY_COUNT:
                print(f"Retrying... (attempt {retry_count + 1}/{MAX_RETRY_COUNT})")
            else:
                print("Using stored data from database")
                return stock_dao.get_symbols_from_db()
            delay = RETRY_DELAY * (2 ** retry_count)
            print(f"Attempt {retry_count + 1} failed with error: {str(e)}. Retrying in {delay} seconds...")
            time.sleep(delay)
    
    return stock_dao.get_symbols_from_db()

def get_all_china_symbols(use_db: bool = True) -> List[Dict[str, Any]]:
    """获取所有A股代码"""
    retry_count = 0
    stock_dao = StockDAO('cn')
    stored_count = stock_dao.get_stored_symbols_count() if use_db else 0
    
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
            
            if stored_count > 0 and len(symbols) < stored_count * FALLBACK_THRESHOLD:
                print(f"Warning: Only got {len(symbols)} symbols, which is less than {FALLBACK_THRESHOLD*100}% of stored {stored_count} symbols")
                if retry_count < MAX_RETRY_COUNT - 1:
                    retry_count += 1
                    print(f"Retrying... (attempt {retry_count + 1}/{MAX_RETRY_COUNT})")
                    continue
                else:
                    print("Using stored data from database")
                    return stock_dao.get_symbols_from_db()
            else:
                stock_dao.update_stock_info(symbols)
                return symbols
            
        except Exception as e:
            print(f"Error getting China symbols: {str(e)}")
            retry_count += 1
            if retry_count < MAX_RETRY_COUNT:
                print(f"Retrying... (attempt {retry_count + 1}/{MAX_RETRY_COUNT})")
            else:
                print("Using stored data from database")
                return stock_dao.get_symbols_from_db()
            delay = RETRY_DELAY * (2 ** retry_count)
            print(f"Attempt {retry_count + 1} failed with error: {str(e)}. Retrying in {delay} seconds...")
            time.sleep(delay)
    
    return stock_dao.get_symbols_from_db()

async def main_async(task_id: Optional[int] = None):
    """主异步函数"""
    # 初始化数据库
    stock_dao = StockDAO('us')
    stock_dao.initialize_tables()
    stock_dao = StockDAO('cn')
    stock_dao.initialize_tables()
    task_dao = TaskDAO()
    task_dao.initialize_table()
    print("Initialized database")
    
    # 根据参数选择市场
    market = 'cn' if china_stock else 'us'
    
    # 执行下载任务
    await download_stocks_async(market, task_id)

def main():
    """主函数"""
    asyncio.run(main_async())

if __name__ == "__main__":
    main()