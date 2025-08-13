import tushare as ts
import akshare as ak
import pandas as pd
import os
import sys
from datetime import datetime, timedelta
import time
import random
import json

# 配置参数
DATA_DIR = "./stock_data"
CACHE_FILE = os.path.join(DATA_DIR, "cache_info.json")
HISTORY_DAYS = 60
TARGET_DAYS = int(sys.argv[1])
HIGH_OR_LOW = 'high' if sys.argv[2]=='high' else 'low'  # Tushare使用英文列名
TARGET = float(sys.argv[3])
USE_CACHE = True if sys.argv[4]=='cache' else False

# Tushare API配置
TUSHARE_TOKEN = "26977fd4744f7f489cd2c9803ce98c8f55dd1dba0dd0c70466877bcc"  # 请替换为你的Tushare token
ts.set_token(TUSHARE_TOKEN)
pro = ts.pro_api()

def init_data_dir():
    """初始化数据存储目录"""
    os.makedirs(DATA_DIR, exist_ok=True)

def get_cache_info():
    """获取缓存信息"""
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, 'r') as f:
                cache_info = json.load(f)
                return datetime.strptime(cache_info['date'], '%Y-%m-%d').date()
        except:
            return None
    return None

def update_cache_info():
    """更新缓存信息"""
    cache_info = {
        'date': datetime.now().strftime('%Y-%m-%d')
    }
    with open(CACHE_FILE, 'w') as f:
        json.dump(cache_info, f)

def is_cache_valid():
    """检查缓存是否在本周内"""
    cache_date = get_cache_info()
    if not cache_date:
        return False
    
    today = datetime.now().date()
    cache_week = cache_date.isocalendar()[1]
    today_week = today.isocalendar()[1]
    cache_year = cache_date.isocalendar()[0]
    today_year = today.isocalendar()[0]
    
    return cache_year == today_year and cache_week >= today_week

# def get_all_a_shares():
#     """获取全量A股代码列表"""
#     file_path = f'{DATA_DIR}/a_stocks_list.csv'
    
#     if os.path.exists(file_path):        
#         stocks_df = pd.read_csv(file_path)
#         print(f"Read {len(stocks_df)} stock symbols from file")
#         return stocks_df[['ts_code', 'name']]
#     else:
#         try:
#             # 获取股票列表
#             stocks = pro.stock_basic(exchange='', list_status='L')
#             # 只保留A股
#             stocks = stocks[stocks['ts_code'].str.endswith(('.SH', '.SZ'))]
#             # 保存到文件
#             stocks.to_csv(file_path, index=False)
#             print(f"Saved {len(stocks)} stock symbols to file")
#             return stocks[['ts_code', 'name']]
#         except Exception as e:
#             print(f"Error getting stock list: {e}")
#             return pd.DataFrame(columns=['ts_code', 'name'])

def get_all_a_shares():
    """获取全量A股代码列表"""
    file_path = f'a_stocks_list.csv'
    
    # Check if file exists and is not older than 1 day
    if os.path.exists(file_path):        
        stocks_df = pd.read_csv(file_path)
        print(f"Read {len(stocks_df)} stock symbols from file")
        return stocks_df[['ts_code', 'name']]
    else:
        all_stocks = ak.stock_info_a_code_name()[['code', 'name']]
        all_stocks.rename(columns={'code': 'ts_code'}, inplace=True)
        # Save to file
        all_stocks.to_csv(file_path, index=False)
        print(f"Saved {len(all_stocks)} stock symbols to file")
        return all_stocks[['ts_code', 'name']]

def load_cached_data(stock_code):
    """加载本地缓存数据"""
    file_path = f"{DATA_DIR}/{stock_code}.csv"
    if os.path.exists(file_path):
        try:
            df = pd.read_csv(file_path)
            if len(df) >= HISTORY_DAYS:
                return df[len(df)-HISTORY_DAYS:]
        except Exception as e:
            print(f"Error reading cache file {stock_code}: {str(e)}")
    return None

def download_with_retry(stock_code, max_retries=3):
    """使用Tushare下载股票数据"""
    for attempt in range(max_retries):
        try:
            if attempt > 0:
                delay = min(5 * (2 ** attempt), 30)  # 最大延迟30秒
                print(f"Retry {attempt + 1}/{max_retries} for {stock_code}, waiting {delay:.1f}s...")
                time.sleep(delay)
            
            # 计算日期范围
            end_date = datetime.now().strftime("%Y%m%d")
            start_date = (datetime.now() - timedelta(days=HISTORY_DAYS*2)).strftime("%Y%m%d")
            stock_code_suf = stock_code
            if stock_code >= 800000:
                stock_code_suf = str(stock_code).zfill(6) + '.BJ'
            elif stock_code > 600000:
                stock_code_suf = str(stock_code).zfill(6) + '.SH'
            else:
                stock_code_suf = str(stock_code).zfill(6) + '.SZ'
            # 使用Tushare下载数据
            df = pro.daily(ts_code=stock_code_suf, 
                         start_date=start_date, 
                         end_date=end_date)
            
            if df is not None and len(df) >= HISTORY_DAYS:
                # 按日期排序
                df = df.sort_values('trade_date')
                df = df.tail(HISTORY_DAYS)
                
                # 保存数据
                temp_file = f"{DATA_DIR}/{stock_code}.tmp"
                df.to_csv(temp_file, index=False)
                os.replace(temp_file, f"{DATA_DIR}/{stock_code}.csv")
                print(f"Successfully downloaded {stock_code}, data length: {len(df)}")
                return True
            else:
                print(f"Insufficient data for {stock_code}: {len(df)}")
                return False
                
        except Exception as e:
            if "too many requests" in str(e).lower():
                wait_time = random.uniform(20, 30)
                print(f"Rate limited, waiting {wait_time:.1f}s...")
                time.sleep(wait_time)
            else:
                print(f"Error downloading {stock_code}: {str(e)}")
                if attempt == max_retries - 1:
                    return False
    
    return False

def batch_download_data():
    """批量下载数据"""
    if USE_CACHE or is_cache_valid():
        print("Using cached stock data")
        return 
        
    stocks = get_all_a_shares()
    print(f"Starting download for {len(stocks)} stocks...")
    
    success_count = 0
    fail_count = 0
    
    for idx, row in stocks.iterrows():
        code = row['ts_code']
        if not load_cached_data(code):
            if download_with_retry(code):
                success_count += 1
            else:
                fail_count += 1
            
            # 每5只股票暂停一下
            if (idx + 1) % 5 == 0:
                pause_time = random.uniform(5, 10)
                print(f"\nProcessed {idx + 1} stocks, success: {success_count}, failed: {fail_count}")
                print(f"Pausing for {pause_time:.1f}s...")
                time.sleep(pause_time)
    
    print(f"\nDownload completed! Success: {success_count}, Failed: {fail_count}")
    update_cache_info()

def analyze_stocks(target_price):
    """分析股票数据"""
    matched = []
    stocks = get_all_a_shares()
    
    for _, row in stocks.iterrows():
        code, name = row['ts_code'], row['name']
        df = load_cached_data(code)
        if df is not None:
            recent_data = df.tail(TARGET_DAYS)
            if HIGH_OR_LOW == 'high':
                extreme = recent_data['high'].max()
            else:
                extreme = recent_data['low'].min()
            
            if abs(extreme - target_price) < 0.001:  # 1% tolerance
                matched.append({
                    '代码': str(code).zfill(6),
                    '名称': name,
                    HIGH_OR_LOW: round(extreme, 2)
                })
    
    return pd.DataFrame(matched)

if __name__ == "__main__":
    if not TUSHARE_TOKEN or TUSHARE_TOKEN == "your_token_here":
        print("Please set your Tushare token in the script")
        sys.exit(1)
        
    try:
        init_data_dir()
        batch_download_data()
        
        result_df = analyze_stocks(TARGET)
        
        if not result_df.empty:
            print(f"\nFound {len(result_df)} matching stocks:")
            print(result_df)
            result_df.to_csv("matched_stocks.csv", index=False)
        else:
            print("No matching stocks found")
            
    except KeyboardInterrupt:
        print("\nProgram interrupted by user")
        sys.exit(0)
    except Exception as e:
        print(f"Program error: {str(e)}")
        sys.exit(1)