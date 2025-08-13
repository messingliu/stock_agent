import akshare as ak
import pandas as pd
import os
import sys
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
import time
import random
import json

# 配置参数
DATA_DIR = "./stock_data"
CACHE_FILE = os.path.join(DATA_DIR, "cache_info.json")
HISTORY_DAYS = 60  # 获取60天历史数据
TARGET_DAYS = int(sys.argv[1])  # 分析最近A天的数据
HIGH_OR_LOW = '最高' if sys.argv[2]=='high' else '最低'
TARGET = float(sys.argv[3])  # 目标价格

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
    # 计算缓存日期和今天是否在同一周内
    cache_week = cache_date.isocalendar()[1]  # 获取周数
    today_week = today.isocalendar()[1]
    cache_year = cache_date.isocalendar()[0]  # 获取年份
    today_year = today.isocalendar()[0]
    
    return cache_year == today_year and cache_week == today_week

def get_all_a_shares():
    """获取全量A股代码列表"""
    file_path = f'{DATA_DIR}/a_stocks_list.csv'
    
    # Check if file exists and is not older than 1 day
    if os.path.exists(file_path):        
        stocks_df = pd.read_csv(file_path)
        print(f"Read {len(stocks_df)} stock symbols from file")
        return stocks_df[['code', 'name']]
    else:
        all_stocks = ak.stock_info_a_code_name()[['code', 'name']]
        # Save to file
        all_stocks.to_csv(file_path, index=False)
        print(f"Saved {len(all_stocks)} stock symbols to file")
        return all_stocks[['code', 'name']]

def download_with_retry(stock_code, max_retries=3):
    """带重试机制的下载函数"""
    for attempt in range(max_retries):
        try:
            # 添加随机延迟，避免请求过于频繁
            time.sleep(random.uniform(1, 3))
            
            end_date = datetime.now().strftime("%Y%m%d")
            start_date = (datetime.now() - timedelta(days=HISTORY_DAYS*2)).strftime("%Y%m%d")
            
            df = ak.stock_zh_a_hist(
                symbol=stock_code, 
                period="daily",
                start_date=start_date,
                end_date=end_date,
                adjust="qfq"  # 前复权处理
            ).tail(HISTORY_DAYS)
            
            if len(df) >= HISTORY_DAYS:
                df.to_csv(f"{DATA_DIR}/{stock_code}.csv", index=False)
                return True
            else:
                print(f"下载{stock_code}数据不足{HISTORY_DAYS}天")
                return False
                
        except Exception as e:
            if "Rate limited" in str(e):
                # 遇到频率限制时等待更长时间
                wait_time = random.uniform(5, 10)
                print(f"遇到频率限制，等待{wait_time:.1f}秒...")
                time.sleep(wait_time)
            elif attempt < max_retries - 1:
                print(f"下载{stock_code}失败 (尝试 {attempt + 1}/{max_retries}): {str(e)}")
                time.sleep(random.uniform(2, 5))  # 失败后等待更长时间
            else:
                print(f"下载{stock_code}最终失败: {str(e)}")
                return False
    return False

def load_cached_data(stock_code):
    """加载本地缓存数据"""
    file_path = f"{DATA_DIR}/{stock_code}.csv"
    if os.path.exists(file_path):
        try:
            df = pd.read_csv(file_path)
            if len(df) >= HISTORY_DAYS:
                return df[len(df)-HISTORY_DAYS:]
        except Exception as e:
            print(f"读取缓存文件{stock_code}失败: {str(e)}")
    return None

def batch_download_data():
    """批量下载全量股票数据"""
    if is_cache_valid():
        print("使用本周缓存的股票数据")
        return
        
    stocks = get_all_a_shares()
    print(f"开始下载{len(stocks)}只A股数据...")
    #     # 更新缓存信息
    # update_cache_info()

    # 减少并发数，避免请求过于频繁
    with ThreadPoolExecutor(max_workers=2) as executor:  # 进一步减少并发数
        futures = []
        for _, row in stocks.iterrows():
            code = row['code']
            if not load_cached_data(code):
                futures.append(executor.submit(download_with_retry, code))
                # 每添加10个任务后暂停一下
                if len(futures) % 10 == 0:
                    time.sleep(random.uniform(2, 4))
        
        # 等待所有下载完成
        for future in futures:
            future.result()
    
    # 更新缓存信息
    update_cache_info()

def analyze_stocks(target_price):
    """分析本地数据"""
    matched = []
    stocks = get_all_a_shares()
    
    for _, row in stocks.iterrows():
        code, name = row['code'], row['name']
        df = load_cached_data(code)
        if df is not None:
            # 只分析最近TARGET_DAYS天的数据
            recent_data = df.tail(TARGET_DAYS)
            max_high = recent_data[HIGH_OR_LOW].max() if HIGH_OR_LOW == '最高' else recent_data[HIGH_OR_LOW].min()
            if abs(max_high - target_price) < 0.001:  # 浮点精度处理
                matched.append({'代码': code, '名称': name, HIGH_OR_LOW: round(max_high,2)})
    
    return pd.DataFrame(matched)

if __name__ == "__main__":
    init_data_dir()
    batch_download_data()  # 每次运行都检查是否需要更新数据
    
    result_df = analyze_stocks(TARGET)
    
    if not result_df.empty:
        print(f"\n找到{len(result_df)}只匹配股票：")
        print(result_df)
        result_df.to_csv("matched_stocks.csv", index=False)
    else:
        print("未找到符合要求的股票")
