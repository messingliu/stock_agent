import yfinance as yf
import pandas as pd
import os
import datetime
import argparse
import pickle
import time

CACHE_FILE = "us_stock_cache.pkl"
CACHE_DATE_FILE = "us_stock_cache_date.txt"
STOCK_LIST_FILE = "us_stocks_list.csv"

def get_stock_list():
    # Download or load the list of US stocks (NASDAQ + NYSE)
    if os.path.exists(STOCK_LIST_FILE):
        stocks_df = pd.read_csv(STOCK_LIST_FILE)
        return stocks_df['Symbol'].tolist()
    # Download from nasdaqtrader
    url1 = "ftp://ftp.nasdaqtrader.com/SymbolDirectory/nasdaqlisted.txt"
    url2 = "ftp://ftp.nasdaqtrader.com/SymbolDirectory/otherlisted.txt"
    nasdaq = pd.read_csv(url1, sep='|', skipfooter=1, engine='python')
    other = pd.read_csv(url2, sep='|', skipfooter=1, engine='python')
    nasdaq = nasdaq[['Symbol']]
    nyse = other[other['Exchange'] == 'N'][['ACT Symbol']]
    nyse.columns = ['Symbol']
    all_stocks = pd.concat([nasdaq, nyse]).drop_duplicates()
    all_stocks.to_csv(STOCK_LIST_FILE, index=False)
    return all_stocks['Symbol'].tolist()

def is_cache_valid():
    if not os.path.exists(CACHE_FILE) or not os.path.exists(CACHE_DATE_FILE):
        return False
    with open(CACHE_DATE_FILE, 'r') as f:
        cache_date = f.read().strip()
    today = datetime.datetime.now().strftime('%Y-%m-%d')
    return cache_date == today

def load_cache():
    with open(CACHE_FILE, 'rb') as f:
        return pickle.load(f)

def save_cache(data):
    with open(CACHE_FILE, 'wb') as f:
        pickle.dump(data, f)
    with open(CACHE_DATE_FILE, 'w') as f:
        f.write(datetime.datetime.now().strftime('%Y-%m-%d'))

def download_stock_data(symbols):
    # Download up to 60 days of data for all symbols
    data = {}
    end = datetime.datetime.now()
    start = end - datetime.timedelta(days=65)
    for symbol in symbols:
        try:
            stock = yf.Ticker(symbol)
            hist = stock.history(start=start, end=end)
            if not hist.empty:
                data[symbol] = hist.tail(60)
            time.sleep(0.1)  # avoid rate limit
        except Exception as e:
            print(f"Error downloading {symbol}: {e}")
    return data

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('A', type=int, help='Number of days to look back')
    parser.add_argument('B', choices=['high', 'low'], help='high or low')
    parser.add_argument('C', type=float, help='Target price')
    args = parser.parse_args()

    # Step 1: Get stock list
    symbols = get_stock_list()

    # Step 2: Load or download cache
    if is_cache_valid():
        print("Using cached stock data.")
        stock_data = load_cache()
    else:
        print("Downloading stock data (this may take a while)...")
        stock_data = download_stock_data(symbols)
        save_cache(stock_data)

    # Step 3: Find matches
    matches = []
    for symbol, hist in stock_data.items():
        if len(hist) < args.A:
            continue
        recent = hist.tail(args.A)
        if args.B == 'high':
            extreme = recent['High'].max()
        else:
            extreme = recent['Low'].min()
        if abs(extreme - args.C) < 0.01:  # float tolerance
            matches.append({'Symbol': symbol, 'Extreme': round(extreme, 2)})

    # Step 4: Output
    if matches:
        df = pd.DataFrame(matches)
        print(df)
        df.to_csv(f"us_stock_matches_{args.B}_{args.A}d_{args.C}.csv", index=False)
    else:
        print("No matching stocks found.")

if __name__ == "__main__":
    main()
