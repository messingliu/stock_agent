import yfinance as yf
import pandas as pd
import os
import datetime
import argparse
import time
import random
from tqdm import tqdm
import pickle
from pandas_datareader import nasdaq_trader

def download_all_us_stocks():
    """
    Download a list of all US stocks from NASDAQ and NYSE exchanges and save to a local file
    """
    print("Downloading list of US stocks from NASDAQ and NYSE...")
    
    try:
        # Try multiple sources for NASDAQ and NYSE stock lists
        all_stocks = pd.DataFrame()
        
        # Third attempt - try ftp.nasdaqtrader.com
        try:
            url = "ftp://ftp.nasdaqtrader.com/SymbolDirectory/nasdaqlisted.txt"
            nasdaq_stocks = pd.read_csv(url, sep='|', skipfooter=1, engine='python')
            # Print column names to debug
            print(f"NASDAQ columns: {nasdaq_stocks.columns.tolist()}")
            
            # Find the symbol column (case insensitive)
            symbol_col = None
            for col in nasdaq_stocks.columns:
                if 'symbol' in col.lower():
                    symbol_col = col
                    break
            
            if symbol_col:
                nasdaq_stocks = nasdaq_stocks[[symbol_col]].copy()
                nasdaq_stocks.columns = ['Symbol']  # Rename to standard name
                all_stocks = pd.concat([all_stocks, nasdaq_stocks])
                print(f"Downloaded {len(nasdaq_stocks)} NASDAQ stocks from nasdaqtrader.com")
            else:
                print("Could not find Symbol column in NASDAQ data")
            
            url = "ftp://ftp.nasdaqtrader.com/SymbolDirectory/otherlisted.txt"
            other_stocks = pd.read_csv(url, sep='|', skipfooter=1, engine='python')
            # Print column names to debug
            print(f"Other exchanges columns: {other_stocks.columns.tolist()}")
            
            # Find symbol and exchange columns (case insensitive)
            symbol_col = None
            exchange_col = None
            for col in other_stocks.columns:
                if 'symbol' in col.lower():
                    symbol_col = col
                if 'exchange' in col.lower():
                    exchange_col = col
            
            if symbol_col and exchange_col:
                nyse_stocks = other_stocks[other_stocks[exchange_col] == 'N'][[symbol_col]].copy()
                nyse_stocks.columns = ['Symbol']  # Rename to standard name
                all_stocks = pd.concat([all_stocks, nyse_stocks])
                print(f"Downloaded {len(nyse_stocks)} NYSE stocks from nasdaqtrader.com")
            else:
                print("Could not find Symbol or Exchange columns in other exchanges data")
        except Exception as e:
            print(f"Could not download stocks from nasdaqtrader.com: {e}")
        
        
        # Remove duplicates and clean up
        if not all_stocks.empty:
            # Handle NaN values before filtering
            all_stocks = all_stocks.dropna(subset=['Symbol'])
            # Filter out non-standard symbols
            all_stocks = all_stocks[all_stocks['Symbol'].str.contains('^[A-Za-z]+$', regex=True, na=False)]
            
            # Save to file
            all_stocks.to_csv('us_stocks_list.csv', index=False)
            print(f"Saved {len(all_stocks)} stock symbols to file")
            return all_stocks['Symbol'].tolist()
        else:
            raise Exception("No stocks were downloaded from any source")
    except Exception as e:
        print(f"Error downloading stock list: {e}")
        # Fallback to a basic list of major stocks if all download attempts fail
        print("Using fallback stock list of major NASDAQ and NYSE stocks")
        return ['AAPL', 'MSFT', 'AMZN', 'GOOGL', 'META', 'TSLA', 'NVDA', 'JPM', 'V', 'JNJ', 
                'UNH', 'HD', 'PG', 'MA', 'BAC', 'XOM', 'AVGO', 'CVX', 'COST', 'ABBV', 
                'MRK', 'KO', 'PEP', 'ADBE', 'WMT', 'CRM', 'LLY', 'TMO', 'MCD', 'CSCO',
                'ACN', 'ABT', 'DHR', 'CMCSA', 'NKE', 'DIS', 'VZ', 'NEE', 'TXN', 'PM',
                'WFC', 'BMY', 'RTX', 'AMD', 'UPS', 'HON', 'QCOM', 'IBM', 'INTC', 'AMGN']

def get_stock_list():
    """
    Get list of US stocks from local file or download if needed
    """
    file_path = 'us_stocks_list.csv'
    
    # Check if file exists and is not older than 1 day
    if os.path.exists(file_path):
        file_time = os.path.getmtime(file_path)
        file_date = datetime.datetime.fromtimestamp(file_time).date()
        today = datetime.datetime.now().date()
        
        if file_date >= today:
            print(f"Using existing stock list from {file_date}")
            stocks_df = pd.read_csv(file_path)
            return stocks_df['Symbol'].tolist()
    
    # If file doesn't exist or is outdated, download new data
    return download_all_us_stocks()


def load_stock_data_cache():
    """
    Load cached stock price data from local file
    """
    cache_file = 'stock_data_cache.pkl'
    cache = {}
    cache_date = None
    
    if os.path.exists(cache_file):
        try:
            with open(cache_file, 'rb') as f:
                cache_data = pickle.load(f)
                cache = cache_data.get('data', {})
                cache_date = cache_data.get('date')
                
            print(f"Loaded cached stock data from {cache_date}")
        except Exception as e:
            print(f"Error loading cache: {e}")
    
    return cache, cache_date


def save_stock_data_cache(cache):
    """
    Save stock price data to local cache file
    """
    cache_file = 'stock_data_cache.pkl'
    cache_data = {
        'data': cache,
        'date': datetime.datetime.now().date()
    }
    
    try:
        with open(cache_file, 'wb') as f:
            pickle.dump(cache_data, f)
        print(f"Saved stock data cache with {len(cache)} stocks")
    except Exception as e:
        print(f"Error saving cache: {e}")


def update_stock_data_cache(stocks):
    """
    Update the stock data cache with the latest 60 days of price data
    """
    cache, cache_date = load_stock_data_cache()
    today = datetime.datetime.now().date()
    
    if cache and today - cache_date <= datetime.timedelta(days=5):
        print("Using this week's cached stock data")
        return cache

    print("Updating stock data cache...")
    new_cache = {}
    end_date = datetime.datetime.now()
    start_date = end_date - datetime.timedelta(days=65)

    stocks_to_download = [s for s in stocks if s not in new_cache]

    if stocks_to_download:
        print(f"Downloading data for {len(stocks_to_download)} stocks...")
        with tqdm(total=len(stocks_to_download)) as progress:
            for ticker in stocks_to_download:
                try:
                    stock = yf.Ticker(ticker)
                    hist = stock.history(start=start_date, end=end_date)
                    
                    if not hist.empty and len(hist) >= 2:
                        new_cache[ticker] = hist
                    
                    # Add a small delay to avoid hitting API rate limits
                    time.sleep(1)
                except Exception as e:
                    progress.write(f"Error downloading {ticker}: {str(e)}")
                
                progress.update(1)

    
    save_stock_data_cache(new_cache)
    return new_cache


def check_price_condition(ticker, hist, days, condition_type, target_price, progress=None):
    """
    Check if a stock meets the price condition
    """
    try:
        if hist.empty or len(hist) < 2:
            return False, None
        
        # Get the last 'days' days of data
        recent_data = hist.tail(days)
        
        if condition_type.lower() == 'high':
            extreme_price = recent_data['High'].max()
            date_of_extreme = recent_data['High'].idxmax()
        else:  # 'low'
            extreme_price = recent_data['Low'].min()
            date_of_extreme = recent_data['Low'].idxmin()
        
        # Check if the extreme price is close to the target price (within 0.5%)
        price_diff_percent = abs((extreme_price - target_price) / target_price * 100)
        
        if price_diff_percent <= 0.01:  # Within 0.5% tolerance
            current_price = hist['Close'].iloc[-1]
            return True, {
                'ticker': ticker,
                'extreme_price': extreme_price,
                'date_of_extreme': date_of_extreme.strftime('%Y-%m-%d'),
                'current_price': current_price,
                'price_change': ((current_price - extreme_price) / extreme_price) * 100
            }
        
        return False, None
    
    except Exception as e:
        if progress:
            progress.write(f"Error processing {ticker}: {str(e)}")
        return False, None


def find_matching_stocks(days, condition_type, target_price):
    """
    Find all stocks that meet the specified condition
    """
    stocks = get_stock_list()
    print(f"finished getting stock list")

    stock_data_cache = update_stock_data_cache(stocks)
    matching_stocks = []
    
    print(f"Checking {len(stock_data_cache)} stocks for {condition_type} price of {target_price} in past {days} days...")
    
    with tqdm(total=len(stock_data_cache)) as progress:
        for ticker, hist in stock_data_cache.items():
            meets_condition, stock_data = check_price_condition(
                ticker, hist, days, condition_type, target_price, progress
            )
            
            if meets_condition:
                matching_stocks.append(stock_data)
                progress.write(f"Found match: {ticker}")
            
            progress.update(1)
    
    return matching_stocks


def save_results(matching_stocks, days, condition_type, target_price):
    """
    Save the results to a CSV file
    """
    if not matching_stocks:
        print("No matching stocks found.")
        return
    
    results_df = pd.DataFrame(matching_stocks)
    
    # Create filename with parameters
    filename = f"stocks_{condition_type}_{days}days_{target_price}_{datetime.datetime.now().strftime('%Y%m%d')}.csv"
    
    #results_df.to_csv(filename, index=False)
    print(f"Results saved to {filename}")
    
    # Display results
    print(f"\nFound {len(matching_stocks)} matching stocks:")
    print(results_df.to_string())


def main():
    parser = argparse.ArgumentParser(description='Find stocks meeting specific price criteria')
    parser.add_argument('days', type=int, help='Number of days to look back')
    parser.add_argument('condition_type', choices=['high', 'low'], help='Whether to check highest or lowest price')
    parser.add_argument('target_price', type=float, help='Target price to match')
    
    args = parser.parse_args()
    
    matching_stocks = find_matching_stocks(args.days, args.condition_type, args.target_price)
    save_results(matching_stocks, args.days, args.condition_type, args.target_price)


if __name__ == "__main__":
    main()

