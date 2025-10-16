import os
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine, text
from typing import List, Dict, Any
from dotenv import load_dotenv

# 导入配置和策略
from config import config
from strategies import AVAILABLE_STRATEGIES, Strategy

def get_db_engine():
    """创建PostgreSQL数据库连接"""
    return create_engine(config.db_url)

class StockAnalyzer:
    """股票分析器"""
    def __init__(self, market: str):
        self.engine = get_db_engine()
        self.market = market.lower()
        self.strategies: List[Strategy] = []

    def add_strategy(self, strategy: Strategy):
        """添加策略"""
        self.strategies.append(strategy)

    def get_stock_data(self, days: int = 10) -> pd.DataFrame:
        """获取指定天数的股票数据"""
        table_name = f"{self.market}_stock_prices"
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=days)

        query = f"""
        SELECT *
        FROM {table_name}
        WHERE date >= :start_date
        ORDER BY symbol, date
        """

        try:
            with self.engine.connect() as conn:
                df = pd.read_sql_query(
                    text(query),
                    conn,
                    params={'start_date': start_date}
                )
            return df
        except Exception as e:
            print(f"Error getting stock data: {str(e)}")
            return pd.DataFrame()

    def analyze(self, min_days: int = 2) -> Dict[str, List[Dict[str, Any]]]:
        """分析所有股票"""
        results = {strategy.name: [] for strategy in self.strategies}
        
        # 获取股票数据
        df = self.get_stock_data(days=min_days + 5)  # 多获取几天数据以确保有足够的历史数据
        if df.empty:
            return results

        # 按股票代码分组分析
        for symbol, group in df.groupby('symbol'):
            if len(group) < min_days:
                continue
            
            # 获取最新数据
            latest_data = group.iloc[-1]
            
            # 对每个策略进行分析
            for strategy in self.strategies:
                try:
                    if strategy.apply(group):
                        stock_info = {
                            'symbol': symbol,
                            'date': latest_data['date'].strftime('%Y-%m-%d'),
                            'close': round(latest_data['close'], 2),
                            'change_percent': round((latest_data['close'] - latest_data['open']) / latest_data['open'] * 100, 2),
                            'volume': int(latest_data['volume']),
                            'ma60': round(latest_data['ma60'], 2)
                        }
                        results[strategy.name].append(stock_info)
                except Exception as e:
                    print(f"Error analyzing {symbol} with strategy {strategy.name}: {str(e)}")
                    continue
        return results

def apply_strategies(market: str, strategy_name: str = None) -> Dict[str, List[Dict[str, Any]]]:
    """应用策略查找股票"""
    analyzer = StockAnalyzer(market)
    
    if strategy_name:
        if strategy_name not in AVAILABLE_STRATEGIES:
            raise ValueError(f"Strategy {strategy_name} not found. Available strategies: {list(AVAILABLE_STRATEGIES.keys())}")
        strategy_class = AVAILABLE_STRATEGIES[strategy_name]
        analyzer.add_strategy(strategy_class())
    else:
        # 如果没有指定策略，使用所有可用策略
        for strategy_class in AVAILABLE_STRATEGIES.values():
            analyzer.add_strategy(strategy_class())
    
    return analyzer.analyze()

def main():
    # 创建分析器
    us_analyzer = StockAnalyzer('us')
    cn_analyzer = StockAnalyzer('cn')

    # 添加所有策略
    for strategy_class in AVAILABLE_STRATEGIES.values():
        strategy = strategy_class()
        us_analyzer.add_strategy(strategy)
        cn_analyzer.add_strategy(strategy)

    # 分析美股
    print("\n分析美股市场...")
    us_results = us_analyzer.analyze()
    for strategy_name, stocks in us_results.items():
        print(f"\n{strategy_name} 策略找到 {len(stocks)} 只股票:")
        if stocks:
            print(f"{'代码':<8} {'日期':<12} {'收盘价':<8} {'涨跌幅':<8} {'成交量':<12} {'MA60':<8}")
            print("-" * 60)
            for stock in stocks:
                print(f"{stock['symbol']:<8} {stock['date']:<12} {stock['close']:<8} {stock['change_percent']:>6.2f}% {stock['volume']:<12} {stock['ma60']:<8}")

    # 分析A股
    print("\n分析A股市场...")
    cn_results = cn_analyzer.analyze()
    for strategy_name, stocks in cn_results.items():
        print(f"\n{strategy_name} 策略找到 {len(stocks)} 只股票:")
        if stocks:
            print(f"{'代码':<8} {'日期':<12} {'收盘价':<8} {'涨跌幅':<8} {'成交量':<12} {'MA60':<8}")
            print("-" * 60)
            for stock in stocks:
                print(f"{stock['symbol']:<8} {stock['date']:<12} {stock['close']:<8} {stock['change_percent']:>6.2f}% {stock['volume']:<12} {stock['ma60']:<8}")

if __name__ == "__main__":
    main()