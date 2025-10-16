import os
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine, text
from abc import ABC, abstractmethod
from typing import List, Dict, Any
from dataclasses import dataclass
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()

# 数据库配置
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432'),
    'database': os.getenv('DB_NAME', 'stock_db'),
    'user': os.getenv('DB_USER', 'mengliu'),
    'password': os.getenv('DB_PASSWORD', 'password')
}

def get_db_engine():
    """创建PostgreSQL数据库连接"""
    return create_engine(
        f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    )

def calc_goldenline_double_green_win(today: pd.Series, yesterday: pd.Series) -> bool:
    """
    计算双阳夹MA60策略
    """
    try:
        # 规则1：今天收阳，昨天收阴
        rule1 = (today['close'] > today['open']) and (yesterday['close'] < yesterday['open'])

        # 规则2：今天实体大于昨天实体
        today_body = today['close'] - today['open']
        yesterday_body = yesterday['open'] - yesterday['close']
        rule2 = today_body > yesterday_body

        # 规则3：今天最低价低于MA60，收盘价高于MA60
        rule3 = (today['low'] < today['ma60']) and (today['close'] > today['ma60'])

        # 规则4：今天成交量大于昨天
        rule4 = today['volume'] > yesterday['volume']

        # 规则5：今天收盘价高于昨天开盘价
        rule5 = today['close'] > yesterday['open']

        return all([rule1, rule2, rule3, rule4, rule5])
    except Exception as e:
        print(f"Error applying calc_goldenline_double_green_win to {today['symbol']}: {str(e)}")
        return False

@dataclass
class StockData:
    """股票数据类"""
    symbol: str
    date: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int
    ma5: float
    ma10: float
    ma20: float
    ma60: float
    ma200: float

class Strategy(ABC):
    """策略基类"""
    def __init__(self, name: str, description: str):
        self.name = name
        self.description = description

    @abstractmethod
    def apply(self, data: pd.DataFrame) -> bool:
        """应用策略"""
        pass

    def __str__(self):
        return f"{self.name}: {self.description}"

class GoldenLineDoubleGreenWin(Strategy):
    """双阳夹MA60策略"""
    def __init__(self):
        super().__init__(
            name="GoldenLineDoubleGreenWin",
            description="双阳夹MA60策略：连续两天上涨，第二天的涨幅大于第一天，且股价突破MA60，放量"
        )

    def apply(self, data: pd.DataFrame) -> bool:
        """
        应用双阳夹MA60策略
        规则：
        1. today's close > open and yesterday's close < open
        2. today's close - open > yesterday's open - close
        3. today's low < MA60 and today's close > MA60
        4. todays' volume > yesterday's volume
        5. today's close > yesterday's open
        """
        if len(data) < 2:  # 需要至少两天的数据
            return False

        today = data.iloc[-1]
        yesterday = data.iloc[-2]

        return calc_goldenline_double_green_win(today, yesterday)

class GoldenLineDoubleGreenWinWithConfirmation(Strategy):
    """双阳夹MA60策略，且第三天确认"""
    def __init__(self):
        super().__init__(
            name="GoldenLineDoubleGreenWinWithConfirmation",
            description="双阳夹MA60策略：连续两天上涨，第二天的涨幅大于第一天，且股价突破MA60，而且第三天确认"
        )

    def apply(self, data: pd.DataFrame) -> bool:

        if len(data) < 3:  # 需要至少三天的数据
            return False

        today = data.iloc[-1]
        yesterday = data.iloc[-2]
        third = data.iloc[-3]
        if not calc_goldenline_double_green_win(today, yesterday):
            return False
        return today['open'] > yesterday['close'] or today['close'] > yesterday['close']

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

def apply_strategies(market: str, strategy: str) -> Dict[str, List[Dict[str, Any]]]:
    """应用策略查找股票"""
    analyzer = StockAnalyzer(market)
    strategy_class = globals()[strategy]()
    analyzer.add_strategy(strategy_class)
    return analyzer.analyze()

def main():
    # 创建分析器
    us_analyzer = StockAnalyzer('us')
    cn_analyzer = StockAnalyzer('cn')

    # 添加策略
    golden_line_strategy = GoldenLineDoubleGreenWin()
    us_analyzer.add_strategy(golden_line_strategy)
    cn_analyzer.add_strategy(golden_line_strategy)

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
