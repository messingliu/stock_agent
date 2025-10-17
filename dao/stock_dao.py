"""股票数据访问对象"""

from datetime import datetime
import pandas as pd
from typing import List, Dict, Any, Optional
from .base import BaseDAO, text

class StockDAO(BaseDAO):
    def __init__(self, market: str):
        """初始化股票DAO
        
        Args:
            market: 市场代码 ('us' 或 'cn')
        """
        super().__init__()
        self.market = market.lower()
        self.price_table = f"{self.market}_stock_prices"
        self.info_table = f"{self.market}_stocks_info"
    
    def get_stored_symbols_count(self) -> int:
        """获取数据库中存储的股票数量"""
        query = f"SELECT COUNT(*) FROM {self.info_table}"
        return self.fetch_scalar(query)
    
    def update_stock_info(self, symbols: List[Dict[str, Any]]):
        """更新股票基本信息
        
        Args:
            symbols: 股票信息列表，每个元素包含 symbol, name, exchange 字段
        """
        for symbol_info in symbols:
            query = f"""
                INSERT INTO {self.info_table} 
                    (symbol, name, exchange, market, update_time)
                VALUES 
                    (:symbol, :name, :exchange, :market, :update_time)
                ON CONFLICT (symbol) DO UPDATE SET
                    name = EXCLUDED.name,
                    exchange = EXCLUDED.exchange,
                    market = EXCLUDED.market,
                    update_time = EXCLUDED.update_time
            """
            self.execute(query, {
                'symbol': symbol_info['symbol'],
                'name': symbol_info.get('name', symbol_info['symbol']),
                'exchange': symbol_info['exchange'],
                'market': self.market,
                'update_time': datetime.now()
            })
    
    def get_symbols_from_db(self, exclude_finished: bool = False) -> List[Dict[str, Any]]:
        """从数据库获取股票信息
        
        Args:
            exclude_finished: 是否排除已完成的股票（仅适用于中国股票）
        
        Returns:
            股票信息列表
        """
        query = f"SELECT * FROM {self.info_table}"
        rows = self.fetch_all(query)
        
        symbols = []
        finished_symbols = set()
        
        # 如果是中国股票且需要排除已完成的，读取完成列表
        if exclude_finished and self.market == 'cn':
            import os
            filename = os.path.join('stock_lists', 'successful_symbols_cn.txt')
            if os.path.exists(filename):
                with open(filename, 'r') as f:
                    finished_symbols = {line.strip().split('|')[0] for line in f}
        
        for row in rows:
            if not exclude_finished or row.symbol not in finished_symbols:
                symbols.append({
                    'symbol': row.symbol,
                    'name': row.name,
                    'exchange': row.exchange,
                    'market': row.market
                })
        
        return symbols
    
    def get_stock_prices(self, days: int = 10) -> pd.DataFrame:
        """获取指定天数的股票价格数据
        
        Args:
            days: 获取的天数
        
        Returns:
            包含股票价格数据的DataFrame
        """
        end_date = datetime.now().date()
        start_date = end_date - pd.Timedelta(days=days)
        
        query = f"""
            SELECT *
            FROM {self.price_table}
            WHERE date >= :start_date
            ORDER BY symbol, date
        """
        
        with self.engine.connect() as conn:
            df = pd.read_sql_query(
                text(query),
                conn,
                params={'start_date': start_date}
            )
        return df
    
    def upsert_stock_prices(self, symbol: str, prices_df: pd.DataFrame):
        """更新或插入股票价格数据
        
        Args:
            symbol: 股票代码
            prices_df: 包含价格数据的DataFrame
        """
        for _, row in prices_df.iterrows():
            query = f"""
                INSERT INTO {self.price_table} (
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
            """
            
            self.execute(query, {
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
    
    def initialize_tables(self):
        """初始化数据库表"""
        # 创建股票信息表
        self.execute(f"""
            CREATE TABLE IF NOT EXISTS {self.info_table} (
                symbol VARCHAR(20) PRIMARY KEY,
                name VARCHAR(100),
                exchange VARCHAR(20),
                market VARCHAR(10),
                update_time TIMESTAMP
            )
        """)
        
        # 创建价格表
        self.execute(f"""
            CREATE TABLE IF NOT EXISTS {self.price_table} (
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
        """)
        
        # 创建索引
        self.execute(f"CREATE INDEX IF NOT EXISTS idx_{self.market}_stock_prices_symbol ON {self.price_table}(symbol)")
        self.execute(f"CREATE INDEX IF NOT EXISTS idx_{self.market}_stock_prices_date ON {self.price_table}(date)")
