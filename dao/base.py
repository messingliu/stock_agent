"""基础DAO类，提供数据库连接和通用操作"""

from sqlalchemy import create_engine, text
from config import config

class BaseDAO:
    def __init__(self):
        """初始化数据库连接"""
        self.engine = create_engine(config.db_url)
    
    def execute(self, query, params=None):
        """执行SQL查询"""
        with self.engine.begin() as conn:
            return conn.execute(text(query), params or {})
    
    def fetch_all(self, query, params=None):
        """执行查询并返回所有结果"""
        with self.engine.connect() as conn:
            result = conn.execute(text(query), params or {})
            return result.fetchall()
    
    def fetch_one(self, query, params=None):
        """执行查询并返回单个结果"""
        with self.engine.connect() as conn:
            result = conn.execute(text(query), params or {})
            return result.fetchone()
    
    def fetch_scalar(self, query, params=None):
        """执行查询并返回单个值"""
        with self.engine.connect() as conn:
            result = conn.execute(text(query), params or {})
            return result.scalar()
