"""任务状态数据访问对象"""

from datetime import datetime
from typing import Dict, Any, Optional
from .base import BaseDAO

class TaskDAO(BaseDAO):
    def __init__(self):
        """初始化任务DAO"""
        super().__init__()
        self.table = "task_status"
    
    def initialize_table(self):
        """初始化任务状态表"""
        self.execute("""
            CREATE TABLE IF NOT EXISTS task_status (
                id SERIAL PRIMARY KEY,
                task_type VARCHAR(50),
                market VARCHAR(10),
                status VARCHAR(20),
                start_time TIMESTAMP,
                end_time TIMESTAMP,
                last_update_time TIMESTAMP,
                total_symbols INT,
                processed_symbols INT,
                failed_symbols INT,
                error_message TEXT
            )
        """)
    
    def create_task(self, task_type: str, market: str) -> int:
        """创建新任务
        
        Args:
            task_type: 任务类型
            market: 市场代码
        
        Returns:
            任务ID
        """
        query = """
            INSERT INTO task_status (
                task_type, market, status, start_time, 
                last_update_time, processed_symbols, failed_symbols
            )
            VALUES (
                :task_type, :market, 'pending', CURRENT_TIMESTAMP,
                CURRENT_TIMESTAMP, 0, 0
            )
            RETURNING id
        """
        result = self.fetch_one(query, {
            'task_type': task_type,
            'market': market
        })
        return result.id
    
    def update_task_status(self, task_id: int, status: str, **kwargs):
        """更新任务状态
        
        Args:
            task_id: 任务ID
            status: 新状态
            **kwargs: 其他需要更新的字段
        """
        set_clauses = ['status = :status', 'last_update_time = CURRENT_TIMESTAMP']
        params = {'task_id': task_id, 'status': status}
        
        # 添加其他需要更新的字段
        for key, value in kwargs.items():
            set_clauses.append(f"{key} = :{key}")
            params[key] = value
        
        query = f"""
            UPDATE task_status 
            SET {', '.join(set_clauses)}
            WHERE id = :task_id
        """
        self.execute(query, params)
    
    def get_task_status(self, task_id: Optional[int] = None, market: Optional[str] = None) -> Dict[str, Any]:
        """获取任务状态
        
        Args:
            task_id: 任务ID
            market: 市场代码
        
        Returns:
            任务状态信息
        """
        conditions = []
        params = {}
        
        if task_id is not None:
            conditions.append("id = :task_id")
            params['task_id'] = task_id
        
        if market is not None:
            conditions.append("market = :market")
            params['market'] = market
        
        where_clause = " AND ".join(conditions) if conditions else "TRUE"
        
        query = f"""
            SELECT *
            FROM task_status
            WHERE {where_clause}
            ORDER BY start_time DESC
            LIMIT 1
        """
        
        row = self.fetch_one(query, params)
        if not row:
            return {}
        
        return {
            'task_id': row.id,
            'task_type': row.task_type,
            'market': row.market,
            'status': row.status,
            'start_time': row.start_time.isoformat() if row.start_time else None,
            'end_time': row.end_time.isoformat() if row.end_time else None,
            'last_update_time': row.last_update_time.isoformat() if row.last_update_time else None,
            'total_symbols': row.total_symbols,
            'processed_symbols': row.processed_symbols,
            'failed_symbols': row.failed_symbols,
            'error_message': row.error_message
        }
    
    def is_task_running(self, market: str) -> bool:
        """检查是否有正在运行的任务
        
        Args:
            market: 市场代码
        
        Returns:
            是否有正在运行的任务
        """
        query = """
            SELECT COUNT(*) 
            FROM task_status 
            WHERE market = :market 
            AND status = 'running'
        """
        count = self.fetch_scalar(query, {'market': market})
        return count > 0
    
    def get_latest_successful_task(self, market: str) -> Dict[str, Any]:
        """获取最近一次成功的任务
        
        Args:
            market: 市场代码
        
        Returns:
            任务信息
        """
        query = """
            SELECT *
            FROM task_status
            WHERE market = :market
            AND status = 'completed'
            ORDER BY end_time DESC
            LIMIT 1
        """
        
        row = self.fetch_one(query, {'market': market})
        if not row:
            return {}
            
        return {
            'task_id': row.id,
            'end_time': row.end_time
        }
