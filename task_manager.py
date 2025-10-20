import asyncio
import threading
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from typing import Optional, Dict, Any
import download_all_stock_pg
from config import config

class TaskManager:
    def __init__(self):
        self.engine = create_engine(config.db_url)
        self._running_tasks = {}
        self._lock = threading.Lock()

    def _get_latest_task(self, task_type: str, market: str) -> Optional[Dict[str, Any]]:
        """获取最新的任务状态"""
        query = """
            SELECT *
            FROM task_status
            WHERE task_type = :task_type
            AND market = :market
            ORDER BY start_time DESC
            LIMIT 1
        """
        with self.engine.connect() as conn:
            result = conn.execute(
                text(query),
                {'task_type': task_type, 'market': market}
            )
            row = result.fetchone()
            if row:
                return dict(row._mapping)
            return None

    def _create_task(self, task_type: str, market: str) -> int:
        """创建新任务"""
        query = """
            INSERT INTO task_status (
                task_type, market, status, start_time, last_update_time,
                total_symbols, processed_symbols, failed_symbols
            )
            VALUES (
                :task_type, :market, 'running', :start_time, :last_update_time,
                0, 0, 0
            )
            RETURNING id
        """
        with self.engine.connect() as conn:
            result = conn.execute(
                text(query),
                {
                    'task_type': task_type,
                    'market': market,
                    'start_time': datetime.now(),
                    'last_update_time': datetime.now()
                }
            )
            conn.commit()
            return result.scalar()

    def _update_task(self, task_id: int, **kwargs):
        """更新任务状态"""
        set_clause = ", ".join(f"{k} = :{k}" for k in kwargs.keys())
        query = f"""
            UPDATE task_status
            SET {set_clause}, last_update_time = :last_update_time
            WHERE id = :task_id
        """
        with self.engine.connect() as conn:
            conn.execute(
                text(query),
                {
                    'task_id': task_id,
                    'last_update_time': datetime.now(),
                    **kwargs
                }
            )
            conn.commit()

    def _is_task_needed(self, task_type: str, market: str) -> bool:
        """检查是否需要运行新任务"""
        latest_task = self._get_latest_task(task_type, market)
        if not latest_task:
            return True

        # 如果有正在运行的任务，检查是否超时
        if latest_task['status'] == 'running':
            if datetime.now() - latest_task['last_update_time'] > timedelta(hours=1):
                return True
            return False

        # 如果最后一次更新是在24小时之内，不需要运行
        if datetime.now() - latest_task['last_update_time'] < timedelta(hours=24):
            return False

        return True

    async def _run_download_task(self, task_id: int, market: str):
        """运行下载任务"""
        try:
            # 设置下载标志
            if market.lower() == 'cn':
                download_all_stock_pg.china_stock = True
            else:
                download_all_stock_pg.china_stock = False

            # 运行下载任务
            await download_all_stock_pg.main_async()

            # 更新任务状态
            self._update_task(
                task_id,
                status='completed',
                end_time=datetime.now(),
                failed_symbols=len(download_all_stock_pg.stats.failed),
                success_symbols=download_all_stock_pg.stats.success,
                total_symbols=download_all_stock_pg.stats.total
            )

        except Exception as e:
            self._update_task(
                task_id,
                status='failed',
                end_time=datetime.now(),
                error_message=str(e)
            )
            raise

    def start_download_task(self, market: str) -> Dict[str, Any]:
        """启动下载任务"""
        task_type = 'stock_download'
        
        with self._lock:
            # 检查是否需要运行新任务
            if not self._is_task_needed(task_type, market):
                latest_task = self._get_latest_task(task_type, market)
                return {
                    'status': 'skipped',
                    'message': 'Recent task exists',
                    'last_task': latest_task
                }

            # 创建新任务
            task_id = self._create_task(task_type, market)

            # 在后台运行任务
            loop = asyncio.new_event_loop()
            thread = threading.Thread(
                target=self._run_task_in_thread,
                args=(loop, task_id, market)
            )
            thread.daemon = True
            thread.start()

            return {
                'status': 'started',
                'task_id': task_id,
                'message': 'Task started successfully'
            }

    def _run_task_in_thread(self, loop, task_id: int, market: str):
        """在新线程中运行异步任务"""
        asyncio.set_event_loop(loop)
        loop.run_until_complete(self._run_download_task(task_id, market))
        loop.close()

    def get_task_status(self, task_id: Optional[int] = None, market: Optional[str] = None) -> Dict[str, Any]:
        """获取任务状态"""
        if task_id:
            query = "SELECT * FROM task_status WHERE id = :task_id"
            params = {'task_id': task_id}
        elif market:
            query = """
                SELECT *
                FROM task_status
                WHERE market = :market
                ORDER BY start_time DESC
                LIMIT 1
            """
            params = {'market': market}
        else:
            query = """
                SELECT *
                FROM task_status
                ORDER BY start_time DESC
                LIMIT 10
            """
            params = {}

        with self.engine.connect() as conn:
            result = conn.execute(text(query), params)
            rows = result.fetchall()
            return [dict(row._mapping) for row in rows]

# 创建全局任务管理器实例
task_manager = TaskManager()
