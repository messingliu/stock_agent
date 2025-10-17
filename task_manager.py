"""任务管理器"""

import threading
from datetime import datetime, timedelta
from typing import Dict, Any, Optional
import asyncio
from download_all_stock_pg import main_async
from dao import TaskDAO

class TaskManager:
    """任务管理器"""
    def __init__(self):
        """初始化任务管理器"""
        self.task_dao = TaskDAO()
        self.task_dao.initialize_table()
        self._lock = threading.Lock()
        self._tasks: Dict[int, threading.Thread] = {}
    
    def _run_download_task(self, task_id: int):
        """运行下载任务"""
        try:
            # 创建新的事件循环
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
            # 运行下载任务
            loop.run_until_complete(main_async(task_id))
            loop.close()
            
        except Exception as e:
            # 更新任务状态为失败
            self.task_dao.update_task_status(
                task_id,
                'failed',
                error_message=str(e),
                end_time=datetime.now()
            )
    
    def start_download_task(self, market: str, force: bool = False) -> Dict[str, Any]:
        """启动下载任务
        
        Args:
            market: 市场代码
            force: 是否强制重新下载
        
        Returns:
            任务信息
        """
        with self._lock:
            # 检查是否有正在运行的任务
            if self.task_dao.is_task_running(market):
                return {
                    'status': 'error',
                    'message': f'A download task for {market} market is already running'
                }
            
            # 如果不是强制下载，检查最近是否已经下载过
            if not force:
                latest_task = self.task_dao.get_latest_successful_task(market)
                if latest_task and datetime.now() - datetime.fromisoformat(latest_task['end_time']) < timedelta(hours=24):
                    return {
                        'status': 'skipped',
                        'message': f'Data for {market} market was downloaded less than 24 hours ago',
                        'last_task_id': latest_task['task_id']
                    }
            
            # 创建新任务
            task_id = self.task_dao.create_task('download', market)
            
            # 启动下载线程
            thread = threading.Thread(
                target=self._run_download_task,
                args=(task_id,),
                daemon=True
            )
            self._tasks[task_id] = thread
            thread.start()
            
            return {
                'status': 'started',
                'task_id': task_id,
                'message': f'Download task for {market} market started'
            }
    
    def get_task_status(self, task_id: Optional[int] = None, market: Optional[str] = None) -> Dict[str, Any]:
        """获取任务状态
        
        Args:
            task_id: 任务ID
            market: 市场代码
        
        Returns:
            任务状态信息
        """
        status = self.task_dao.get_task_status(task_id, market)
        
        # 添加线程状态信息
        if task_id in self._tasks:
            thread = self._tasks[task_id]
            if thread.is_alive():
                status['thread_status'] = 'running'
            else:
                status['thread_status'] = 'finished'
                # 清理已完成的线程
                self._tasks.pop(task_id)
        else:
            status['thread_status'] = 'not_found'
        
        return status

# 创建全局任务管理器实例
task_manager = TaskManager()