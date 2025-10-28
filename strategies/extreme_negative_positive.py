"""极阴次阳策略"""

from .base import Strategy
from .utils.indicators import (
    is_down, is_up, volume_increase,
    get_change_percent, calc_body
)
import pandas as pd
import numpy as np

class ExtremeNegativePositiveStrategy(Strategy):
    """极阴次阳策略
    
    条件：
    1. 极阴：连续下跌7-9天（极限10天以上）
    2. 次阳：
       - 涨幅5%以上的中阳线
       - 放量（最好是倍量）
       - 收盘价在最近中大阴柱的1/2位置以上
    3. 最后一根阴柱：
       - 踩到重要支撑点
       - 必须缩量
    4. 从高点下跌幅度要求：
       - 下跌30%-50%或以上
       - 周线或月线的高量柱实顶不能有效跌破
       - 极限不能破高量柱的1/2位置
    """
    def __init__(self):
        super().__init__(
            name="ExtremeNegativePositive",
            description="极阴次阳：连续下跌后的放量阳线反转形态"
        )
    
    def _check_continuous_decline(self, data: pd.DataFrame, min_days: int = 7) -> bool:
        """检查是否连续下跌
        
        Args:
            data: 股票数据
            min_days: 最小下跌天数（默认7天）
        
        Returns:
            bool: 是否满足连续下跌条件
        """
        if len(data) < min_days + 1:  # 需要额外一天计算涨跌
            return False
            
        # 获取最近min_days天的数据（不包括最新一天）
        recent_data = data.iloc[-(min_days+1):-1]
        
        # 计算每天的涨跌幅
        daily_changes = recent_data['close'].pct_change()
        
        # 检查是否连续下跌（允许一天小幅上涨，不超过1%）
        negative_days = sum(daily_changes <= 0.01)
        return negative_days >= min_days - 1  # 允许一天不下跌
    
    def _check_second_positive(self, data: pd.DataFrame) -> bool:
        """检查次阳线条件
        
        Returns:
            bool: 是否满足次阳线条件
        """
        if len(data) < 2:
            return False
            
        today = data.iloc[-1]
        prev_day = data.iloc[-2]
        
        # 检查是否是阳线
        if not is_up(today):
            return False
            
        # 检查涨幅是否超过5%
        change_percent = get_change_percent(today)
        if change_percent < 5:
            return False
            
        # 检查是否放量（至少1.5倍）
        if today['volume'] < prev_day['volume'] * 1.5:
            return False
            
        # 找到最近的中大阴柱
        for i in range(len(data)-2, max(-1, len(data)-10), -1):
            day = data.iloc[i]
            if is_down(day) and abs(get_change_percent(day)) > 2:  # 跌幅超过2%的阴线
                # 检查收盘价是否在阴柱1/2位置以上
                half_point = day['low'] + (day['high'] - day['low']) * 0.5
                if today['close'] <= half_point:
                    return False
                break
        
        return True
    
    def _check_last_negative(self, data: pd.DataFrame) -> bool:
        """检查最后一根阴柱条件
        
        Returns:
            bool: 是否满足最后阴柱条件
        """
        if len(data) < 3:
            return False
            
        last_negative = data.iloc[-2]  # 最后一根阴柱（次阳线的前一天）
        prev_days = data.iloc[-7:-2]  # 前5天数据
        
        # 检查是否是阴线
        if not is_down(last_negative):
            return False
            
        # 检查是否缩量
        avg_volume = prev_days['volume'].mean()
        if last_negative['volume'] >= avg_volume:
            return False
            
        # # 检查是否踩到支撑点（使用近期低点作为支撑）
        # recent_low = prev_days['low'].min()
        # if abs(last_negative['low'] - recent_low) / recent_low > 0.02:  # 允许2%的误差
        #     return False
            
        return True
    
    def _check_decline_range(self, data: pd.DataFrame) -> bool:
        """检查下跌幅度要求
        
        Returns:
            bool: 是否满足下跌幅度要求
        """
        if len(data) < 30:  # 需要足够的历史数据
            return False
            
        # 计算最近高点
        high_price = data['high'].max()
        current_price = data.iloc[-1]['close']
        
        # 计算跌幅
        decline_percent = (high_price - current_price) / high_price * 100
        if decline_percent < 30:  # 至少下跌30%
            return False
            
        # 找到高量柱的位置
        volume_mean = data['volume'].mean()
        high_volume_days = data[data['volume'] > volume_mean * 2]  # 2倍均量定义为高量
        if high_volume_days.empty:
            return False
            
        # 获取最近的高量柱
        recent_high_volume = high_volume_days.iloc[-1]
        half_point = recent_high_volume['low'] + (recent_high_volume['high'] - recent_high_volume['low']) * 0.5
        
        # 检查是否破位
        if current_price < half_point:
            return False
            
        return True
    
    def apply(self, data: pd.DataFrame) -> bool:
        """应用极阴次阳策略
        
        Args:
            data: 股票数据
        
        Returns:
            bool: 是否满足策略条件
        """
        if len(data) < 30:  # 需要足够的历史数据
            return False
            
        # 检查各个条件
        if not self._check_continuous_decline(data):
            return False
            
        if not self._check_second_positive(data):
            return False
            
        if not self._check_last_negative(data):
            return False
            
        if not self._check_decline_range(data):
            return False
            
        return True
