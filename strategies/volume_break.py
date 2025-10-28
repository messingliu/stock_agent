from .base import Strategy
from .utils.indicators import (
    volume_increase, get_change_percent,
    get_support_resistance, calc_body
)
import pandas as pd
import numpy as np

class HighVolumeBreakStrategy(Strategy):
    """高量破有灾祸策略"""
    def __init__(self):
        super().__init__(
            name="HighVolumeBreak",
            description="高量破有灾祸：找到前期放量大阳线，当前价格跌破该阳线最低价"
        )

    def _find_high_volume_rise(self, data: pd.DataFrame, lookback: int = 30) -> pd.Series:
        """
        寻找前期放量大阳线
        
        条件：
        1. 是阳线（收盘价高于开盘价）
        2. 成交量是前5天平均成交量的2倍以上
        3. 涨幅超过2%
        4. 实体够大（至少1%）
        """
        if len(data) < lookback:
            return None
            
        # 计算前期数据（不包括最近5天）
        historical_data = data.iloc[:]
        max_vol = 0
        max_day = None
        for idx in range(len(historical_data)-2, -1, -1):
            day = historical_data.iloc[idx]
            prev = historical_data.iloc[idx-1]
            prev_5_days = historical_data.iloc[idx-5:idx]
            
            # 检查是否是阳线
            if day['close'] <= prev['close']:
                continue
                
            if day['volume'] < max_vol:
                continue

            # 检查成交量
            avg_volume = prev_5_days['volume'].mean()
            if day['volume'] < avg_volume * 1.5:
                continue
                
            # 检查涨幅
            change_percent = (day['close'] - prev['close']) / prev['open'] * 100
            if change_percent < 2:
                continue
                
            # # 检查实体大小
            # body = calc_body(day)
            # if body / day['open'] * 100 < 1:
            #     continue
            max_vol = day['volume']
            max_day = day
        return max_day
 

    def _check_price_break(self, data: pd.DataFrame, volume_rise_day: pd.Series) -> bool:
        """
        检查当前价格是否跌破放量大阳线的最低价
        
        条件：
        1. 当前价格跌破放量日最低价
        2. 之前的价格维持在放量日最低价之上（震荡盘整）
        """
        if volume_rise_day is None:
            return False
        
        today = data.iloc[-1]
        # 放量日的最低价作为支撑位
        support_price = volume_rise_day['low']
        
        # 检查当前价格是否跌破支撑位
        break_support = today['close'] < support_price
        
        # 检查之前是否维持在支撑位之上（震荡盘整）
        maintained_support = True
        volume_rise_idx = data.index.get_loc(volume_rise_day.name)
                # 检查震荡期间的成交量
        consolidation_period = data.iloc[volume_rise_idx+1:-2]
        for _, day in consolidation_period.iterrows():
            if day['low'] < volume_rise_day['low']:
                maintained_support = False
                break
        
        return break_support and maintained_support

    def _check_volume_pattern(self, data: pd.DataFrame, volume_rise_idx: int) -> bool:
        """
        检查成交量形态
        
        条件：
        1. 跌破时的成交量要适中（不能过大也不能过小）
        2. 前期震荡时的成交量要小于放量日
        """
        if volume_rise_idx is None:
            return False
            
        volume_rise_day = data.iloc[volume_rise_idx]
        current_day = data.iloc[-1]
        
        # 跌破时的成交量应该是放量日的30%-80%之间
        volume_ratio = current_day['volume'] / volume_rise_day['volume']
        if volume_ratio < 0.3 or volume_ratio > 0.8:
            return False
            
        # 检查震荡期间的成交量
        consolidation_period = data.iloc[volume_rise_idx+1:-1]
        for _, day in consolidation_period.iterrows():
            if day['volume'] > volume_rise_day['volume']:
                return False
                
        return True

    def apply(self, data: pd.DataFrame) -> bool:
        """
        应用高量破有灾祸策略
        
        步骤：
        1. 找到前期放量大阳线
        2. 确认当前价格跌破该阳线最低价
        3. 确认中间经过了震荡盘整
        4. 检查成交量形态
        """
        if len(data) < 30:  # 需要至少30天的数据
            return False

        # 找到前期放量大阳线
        volume_rise_day = self._find_high_volume_rise(data)
        if volume_rise_day is None:
            return False
        # 检查当前价格是否跌破支撑位
        current_day = data.iloc[-1]
        if not self._check_price_break(data, volume_rise_day):
            return False
        print(f"Price break: {self._check_price_break(data, volume_rise_day)}, high volume rise day: {volume_rise_day['date']} for {data.iloc[-1]['symbol']}")
        # # 检查成交量形态
        # volume_rise_idx = data.index.get_loc(volume_rise_day.name)
        # if not self._check_volume_pattern(data, volume_rise_idx):
        #     return False
            
        return True