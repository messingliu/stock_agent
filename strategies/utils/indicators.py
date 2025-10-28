import pandas as pd
from typing import Tuple

def calc_body(data: pd.Series) -> float:
    """计算K线实体"""
    return abs(data['close'] - data['open'])

def is_up(data: pd.Series) -> bool:
    """判断是否为阳线"""
    return data['close'] > data['open']

def is_down(data: pd.Series) -> bool:
    """判断是否为阴线"""
    return data['close'] < data['open']

def cross_ma(data: pd.Series, ma: str) -> bool:
    """判断是否突破均线"""
    return data['low'] < data[ma] and data['close'] > data[ma]

def volume_increase(today: pd.Series, yesterday: pd.Series) -> bool:
    """判断成交量是否放大"""
    return today['volume'] > yesterday['volume']

def price_increase(today: pd.Series, yesterday: pd.Series) -> bool:
    """判断价格是否上涨"""
    return today['close'] > yesterday['close']

def get_change_percent(data: pd.Series) -> float:
    """计算涨跌幅"""
    return (data['close'] - data['open']) / data['open'] * 100

def get_ma_trend(data: pd.DataFrame, ma: str, days: int = 5) -> bool:
    """判断均线趋势"""
    if len(data) < days:
        return False
    ma_values = data[ma].tail(days)
    return ma_values.iloc[-1] > ma_values.iloc[0]

def get_support_resistance(data: pd.DataFrame, period: int = 20) -> Tuple[float, float]:
    """计算支撑位和压力位"""
    if len(data) < period:
        return 0, 0
    
    recent_data = data.tail(period)
    support = recent_data['low'].min()
    resistance = recent_data['high'].max()
    
    return support, resistance
