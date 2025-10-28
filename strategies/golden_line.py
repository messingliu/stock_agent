from .base import Strategy
from .utils.indicators import (
    is_down, is_up, cross_ma, volume_increase,
    calc_body, get_change_percent
)
import pandas as pd

def calc_goldenline_double_green_win(today: pd.Series, yesterday: pd.Series) -> bool:
    """
    计算双阳夹MA60策略
    """
    try:
        # 规则1：今天收阳，昨天收阴
        rule1 = is_up(today) and is_down(yesterday)

        # 规则2：今天实体大于昨天实体
        today_body = calc_body(today)
        yesterday_body = calc_body(yesterday)
        rule2 = today_body > yesterday_body

        # 规则3：今天最低价低于MA60，收盘价高于MA60
        rule3 = cross_ma(today, 'ma60')

        # 规则4：今天成交量大于昨天
        rule4 = volume_increase(today, yesterday)

        # 规则5：今天收盘价高于昨天开盘价
        rule5 = today['close'] > yesterday['open']

        return all([rule1, rule2, rule3, rule4, rule5])
    except Exception as e:
        print(f"Error applying calc_goldenline_double_green_win to {today['symbol']}: {str(e)}")
        return False

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

        # 检查前两天是否满足双阳夹MA60
        if not calc_goldenline_double_green_win(yesterday, third):
            return False

        # 第三天确认：开盘价或收盘价高于前一天
        return today['open'] > yesterday['close'] or today['close'] > yesterday['close']
