from .base import Strategy
from .utils.indicators import volume_increase
import pandas as pd


class MA60BreakDownSellIndicator(Strategy):
    """跌破MA60卖点指标"""
    def __init__(self):
        super().__init__(
            name="MA60BreakDownSellIndicator",
            description="跌破MA60卖点指标：当股价从MA60上方跌破MA60时，产生卖出信号"
        )

    def apply(self, data: pd.DataFrame) -> bool:
        """
        应用跌破MA60卖点指标
        规则：
        1. 需要至少2天的数据
        2. 前一天收盘价在MA60之上
        3. 当前天收盘价在MA60之下
        4. 当前天成交量大于前一天成交量（增强信号强度，提高信号质量）
        """
        if len(data) < 2:  # 需要至少两天的数据
            return False

        today = data.iloc[-1]
        yesterday = data.iloc[-2]

        try:
            # 规则1：前一天收盘价在MA60之上
            rule1 = yesterday['close'] > yesterday['ma60']

            # 规则2：当前天收盘价在MA60之下
            rule2 = today['close'] < today['ma60']

            # 规则3：成交量放大（增强信号强度，过滤弱信号）
            rule3 = volume_increase(today, yesterday)

            # 必须满足所有规则：核心逻辑（规则1和2）+ 成交量确认（规则3）
            return rule1 and rule2 and rule3
        except Exception as e:
            print(f"Error applying MA60BreakDownSellIndicator to {today.get('symbol', 'unknown')}: {str(e)}")
            return False

