from .base import Strategy
from .utils.indicators import volume_increase, get_ma_trend, is_down, calc_macd
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


class MADeathCrossSell(Strategy):
    """均线死叉卖出策略"""

    def __init__(self):
        super().__init__(
            name="MADeathCrossSell",
            description=(
                "均线死叉卖出：MA5 从上方下穿 MA20，MA20 呈下降趋势，"
                "收盘价跌破 MA60，成交量放大确认，趋势反转信号"
            )
        )

    def apply(self, data: pd.DataFrame) -> bool:
        """
        规则：
        1. MA5 死叉 MA20：今日 MA5 < MA20，昨日 MA5 > MA20
        2. MA20 近5日趋势向下
        3. 收盘价跌破 MA60（空头结构成立）
        4. 今日成交量 > 昨日成交量（放量下跌，资金出逃确认）
        """
        if len(data) < 30:
            return False

        today = data.iloc[-1]
        yesterday = data.iloc[-2]

        try:
            # 规则1：MA5 死叉 MA20
            rule1 = (today['ma5'] < today['ma20']) and (yesterday['ma5'] > yesterday['ma20'])

            # 规则2：MA20 近5日趋势向下
            ma20_values = data['ma20'].tail(5)
            rule2 = ma20_values.iloc[-1] < ma20_values.iloc[0]

            # 规则3：收盘跌破 MA60
            rule3 = today['close'] < today['ma60']

            # 规则4：成交量放大
            rule4 = volume_increase(today, yesterday)

            return all([rule1, rule2, rule3, rule4])
        except Exception as e:
            print(f"Error applying MADeathCrossSell to {today.get('symbol', 'unknown')}: {str(e)}")
            return False


class HighVolumeStalledSell(Strategy):
    """高位放量滞涨卖出策略"""

    def __init__(self):
        super().__init__(
            name="HighVolumeStalledSell",
            description=(
                "高位放量滞涨：股价处于近30日高位区域，今日成交量明显放大（≥近5日均量1.5倍），"
                "但价格涨幅极小（<1%）或收阴，典型主力高位出货特征"
            )
        )

    def apply(self, data: pd.DataFrame) -> bool:
        """
        规则：
        1. 需要至少 30 天数据
        2. 今日收盘价处于近30日高位区间（≥ 近30日最高价的 88%）
        3. 今日成交量 ≥ 近5日均量的 1.5 倍（异常放量）
        4. 今日涨幅 < 1% 或收阴线（价格几乎不动 = 高位出货）
        5. 近5日 MA5 趋势走平或下行（动能衰减）
        """
        if len(data) < 30:
            return False

        today = data.iloc[-1]

        try:
            # 规则1：股价处于近30日高位（88% 阈值）
            recent_high = data.iloc[-30:]['high'].max()
            rule1 = recent_high > 0 and today['close'] >= recent_high * 0.88

            # 规则2：今日放量（≥近5日均量 * 1.5）
            avg_vol_5 = data.iloc[-6:-1]['volume'].mean()
            rule2 = avg_vol_5 > 0 and today['volume'] >= avg_vol_5 * 1.5

            # 规则3：今日涨幅极小（< 1%）或收阴
            change_pct = (today['close'] - today['open']) / today['open'] * 100
            rule3 = change_pct < 1.0

            # 规则4：MA5 走平或下行（5日内 MA5 末值 ≤ 初值）
            ma5_recent = data['ma5'].tail(5)
            rule4 = ma5_recent.iloc[-1] <= ma5_recent.iloc[0] * 1.005  # 允许 0.5% 误差

            return all([rule1, rule2, rule3, rule4])
        except Exception as e:
            print(f"Error applying HighVolumeStalledSell to {today.get('symbol', 'unknown')}: {str(e)}")
            return False


class MACDDeathCrossSell(Strategy):
    """MACD 顶部死叉卖出策略"""

    def __init__(self):
        super().__init__(
            name="MACDDeathCrossSell",
            description=(
                "MACD顶部死叉：DIF 从上方下穿 DEA 形成死叉，且死叉发生在 0 轴以上（高位），"
                "MACD 柱由正转负，动能由多转空，为中期见顶信号"
            )
        )

    def apply(self, data: pd.DataFrame) -> bool:
        """
        规则：
        1. 需要至少 60 天数据
        2. 昨日 DIF > DEA，今日 DIF < DEA（死叉）
        3. 死叉发生在 0 轴以上（DIF > 0），高位死叉杀伤力更强
        4. MACD 柱在缩小或由正转负（动能衰减）
        5. 今日收阴线，价格与指标共振确认
        """
        if len(data) < 60:
            return False

        today = data.iloc[-1]

        try:
            close = data['close']
            dif, dea, hist = calc_macd(close)

            today_dif = dif.iloc[-1]
            today_dea = dea.iloc[-1]
            yesterday_dif = dif.iloc[-2]
            yesterday_dea = dea.iloc[-2]

            # 规则1：DIF 死叉 DEA
            rule1 = (yesterday_dif > yesterday_dea) and (today_dif < today_dea)

            # 规则2：死叉在 0 轴以上（顶部特征）
            rule2 = today_dif > 0

            # 规则3：MACD 柱减小（动能衰减）
            rule3 = hist.iloc[-1] < hist.iloc[-2]

            # 规则4：今日收阴
            rule4 = is_down(today)

            return all([rule1, rule2, rule3, rule4])
        except Exception as e:
            print(f"Error applying MACDDeathCrossSell to {today.get('symbol', 'unknown')}: {str(e)}")
            return False

