from .base import Strategy
from .utils.indicators import (
    is_up, volume_increase, get_ma_trend, calc_macd
)
import pandas as pd


class MAGoldenCrossBuy(Strategy):
    """均线金叉买入策略"""

    def __init__(self):
        super().__init__(
            name="MAGoldenCrossBuy",
            description=(
                "均线金叉买入：MA5 从下方上穿 MA20，MA20 近5日呈上升趋势，"
                "股价收盘在 MA60 之上（多头结构），今日成交量放大"
            )
        )

    def apply(self, data: pd.DataFrame) -> bool:
        """
        规则：
        1. MA5 金叉 MA20：今日 MA5 > MA20，昨日 MA5 < MA20
        2. MA20 近5日趋势向上（排除假突破）
        3. 收盘价在 MA60 之上（健康多头结构）
        4. 今日成交量 > 昨日成交量（量能配合）
        """
        if len(data) < 30:
            return False

        today = data.iloc[-1]
        yesterday = data.iloc[-2]

        try:
            rule1 = (today['ma5'] > today['ma20']) and (yesterday['ma5'] < yesterday['ma20'])
            rule2 = get_ma_trend(data, 'ma20', days=5)
            rule3 = today['close'] > today['ma60']
            rule4 = volume_increase(today, yesterday)
            return all([rule1, rule2, rule3, rule4])
        except Exception as e:
            print(f"Error applying MAGoldenCrossBuy to {today.get('symbol', 'unknown')}: {str(e)}")
            return False


class VolumeContractionBounceBuy(Strategy):
    """缩量回踩均线反弹买入策略"""

    def __init__(self):
        super().__init__(
            name="VolumeContractionBounceBuy",
            description=(
                "缩量回踩均线反弹：经过3日以上缩量回调，今日在 MA20/MA60 附近放量阳线反弹，"
                "是主力洗盘结束后常见的买入时机"
            )
        )

    def apply(self, data: pd.DataFrame) -> bool:
        """
        规则：
        1. 近3日（不含今日）成交量均低于近10日均量的 85%（缩量调整）
        2. 今日最低价在 MA20 或 MA60 附近（5% 以内）触及支撑
        3. 今日收阳线（close > open）
        4. 今日成交量 > 近3日均量 * 1.3（放量反弹）
        5. 今日收盘价高于 MA20 或 MA60（站回均线）
        """
        if len(data) < 20:
            return False

        today = data.iloc[-1]

        try:
            recent_3_vol = data.iloc[-4:-1]['volume'].mean()
            period_10_vol = data.iloc[-11:-1]['volume'].mean()

            # 规则1：近3日整体缩量（低于10日均量的85%）
            rule1 = period_10_vol > 0 and recent_3_vol < period_10_vol * 0.85

            # 规则2：今日触及 MA20 或 MA60 支撑
            ma20 = today['ma20']
            ma60 = today['ma60']
            near_ma20 = ma20 > 0 and abs(today['low'] - ma20) / ma20 <= 0.05
            near_ma60 = ma60 > 0 and abs(today['low'] - ma60) / ma60 <= 0.05
            rule2 = near_ma20 or near_ma60

            # 规则3：今日收阳
            rule3 = is_up(today)

            # 规则4：今日放量反弹
            rule4 = recent_3_vol > 0 and today['volume'] > recent_3_vol * 1.3

            # 规则5：收盘站上均线
            above_ma20 = ma20 > 0 and today['close'] > ma20
            above_ma60 = ma60 > 0 and today['close'] > ma60
            rule5 = above_ma20 or above_ma60

            return all([rule1, rule2, rule3, rule4, rule5])
        except Exception as e:
            print(f"Error applying VolumeContractionBounceBuy to {today.get('symbol', 'unknown')}: {str(e)}")
            return False


class MACDGoldenCrossBuy(Strategy):
    """MACD 底部金叉买入策略"""

    def __init__(self):
        super().__init__(
            name="MACDGoldenCrossBuy",
            description=(
                "MACD底部金叉：DIF 从下方上穿 DEA 形成金叉，且金叉位置在 0 轴以下（底部），"
                "MACD 柱由负转正，动能由空转多，为中期趋势反转信号"
            )
        )

    def apply(self, data: pd.DataFrame) -> bool:
        """
        规则：
        1. 需要至少 60 天数据，确保 EMA 充分收敛
        2. 昨日 DIF < DEA（死叉状态），今日 DIF > DEA（形成金叉）
        3. 金叉发生在 0 轴以下（DIF < 0），底部反转信号更强
        4. MACD 柱在增大（hist 较昨日上升，动能增强）
        5. 今日收阳线，价格与指标共振
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

            # 规则1：DIF 金叉 DEA
            rule1 = (yesterday_dif < yesterday_dea) and (today_dif > today_dea)

            # 规则2：金叉在 0 轴以下（底部特征）
            rule2 = today_dif < 0

            # 规则3：MACD 柱增大（动能由空转多）
            rule3 = hist.iloc[-1] > hist.iloc[-2]

            # 规则4：今日收阳
            rule4 = is_up(today)

            return all([rule1, rule2, rule3, rule4])
        except Exception as e:
            print(f"Error applying MACDGoldenCrossBuy to {today.get('symbol', 'unknown')}: {str(e)}")
            return False
