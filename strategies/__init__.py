from .base import Strategy, StockData
from .golden_line import (
    GoldenLineDoubleGreenWin,
    GoldenLineDoubleGreenWinWithConfirmation
)
from .volume_break import (
    HighVolumeBreakStrategy,
)
from .extreme_negative_positive import (
    ExtremeNegativePositiveStrategy,
)
from .sell_indicators import (
    MA60BreakDownSellIndicator,
)

# 导出所有可用的策略
AVAILABLE_STRATEGIES = {
    'GoldenLineDoubleGreenWin': GoldenLineDoubleGreenWin,
    'GoldenLineDoubleGreenWinWithConfirmation': GoldenLineDoubleGreenWinWithConfirmation,
    'HighVolumeBreak': HighVolumeBreakStrategy,
    'ExtremeNegativePositive': ExtremeNegativePositiveStrategy,
    'MA60BreakDownSellIndicator': MA60BreakDownSellIndicator,
}

DAYS_MAP = {
    'GoldenLineDoubleGreenWin': 3,
    'GoldenLineDoubleGreenWinWithConfirmation': 3,
    'HighVolumeBreak': 30,
    'ExtremeNegativePositive': 60,  # 需要更多历史数据来判断下跌幅度
    'MA60BreakDownSellIndicator': 60,  # 需要足够的历史数据来计算MA60
}

__all__ = [
    'Strategy',
    'StockData',
    'GoldenLineDoubleGreenWin',
    'GoldenLineDoubleGreenWinWithConfirmation',
    'HighVolumeBreakStrategy',
    'ExtremeNegativePositiveStrategy',
    'MA60BreakDownSellIndicator',
    'AVAILABLE_STRATEGIES'
]
