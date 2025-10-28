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

# 导出所有可用的策略
AVAILABLE_STRATEGIES = {
    'GoldenLineDoubleGreenWin': GoldenLineDoubleGreenWin,
    'GoldenLineDoubleGreenWinWithConfirmation': GoldenLineDoubleGreenWinWithConfirmation,
    'HighVolumeBreak': HighVolumeBreakStrategy,
    'ExtremeNegativePositive': ExtremeNegativePositiveStrategy,
}

DAYS_MAP = {
    'GoldenLineDoubleGreenWin': 3,
    'GoldenLineDoubleGreenWinWithConfirmation': 3,
    'HighVolumeBreak': 30,
    'ExtremeNegativePositive': 60,  # 需要更多历史数据来判断下跌幅度
}

__all__ = [
    'Strategy',
    'StockData',
    'GoldenLineDoubleGreenWin',
    'GoldenLineDoubleGreenWinWithConfirmation',
    'HighVolumeBreakStrategy',
    'ExtremeNegativePositiveStrategy',
    'AVAILABLE_STRATEGIES'
]
