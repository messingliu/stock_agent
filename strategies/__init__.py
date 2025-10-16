from .base import Strategy, StockData
from .golden_line import (
    GoldenLineDoubleGreenWin,
    GoldenLineDoubleGreenWinWithConfirmation
)
from .volume_break import (
    HighVolumeBreakStrategy,
)

# 导出所有可用的策略
AVAILABLE_STRATEGIES = {
    'GoldenLineDoubleGreenWin': GoldenLineDoubleGreenWin,
    'GoldenLineDoubleGreenWinWithConfirmation': GoldenLineDoubleGreenWinWithConfirmation,
    'HighVolumeBreak': HighVolumeBreakStrategy,
}

DAYS_MAP = {
    'GoldenLineDoubleGreenWin': 3,
    'GoldenLineDoubleGreenWinWithConfirmation': 3,
    'HighVolumeBreak': 100,
}

__all__ = [
    'Strategy',
    'StockData',
    'GoldenLineDoubleGreenWin',
    'GoldenLineDoubleGreenWinWithConfirmation',
    'HighVolumeBreakStrategy',
    'AVAILABLE_STRATEGIES'
]
