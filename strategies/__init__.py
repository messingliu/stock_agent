from .base import Strategy, StockData
from .golden_line import (
    GoldenLineDoubleGreenWin,
    GoldenLineDoubleGreenWinWithConfirmation
)

# 导出所有可用的策略
AVAILABLE_STRATEGIES = {
    'GoldenLineDoubleGreenWin': GoldenLineDoubleGreenWin,
    'GoldenLineDoubleGreenWinWithConfirmation': GoldenLineDoubleGreenWinWithConfirmation
}

__all__ = [
    'Strategy',
    'StockData',
    'GoldenLineDoubleGreenWin',
    'GoldenLineDoubleGreenWinWithConfirmation',
    'AVAILABLE_STRATEGIES'
]
