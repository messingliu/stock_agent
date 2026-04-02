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
    MADeathCrossSell,
    HighVolumeStalledSell,
    MACDDeathCrossSell,
)
from .buy_indicators import (
    MAGoldenCrossBuy,
    VolumeContractionBounceBuy,
    MACDGoldenCrossBuy,
)

# 导出所有可用的策略
AVAILABLE_STRATEGIES = {
    # ── 买点信号 ──────────────────────────────────────────────────────────
    'GoldenLineDoubleGreenWin': GoldenLineDoubleGreenWin,
    'GoldenLineDoubleGreenWinWithConfirmation': GoldenLineDoubleGreenWinWithConfirmation,
    'ExtremeNegativePositive': ExtremeNegativePositiveStrategy,
    'MAGoldenCross': MAGoldenCrossBuy,
    'VolumeContractionBounce': VolumeContractionBounceBuy,
    'MACDGoldenCross': MACDGoldenCrossBuy,
    # ── 卖点信号 ──────────────────────────────────────────────────────────
    'HighVolumeBreak': HighVolumeBreakStrategy,
    'MA60BreakDownSellIndicator': MA60BreakDownSellIndicator,
    'MADeathCross': MADeathCrossSell,
    'HighVolumeStalled': HighVolumeStalledSell,
    'MACDDeathCross': MACDDeathCrossSell,
}

DAYS_MAP = {
    # 买点
    'GoldenLineDoubleGreenWin': 3,
    'GoldenLineDoubleGreenWinWithConfirmation': 3,
    'ExtremeNegativePositive': 60,
    'MAGoldenCross': 30,
    'VolumeContractionBounce': 20,
    'MACDGoldenCross': 60,
    # 卖点
    'HighVolumeBreak': 30,
    'MA60BreakDownSellIndicator': 60,
    'MADeathCross': 30,
    'HighVolumeStalled': 30,
    'MACDDeathCross': 60,
}

__all__ = [
    'Strategy',
    'StockData',
    # 买点
    'GoldenLineDoubleGreenWin',
    'GoldenLineDoubleGreenWinWithConfirmation',
    'ExtremeNegativePositiveStrategy',
    'MAGoldenCrossBuy',
    'VolumeContractionBounceBuy',
    'MACDGoldenCrossBuy',
    # 卖点
    'HighVolumeBreakStrategy',
    'MA60BreakDownSellIndicator',
    'MADeathCrossSell',
    'HighVolumeStalledSell',
    'MACDDeathCrossSell',
    'AVAILABLE_STRATEGIES',
]
