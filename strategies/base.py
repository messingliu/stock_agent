from abc import ABC, abstractmethod
import pandas as pd
from typing import List, Dict, Any
from dataclasses import dataclass
from datetime import datetime

@dataclass
class StockData:
    """股票数据类"""
    symbol: str
    date: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int
    ma5: float
    ma10: float
    ma20: float
    ma60: float
    ma200: float

class Strategy(ABC):
    """策略基类"""
    def __init__(self, name: str, description: str):
        self.name = name
        self.description = description

    @abstractmethod
    def apply(self, data: pd.DataFrame) -> bool:
        """应用策略"""
        pass

    def __str__(self):
        return f"{self.name}: {self.description}"
