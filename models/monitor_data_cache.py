# models/monitor_data_cache.py
from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class MonitorDataCache:
    """监控数据缓存实体"""
    id: int
    code: str
    timeframe: str
    current_price: float
    ema144: Optional[float]
    ema188: Optional[float]
    ema5: Optional[float]
    ema10: Optional[float]
    ema20: Optional[float]
    ema30: Optional[float]
    ema60: Optional[float]
    ema7: Optional[float]
    ema21: Optional[float]
    ema42: Optional[float]
    eps_forecast: Optional[float]
    created_at: datetime

    def to_dict(self):
        """转换为字典"""
        return {
            'id': self.id,
            'code': self.code,
            'timeframe': self.timeframe,
            'current_price': self.current_price,
            'ema144': self.ema144,
            'ema188': self.ema188,
            'ema5': self.ema5,
            'ema10': self.ema10,
            'ema20': self.ema20,
            'ema30': self.ema30,
            'ema60': self.ema60,
            'ema7': self.ema7,
            'ema21': self.ema21,
            'ema42': self.ema42,
            'eps_forecast': self.eps_forecast,
            'created_at': self.created_at.strftime('%Y-%m-%d %H:%M:%S') if self.created_at else None
        }