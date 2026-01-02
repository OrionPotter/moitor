# models/kline_data.py
from dataclasses import dataclass
from datetime import datetime


@dataclass
class KlineData:
    """K线数据实体"""
    id: int
    code: str
    date: str
    open: float
    close: float
    high: float
    low: float
    volume: int
    amount: float
    created_at: datetime
    updated_at: datetime

    def to_dict(self):
        """转换为字典"""
        return {
            'id': self.id,
            'code': self.code,
            'date': self.date,
            'open': self.open,
            'close': self.close,
            'high': self.high,
            'low': self.low,
            'volume': self.volume,
            'amount': self.amount,
            'created_at': self.created_at.strftime('%Y-%m-%d %H:%M:%S') if self.created_at else None,
            'updated_at': self.updated_at.strftime('%Y-%m-%d %H:%M:%S') if self.updated_at else None
        }