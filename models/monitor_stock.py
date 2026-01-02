# models/monitor_stock.py
from dataclasses import dataclass
from datetime import datetime


@dataclass
class MonitorStock:
    """监控股票实体"""
    id: int
    code: str
    name: str
    timeframe: str
    reasonable_pe_min: float
    reasonable_pe_max: float
    enabled: bool
    created_at: datetime
    updated_at: datetime

    def to_dict(self):
        """转换为字典"""
        return {
            'id': self.id,
            'code': self.code,
            'name': self.name,
            'timeframe': self.timeframe,
            'reasonable_pe_min': self.reasonable_pe_min,
            'reasonable_pe_max': self.reasonable_pe_max,
            'enabled': self.enabled,
            'created_at': self.created_at.strftime('%Y-%m-%d %H:%M:%S') if self.created_at else None,
            'updated_at': self.updated_at.strftime('%Y-%m-%d %H:%M:%S') if self.updated_at else None
        }