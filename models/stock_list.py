# models/stock_list.py
from datetime import datetime


class StockList:
    """股票代码模型"""

    def __init__(self, code, name, last_update=None, created_at=None, updated_at=None):
        self.code = code
        self.name = name
        self.last_update = last_update
        self.created_at = created_at
        self.updated_at = updated_at

    def to_dict(self):
        """转换为字典"""
        return {
            'code': self.code,
            'name': self.name,
            'last_update': self.last_update.isoformat() if self.last_update else None,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None
        }