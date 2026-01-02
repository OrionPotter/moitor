# models/stock.py
from dataclasses import dataclass
from datetime import datetime


@dataclass
class Stock:
    """股票持仓实体"""
    id: int
    code: str
    name: str
    cost_price: float
    shares: int

    def to_dict(self):
        """转换为字典"""
        return {
            'id': self.id,
            'code': self.code,
            'name': self.name,
            'cost_price': self.cost_price,
            'shares': self.shares
        }