from dataclasses import dataclass
from datetime import datetime


@dataclass
class XueqiuCube:
    """雪球组合实体"""
    id: int
    cube_symbol: str
    cube_name: str
    enabled: bool
    created_at: datetime
    updated_at: datetime
    
    def to_dict(self):
        """转换为字典"""
        return {
            'id': self.id,
            'cube_symbol': self.cube_symbol,
            'cube_name': self.cube_name,
            'enabled': self.enabled,
            'created_at': self.created_at,
            'updated_at': self.updated_at
        }