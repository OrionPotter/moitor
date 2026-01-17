# api/portfolio_routes.py
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional
from repositories.portfolio_repository import StockRepository
from services.portfolio_service import PortfolioService
from datetime import datetime
from utils.logger import get_logger

logger = get_logger('portfolio_routes')

portfolio_router = APIRouter()


def _clean_nan_values(obj):
    """递归清理 NaN 值，将其转换为 None"""
    if isinstance(obj, float):
        if obj != obj:  # NaN 检查
            return None
        return obj
    elif isinstance(obj, dict):
        return {k: _clean_nan_values(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [_clean_nan_values(item) for item in obj]
    return obj


class StockCreate(BaseModel):
    code: str
    name: str
    cost_price: float
    shares: int


class StockUpdate(BaseModel):
    name: Optional[str] = None
    cost_price: Optional[float] = None
    shares: Optional[int] = None


@portfolio_router.get('')
async def get_portfolio():
    """获取投资组合数据"""
    logger.info("GET /api/portfolio - 请求开始")
    try:
        rows, summary = await PortfolioService.get_portfolio_data()
        result = {
            'status': 'success',
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'rows': rows,
            'summary':  summary
        }
        # 清理 NaN 值
        result = _clean_nan_values(result)
        logger.info(f"GET /api/portfolio - 返回成功，股票数量: {len(rows)}, 总市值: {summary.get('market_value', 0)}")
        return result
    except Exception as e:
        logger.error(f"GET /api/portfolio - 请求失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@portfolio_router.post('')
async def create_stock(data: StockCreate):
    """创建股票"""
    logger.info(f"POST /api/portfolio - 创建股票: {data.code} {data.name}, 成本价: {data.cost_price}, 持仓: {data.shares}")
    success, msg = await StockRepository.add(
        data.code,
        data.name,
        data.cost_price,
        data.shares
    )

    result = {
        'status':  'success' if success else 'error',
        'message': msg
    }
    logger.info(f"POST /api/portfolio - 创建股票结果: {result}")
    return result


@portfolio_router.put('/{code}')
async def update_stock(code: str, data: StockUpdate):
    """更新股票"""
    logger.info(f"PUT /api/portfolio/{code} - 更新股票: {data}")
    success = await StockRepository.update(
        code,
        data.name,
        data.cost_price,
        data.shares
    )

    result = {
        'status':  'success' if success else 'error',
        'message':  '更新成功' if success else '更新失败'
    }
    logger.info(f"PUT /api/portfolio/{code} - 更新股票结果: {result}")
    return result


@portfolio_router.delete('/{code}')
async def delete_stock(code: str):
    """删除股票"""
    logger.info(f"DELETE /api/portfolio/{code} - 删除股票")
    success = await StockRepository.delete(code)

    result = {
        'status': 'success' if success else 'error',
        'message': '删除成功' if success else '删除失败'
    }
    logger.info(f"DELETE /api/portfolio/{code} - 删除股票结果: {result}")
    return result