# api/admin_routes.py
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional
from repositories.portfolio_repository import StockRepository
from repositories.monitor_repository import MonitorStockRepository
from datetime import datetime
from utils.logger import get_logger

logger = get_logger('admin_routes')

admin_router = APIRouter()


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


# ========== 股票管理 ==========

@admin_router.get('/stocks')
def list_stocks():
    """列出所有股票"""
    logger.info("GET /api/admin/stocks - 列出所有股票")
    stocks = StockRepository.get_all()
    result = {
        'status': 'success',
        'data': [s.to_dict() for s in stocks]
    }
    # 清理 NaN 值
    result = _clean_nan_values(result)
    logger.info(f"GET /api/admin/stocks - 返回成功，股票数量: {len(stocks)}")
    return result

class StockCreate(BaseModel):
    code: str
    name: str
    cost_price: float
    shares: int

class StockUpdate(BaseModel):
    name: str
    cost_price: float
    shares: int

@admin_router.post('/stocks')
def create_stock(data: StockCreate):
    """创建股票"""
    success, msg = StockRepository.add(
        data.code, data.name, data.cost_price, data.shares
    )
    return {'status': 'success' if success else 'error', 'message': msg}

@admin_router.put('/stocks/{code}')
def update_stock(code: str, data: StockUpdate):
    """更新股票"""
    success = StockRepository.update(
        code, data.name, data.cost_price, data.shares
    )
    return {'status': 'success' if success else 'error', 'message':  '更新成功' if success else '更新失败'}

@admin_router.delete('/stocks/{code}')
def delete_stock(code: str):
    """删除股票"""
    success = StockRepository.delete(code)
    return {'status': 'success' if success else 'error', 'message':  '删除成功' if success else '删除失败'}

# ========== 监控股票管理 ==========

@admin_router.get('/monitor-stocks')
def list_monitor_stocks():
    """列出所有监控股票"""
    logger.info("GET /api/admin/monitor-stocks - 列出所有监控股票")
    stocks = MonitorStockRepository.get_all()
    result = {
        'status': 'success',
        'data': [s.to_dict() for s in stocks]
    }
    # 清理 NaN 值
    result = _clean_nan_values(result)
    logger.info(f"GET /api/admin/monitor-stocks - 返回成功，监控股票数量: {len(stocks)}")
    return result

class MonitorStockCreate(BaseModel):
    code: str
    name: str
    timeframe: str
    reasonable_pe_min: float = 15
    reasonable_pe_max: float = 20

class MonitorStockUpdate(BaseModel):
    name: str
    timeframe: str
    reasonable_pe_min: float = 15
    reasonable_pe_max: float = 20

class ToggleEnabled(BaseModel):
    enabled: bool = True

@admin_router.post('/monitor-stocks')
def create_monitor_stock(data: MonitorStockCreate):
    """创建监控股票"""
    success, msg = MonitorStockRepository.add(
        data.code, data.name, data.timeframe,
        data.reasonable_pe_min, data.reasonable_pe_max
    )
    return {'status': 'success' if success else 'error', 'message': msg}

@admin_router.put('/monitor-stocks/{code}')
def update_monitor_stock(code: str, data: MonitorStockUpdate):
    """更新监控股票"""
    success = MonitorStockRepository.update(
        code, data.name, data.timeframe,
        data.reasonable_pe_min, data.reasonable_pe_max
    )
    return {
        'status': 'success' if success else 'error', 
        'message': '更新成功' if success else '更新失败'
    }

@admin_router.delete('/monitor-stocks/{code}')
def delete_monitor_stock(code: str):
    """删除监控股票"""
    success = MonitorStockRepository.delete(code)
    return {
        'status': 'success' if success else 'error',
        'message': '删除成功' if success else '删除失败'
    }

@admin_router.post('/monitor-stocks/{code}/toggle')
def toggle_monitor_stock(code: str, data: ToggleEnabled):
    """启用/禁用监控股票"""
    success = MonitorStockRepository.toggle_enabled(code, data.enabled)
    return {
        'status': 'success' if success else 'error',
        'message': '操作成功' if success else '操作失败'
    }

# ========== 雪球组合管理 ==========

@admin_router.get('/xueqiu-cubes')
def list_xueqiu_cubes():
    """列出所有雪球组合"""
    from repositories.xueqiu_repository import XueqiuCubeRepository
    cubes = XueqiuCubeRepository.get_all()
    result = {
        'status': 'success',
        'data': [cube.to_dict() for cube in cubes]
    }
    # 清理 NaN 值
    result = _clean_nan_values(result)
    return result

class XueqiuCubeCreate(BaseModel):
    cube_symbol: str
    cube_name: str
    enabled: bool = True

class XueqiuCubeUpdate(BaseModel):
    cube_name: str
    enabled: bool = True

@admin_router.post('/xueqiu-cubes')
def create_xueqiu_cube(data: XueqiuCubeCreate):
    """创建雪球组合"""
    from repositories.xueqiu_repository import XueqiuCubeRepository
    success, msg = XueqiuCubeRepository.add(
        data.cube_symbol, data.cube_name, data.enabled
    )
    return {'status': 'success' if success else 'error', 'message': msg}

@admin_router.put('/xueqiu-cubes/{cube_symbol}')
def update_xueqiu_cube(cube_symbol: str, data: XueqiuCubeUpdate):
    """更新雪球组合"""
    from repositories.xueqiu_repository import XueqiuCubeRepository
    success = XueqiuCubeRepository.update(
        cube_symbol, data.cube_name, data.enabled
    )
    return {
        'status': 'success' if success else 'error',
        'message': '更新成功' if success else '更新失败'
    }

@admin_router.delete('/xueqiu-cubes/{cube_symbol}')
def delete_xueqiu_cube(cube_symbol: str):
    """删除雪球组合"""
    from repositories.xueqiu_repository import XueqiuCubeRepository
    success = XueqiuCubeRepository.delete(cube_symbol)
    return {
        'status': 'success' if success else 'error',
        'message': '删除成功' if success else '删除失败'
    }

@admin_router.post('/xueqiu-cubes/{cube_symbol}/toggle')
def toggle_xueqiu_cube(cube_symbol: str, data: ToggleEnabled):
    """启用/禁用雪球组合"""
    from repositories.xueqiu_repository import XueqiuCubeRepository
    success = XueqiuCubeRepository.toggle_enabled(cube_symbol, data.enabled)
    return {
        'status': 'success' if success else 'error',
        'message': '操作成功' if success else '操作失败'
    }