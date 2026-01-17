# api/monitor_routes.py
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional
from services.monitor_service import MonitorService
from datetime import datetime
import threading
import time
import json
from utils.logger import get_logger

logger = get_logger('monitor_routes')

monitor_router = APIRouter()

# 内存缓存
_monitor_cache = {
    'data': None,
    'timestamp': None,
    'lock': threading.Lock()
}
_CACHE_TTL = 60  # 缓存有效期60秒


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


@monitor_router.get('')
def get_monitor():
    """获取监控数据"""
    logger.info("GET /api/monitor - 请求开始")
    try:
        current_time = time.time()

        # 检查缓存是否有效
        with _monitor_cache['lock']:
            if (_monitor_cache['data'] is not None and
                _monitor_cache['timestamp'] is not None and
                current_time - _monitor_cache['timestamp'] < _CACHE_TTL):
                logger.info("GET /api/monitor - 返回缓存数据")
                return _monitor_cache['data']

        # 缓存过期或不存在，重新获取数据
        logger.info("GET /api/monitor - 缓存过期，重新获取数据")
        stocks = MonitorService.get_monitor_data()

        # 丰富数据
        for stock in stocks:
            min_price, max_price = MonitorService.calculate_reasonable_price(
                stock.get('eps_forecast'),
                stock.get('reasonable_pe_min'),
                stock.get('reasonable_pe_max')
            )
            stock['reasonable_price_min'] = min_price
            stock['reasonable_price_max'] = max_price
            stock['valuation_status'] = MonitorService.check_valuation_status(
                stock.get('current_price'),
                stock.get('eps_forecast'),
                stock.get('reasonable_pe_min'),
                stock.get('reasonable_pe_max')
            )
            stock['technical_status'] = MonitorService.check_technical_status(
                stock.get('current_price'),
                stock.get('ema144'),
                stock.get('ema188')
            )
            stock['trend'] = MonitorService.check_trend({
                'ema5': stock.get('ema5'),
                'ema10':  stock.get('ema10'),
                'ema20': stock.get('ema20'),
                'ema30': stock.get('ema30'),
                'ema60': stock.get('ema60'),
                'ema7': stock.get('ema7'),
                'ema21': stock.get('ema21'),
                'ema42':  stock.get('ema42'),
            }, stock.get('timeframe'))

        result = {
            'status': 'success',
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'stocks': stocks
        }

        # 清理 NaN 值
        result = _clean_nan_values(result)

        # 更新缓存
        with _monitor_cache['lock']:
            _monitor_cache['data'] = result
            _monitor_cache['timestamp'] = current_time

        logger.info(f"GET /api/monitor - 返回成功，股票数量: {len(stocks)}")
        return result
    except Exception as e:
        logger.error(f"GET /api/monitor - 请求失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@monitor_router.get('/stocks')
def list_monitor_stocks():
    """列表监控股票配置"""
    try:
        stocks = MonitorService.get_all_monitor_stocks()
        return {'status': 'success', 'data': stocks}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


class MonitorStockCreate(BaseModel):
    code: str
    name: str
    timeframe: str
    reasonable_pe_min: float = 15
    reasonable_pe_max: float = 20


class MonitorStockUpdate(BaseModel):
    name: Optional[str] = None
    timeframe: Optional[str] = None
    reasonable_pe_min: Optional[float] = None
    reasonable_pe_max: Optional[float] = None


class ToggleStock(BaseModel):
    enabled: bool = True


@monitor_router.post('/stocks')
def create_monitor_stock(data: MonitorStockCreate):
    """创建监控股票"""
    success, msg = MonitorService.create_monitor_stock(
        data.code, data.name, data.timeframe,
        data.reasonable_pe_min, data.reasonable_pe_max
    )
    return {'status':  'success' if success else 'error', 'message': msg}


@monitor_router.put('/stocks/{code}')
def update_monitor_stock(code: str, data: MonitorStockUpdate):
    """更新监控股票"""
    success, msg = MonitorService.update_monitor_stock(
        code, data.name, data.timeframe,
        data.reasonable_pe_min, data.reasonable_pe_max
    )
    return {'status':  'success' if success else 'error', 'message': msg}


@monitor_router.delete('/stocks/{code}')
def delete_monitor_stock(code: str):
    """删除监控股票"""
    success, msg = MonitorService.delete_monitor_stock(code)
    return {'status': 'success' if success else 'error', 'message': msg}


@monitor_router.post('/stocks/{code}/toggle')
def toggle_monitor_stock(code: str, data: ToggleStock):
    """启用/禁用监控股票"""
    success, msg = MonitorService.toggle_monitor_stock(code, data.enabled)
    return {'status': 'success' if success else 'error', 'message': msg}


class UpdateKline(BaseModel):
    force_update: bool = False


@monitor_router.post('/update-kline')
def update_kline(data: UpdateKline):
    """手动更新K线数据"""
    try:
        from services.kline_service import KlineService
        
        def task():
            KlineService.batch_update_kline(force_update=data.force_update, max_workers=3)
        
        threading.Thread(target=task, daemon=True).start()
        return {'status': 'success', 'message': 'K线更新任务已启动'}
    except Exception as e: 
        raise HTTPException(status_code=500, detail=str(e))