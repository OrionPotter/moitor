from fastapi import APIRouter, HTTPException
from services.xueqiu_service import XueqiuService
from datetime import datetime
import time
from utils.logger import get_logger

logger = get_logger('xueqiu_routes')

xueqiu_router = APIRouter()


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


@xueqiu_router.get('')
def get_xueqiu_data():
    """获取所有雪球组合的调仓数据"""
    start_time = time.time()
    logger.info("GET /api/xueqiu - 请求开始")
    try:
        # 调用同步方法，内部使用asyncio.run()运行异步代码
        all_data = XueqiuService.get_all_formatted_data()

        elapsed = time.time() - start_time
        result = {
            'status': 'success',
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'data': all_data
        }
        # 清理 NaN 值
        result = _clean_nan_values(result)
        logger.info(f"GET /api/xueqiu - 返回成功，组合数量: {len(all_data)}, 耗时: {elapsed:.2f}秒")
        return result
    except Exception as e:
        elapsed = time.time() - start_time
        logger.error(f"GET /api/xueqiu - 请求失败，耗时: {elapsed:.2f}秒，错误: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@xueqiu_router.get('/{cube_symbol}')
def get_cube_data(cube_symbol: str):
    """获取指定雪球组合的调仓数据"""
    logger.info(f"GET /api/xueqiu/{cube_symbol} - 请求开始")
    try:
        # 在同步函数中运行异步代码
        import asyncio
        async def fetch_single():
            headers = XueqiuService._get_headers()
            import aiohttp
            async with aiohttp.ClientSession(headers=headers, trust_env=False) as session:
                history = await XueqiuService._fetch_cube_data(session, cube_symbol)
            return history

        history = asyncio.run(fetch_single())

        if history is None:
            logger.warning(f"GET /api/xueqiu/{cube_symbol} - 获取数据失败")
            raise HTTPException(status_code=500, detail='获取数据失败')

        formatted = XueqiuService.format_rebalancing_data(cube_symbol, history)

        result = {
            'status': 'success',
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'cube_symbol': cube_symbol,
            'data': formatted
        }
        # 清理 NaN 值
        result = _clean_nan_values(result)
        logger.info(f"GET /api/xueqiu/{cube_symbol} - 返回成功，调仓记录数量: {len(formatted)}")
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"GET /api/xueqiu/{cube_symbol} - 请求失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))