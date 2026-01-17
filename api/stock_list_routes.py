from fastapi import APIRouter, HTTPException
from services.stock_list_service import StockListService
from datetime import datetime
from utils.logger import get_logger

logger = get_logger('stock_list_routes')

stock_list_router = APIRouter()


@stock_list_router.get('')
async def get_stock_list():
    """获取所有股票代码"""
    logger.info("GET /api/stock-list - 请求开始")
    try:
        stocks = await StockListService.get_all_stocks_async()
        result = {
            'status': 'success',
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'count': len(stocks),
            'data': [stock.to_dict() for stock in stocks]
        }
        logger.info(f"GET /api/stock-list - 返回成功，股票数量: {len(stocks)}")
        return result
    except Exception as e:
        logger.error(f"GET /api/stock-list - 请求失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@stock_list_router.get('/count')
async def get_stock_count():
    """获取股票总数"""
    logger.info("GET /api/stock-list/count - 请求开始")
    try:
        count = await StockListService.get_stock_count_async()
        result = {
            'status': 'success',
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'count': count
        }
        logger.info(f"GET /api/stock-list/count - 返回成功，股票总数: {count}")
        return result
    except Exception as e:
        logger.error(f"GET /api/stock-list/count - 请求失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@stock_list_router.get('/{code}')
async def get_stock_by_code(code: str):
    """根据代码获取股票"""
    logger.info(f"GET /api/stock-list/{code} - 请求开始")
    try:
        stock = await StockListService.get_stock_by_code_async(code)
        if stock:
            result = {
                'status': 'success',
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'data': stock.to_dict()
            }
            logger.info(f"GET /api/stock-list/{code} - 返回成功")
            return result
        else:
            logger.warning(f"GET /api/stock-list/{code} - 股票不存在")
            raise HTTPException(status_code=404, detail="股票不存在")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"GET /api/stock-list/{code} - 请求失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@stock_list_router.get('/search/{keyword}')
async def search_stocks(keyword: str):
    """搜索股票"""
    logger.info(f"GET /api/stock-list/search/{keyword} - 请求开始")
    try:
        stocks = await StockListService.search_stocks_async(keyword)
        result = {
            'status': 'success',
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'count': len(stocks),
            'data': [stock.to_dict() for stock in stocks]
        }
        logger.info(f"GET /api/stock-list/search/{keyword} - 返回成功，匹配数量: {len(stocks)}")
        return result
    except Exception as e:
        logger.error(f"GET /api/stock-list/search/{keyword} - 请求失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@stock_list_router.post('/update')
async def update_stock_list():
    """手动更新股票列表"""
    logger.info("POST /api/stock-list/update - 请求开始")
    try:
        success, message = await StockListService.update_stock_list_async()
        result = {
            'status': 'success' if success else 'error',
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'message': message
        }
        logger.info(f"POST /api/stock-list/update - 返回成功: {message}")
        return result
    except Exception as e:
        logger.error(f"POST /api/stock-list/update - 请求失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))