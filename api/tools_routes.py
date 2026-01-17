from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from io import BytesIO
from datetime import datetime
import pandas as pd
from utils.logger import get_logger

logger = get_logger('tools_routes')

tools_router = APIRouter()


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


class Position(BaseModel):
    price: float
    shares: int


class CalculateCostRequest(BaseModel):
    positions: list[Position]


@tools_router.post('/calculate-cost')
def calculate_cost(data: CalculateCostRequest):
    try:
        positions = data.positions
        
        if not positions:
            raise HTTPException(status_code=400, detail='请提供买入记录')
        
        total_shares = 0
        total_cost = 0
        
        for pos in positions:
            price = pos.price
            shares = pos.shares
            
            if price <= 0 or shares <= 0:
                raise HTTPException(status_code=400, detail='价格和股数必须大于0')
            
            total_shares += shares
            total_cost += price * shares
        
        if total_shares == 0:
            raise HTTPException(status_code=400, detail='总持仓数不能为0')
        
        avg_cost = round(total_cost / total_shares, 2)
        
        return {
            'status': 'success',
            'data': {
                'total_shares': total_shares,
                'average_cost': avg_cost,
                'total_cost': round(total_cost, 2)
            }
        }
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@tools_router.get('/export-kline/stocks')
def get_export_stocks():
    """获取可导出K线数据的股票列表"""
    logger.info("GET /api/tools/export-kline/stocks - 请求开始")
    try:
        from repositories.monitor_repository import MonitorStockRepository
        from repositories.kline_repository import KlineRepository

        stocks = MonitorStockRepository.get_enabled()
        result = []

        for stock in stocks:
            code = stock.code
            name = stock.name
            latest_date = KlineRepository.get_latest_date(code)

            result.append({
                'code': code,
                'name': name,
                'latest_date': latest_date
            })

        response = {
            'status': 'success',
            'data': result
        }
        # 清理 NaN 值
        response = _clean_nan_values(response)
        logger.info(f"GET /api/tools/export-kline/stocks - 返回成功，股票数量: {len(result)}")
        return response

    except Exception as e:
        logger.error(f"GET /api/tools/export-kline/stocks - 请求失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


class ExportKlineRequest(BaseModel):
    code: str
    format: str = 'csv'
    start_date: str = None
    end_date: str = None


@tools_router.post('/export-kline')
def export_kline(data: ExportKlineRequest):
    """导出K线数据"""
    try:
        code = data.code
        format_type = data.format
        start_date = data.start_date
        end_date = data.end_date
        
        if not code:
            raise HTTPException(status_code=400, detail='请选择股票')
        
        if format_type not in ['csv', 'excel']:
            raise HTTPException(status_code=400, detail='不支持的导出格式')

        from repositories.kline_repository import KlineRepository
        from repositories.monitor_repository import MonitorStockRepository

        # 获取股票名称
        stock = MonitorStockRepository.get_by_code(code)
        stock_name = stock.name if stock else code

        # 获取K线数据
        df = KlineRepository.export_kline_data(code, start_date, end_date)
        
        if df is None or df.empty:
            raise HTTPException(status_code=400, detail='没有可导出的数据')
        
        # 生成文件名
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        if format_type == 'csv':
            filename = f'{stock_name}_{code}_K线_{timestamp}.csv'
            
            # 创建CSV文件
            output = BytesIO()
            df.to_csv(output, index=False, encoding='utf-8-sig')
            output.seek(0)
            
            return StreamingResponse(
                output,
                media_type='text/csv',
                headers={'Content-Disposition': f'attachment; filename="{filename}"'}
            )
        
        else:  # excel
            filename = f'{stock_name}_{code}_K线_{timestamp}.xlsx'
            
            # 创建Excel文件
            output = BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                df.to_excel(writer, index=False, sheet_name='K线数据')
            output.seek(0)
            
            return StreamingResponse(
                output,
                media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                headers={'Content-Disposition': f'attachment; filename="{filename}"'}
            )
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
