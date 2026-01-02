# services/monitor_service.py
from models.db import MonitorStockRepository
from services.data_service import DataService
from datetime import datetime
import os

os.environ.pop('http_proxy', None)
os.environ.pop('https_proxy', None)
os.environ.pop('all_proxy', None)


class MonitorService: 
    """监控业务逻辑"""
    
    @staticmethod
    def get_monitor_data():
        """获取监控数据"""
        return DataService.get_monitor_data()
    
    @staticmethod
    def get_all_monitor_stocks():
        """获取所有监控股票"""
        stocks = MonitorStockRepository.get_all()
        return [
            {
                'id': s[0],
                'code': s[1],
                'name': s[2],
                'timeframe': s[3],
                'reasonable_pe_min': s[4],
                'reasonable_pe_max': s[5],
                'enabled': bool(s[6])
            }
            for s in stocks
        ]
    
    @staticmethod
    def get_monitor_stock(code):
        """获取单个监控股票"""
        stock = MonitorStockRepository.get_by_code(code)
        if not stock:
            return None
        
        return {
            'id': stock[0],
            'code': stock[1],
            'name':  stock[2],
            'timeframe': stock[3],
            'reasonable_pe_min': stock[4],
            'reasonable_pe_max': stock[5],
            'enabled': bool(stock[6])
        }
    
    @staticmethod
    def create_monitor_stock(code, name, timeframe, pe_min=15, pe_max=20):
        """创建监控股票"""
        success, msg = MonitorStockRepository.add(code, name, timeframe, pe_min, pe_max)
        return success, msg
    
    @staticmethod
    def update_monitor_stock(code, name, timeframe, pe_min, pe_max):
        """更新监控股票"""
        success = MonitorStockRepository.update(code, name, timeframe, pe_min, pe_max)
        return success, "更新成功" if success else "更新失败"
    
    @staticmethod
    def delete_monitor_stock(code):
        """删除监控股票"""
        success = MonitorStockRepository.delete(code)
        return success, "删除成功" if success else "删除失败"
    
    @staticmethod
    def toggle_monitor_stock(code, enabled):
        """启用/禁用监控股票"""
        success = MonitorStockRepository.toggle_enabled(code, enabled)
        return success, "操作成功" if success else "操作失败"
    
    @staticmethod
    def calculate_reasonable_price(eps_forecast, pe_min, pe_max):
        """计算合理价格范围"""
        if not eps_forecast:
            return None, None
        
        return round(eps_forecast * pe_min, 2), round(eps_forecast * pe_max, 2)
    
    @staticmethod
    def check_valuation_status(current_price, eps_forecast, pe_min, pe_max):
        """检查估值状态
        
        Returns:
            str: '低估' / '正常' / '高估'
        """
        if not eps_forecast:
            return '未知'
        
        min_price = eps_forecast * pe_min
        max_price = eps_forecast * pe_max
        
        if current_price < min_price:
            return '低估'
        elif current_price > max_price:
            return '高估'
        else:
            return '正常'
    
    @staticmethod
    def check_technical_status(current_price, ema144, ema188):
        """检查技术面状态
        
        Returns: 
            str: '加仓' / '破位' / '无信号'
        """
        if not ema144 or not ema188:
            return '无信号'
        
        min_ema = min(ema144, ema188)
        max_ema = max(ema144, ema188)
        
        if current_price < min_ema:
            return '破位'
        elif min_ema <= current_price <= max_ema:
            return '加仓'
        else:
            return '无信号'
    
    @staticmethod
    def check_trend(ema_dict, timeframe):
        """检查趋势
        
        Returns: 
            str: '多头' / '空头' / '震荡'
        """
        if timeframe == '1d':
            ema5, ema10, ema20 = ema_dict.get('ema5'), ema_dict.get('ema10'), ema_dict.get('ema20')
            if ema5 and ema10 and ema20:
                if ema5 > ema10 > ema20:
                    return '多头'
                elif ema5 < ema10 < ema20:
                    return '空头'
                else:
                    return '震荡'
        
        elif timeframe == '2d':
            ema10, ema30, ema60 = ema_dict.get('ema10'), ema_dict.get('ema30'), ema_dict.get('ema60')
            if ema10 and ema30 and ema60:
                if ema10 > ema30 > ema60:
                    return '多头'
                elif ema10 < ema30 < ema60:
                    return '空头'
                else:
                    return '震荡'
        
        elif timeframe == '3d':
            ema7, ema21, ema42 = ema_dict.get('ema7'), ema_dict.get('ema21'), ema_dict.get('ema42')
            if ema7 and ema21 and ema42:
                if ema7 > ema21 > ema42:
                    return '多头'
                elif ema7 < ema21 < ema42:
                    return '空头'
                else:
                    return '震荡'
        
        return '未知'