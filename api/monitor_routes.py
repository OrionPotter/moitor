# api/monitor_routes.py
from flask import Blueprint, request, jsonify
from services.monitor_service import MonitorService
from datetime import datetime
import threading
import time

monitor_routes = Blueprint('monitor', __name__)

# 内存缓存
_monitor_cache = {
    'data': None,
    'timestamp': None,
    'lock': threading.Lock()
}
_CACHE_TTL = 60  # 缓存有效期60秒


@monitor_routes.route('', methods=['GET'])
def get_monitor():
    """获取监控数据"""
    try:
        current_time = time.time()
        
        # 检查缓存是否有效
        with _monitor_cache['lock']:
            if (_monitor_cache['data'] is not None and 
                _monitor_cache['timestamp'] is not None and 
                current_time - _monitor_cache['timestamp'] < _CACHE_TTL):
                return jsonify(_monitor_cache['data'])
        
        # 缓存过期或不存在，重新获取数据
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
        
        # 更新缓存
        with _monitor_cache['lock']:
            _monitor_cache['data'] = result
            _monitor_cache['timestamp'] = current_time
        
        return jsonify(result)
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500


@monitor_routes.route('/stocks', methods=['GET'])
def list_monitor_stocks():
    """列表监控股票配置"""
    try:
        stocks = MonitorService.get_all_monitor_stocks()
        return jsonify({'status': 'success', 'data': stocks})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500


@monitor_routes.route('/stocks', methods=['POST'])
def create_monitor_stock():
    """创建监控股票"""
    data = request.get_json()
    success, msg = MonitorService.create_monitor_stock(
        data.get('code'), data.get('name'), data.get('timeframe'),
        float(data.get('reasonable_pe_min', 15)), float(data.get('reasonable_pe_max', 20))
    )
    return jsonify({'status':  'success' if success else 'error', 'message': msg})


@monitor_routes.route('/stocks/<code>', methods=['PUT'])
def update_monitor_stock(code):
    """更新监控股票"""
    data = request.get_json()
    success, msg = MonitorService.update_monitor_stock(
        code, data.get('name'), data.get('timeframe'),
        float(data.get('reasonable_pe_min')), float(data.get('reasonable_pe_max'))
    )
    return jsonify({'status':  'success' if success else 'error', 'message': msg})


@monitor_routes.route('/stocks/<code>', methods=['DELETE'])
def delete_monitor_stock(code):
    """删除监控股票"""
    success, msg = MonitorService.delete_monitor_stock(code)
    return jsonify({'status': 'success' if success else 'error', 'message': msg})


@monitor_routes.route('/stocks/<code>/toggle', methods=['POST'])
def toggle_monitor_stock(code):
    """启用/禁用监控股票"""
    data = request.get_json()
    success, msg = MonitorService.toggle_monitor_stock(code, data.get('enabled', True))
    return jsonify({'status': 'success' if success else 'error', 'message': msg})


@monitor_routes.route('/update-kline', methods=['POST'])
def update_kline():
    """手动更新K线数据"""
    try:
        from services.kline_service import KlineService
        force = request.get_json().get('force_update', False)
        
        def task():
            KlineService.batch_update_kline(force_update=force, max_workers=3)
        
        threading.Thread(target=task, daemon=True).start()
        return jsonify({'status': 'success', 'message': 'K线更新任务已启动'})
    except Exception as e: 
        return jsonify({'status': 'error', 'message': str(e)}), 500