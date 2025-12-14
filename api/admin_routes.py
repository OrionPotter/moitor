# api/admin_routes.py
from flask import Blueprint, request, jsonify
from models.db import StockRepository, MonitorStockRepository
from datetime import datetime

admin_routes = Blueprint('admin', __name__)

# ========== 股票管理 ==========

@admin_routes.route('/stocks', methods=['GET'])
def list_stocks():
    """列出所有股票"""
    stocks = StockRepository.get_all()
    return jsonify({
        'status': 'success',
        'data': [
            {'id': s[0], 'code': s[1], 'name': s[2], 'cost_price': s[3], 'shares': s[4]}
            for s in stocks
        ]
    })

@admin_routes.route('/stocks', methods=['POST'])
def create_stock():
    """创建股票"""
    data = request.get_json()
    success, msg = StockRepository.add(
        data['code'], data['name'], float(data['cost_price']), int(data['shares'])
    )
    return jsonify({'status': 'success' if success else 'error', 'message': msg})

@admin_routes.route('/stocks/<code>', methods=['PUT'])
def update_stock(code):
    """更新股票"""
    data = request.get_json()
    success = StockRepository.update(
        code, data['name'], float(data['cost_price']), int(data['shares'])
    )
    return jsonify({'status': 'success' if success else 'error', 'message':  '更新成功' if success else '更新失败'})

@admin_routes.route('/stocks/<code>', methods=['DELETE'])
def delete_stock(code):
    """删除股票"""
    success = StockRepository.delete(code)
    return jsonify({'status': 'success' if success else 'error', 'message':  '删除成功' if success else '删除失败'})

# ========== 监控股票管理 ==========

@admin_routes.route('/monitor-stocks', methods=['GET'])
def list_monitor_stocks():
    """列出所有监控股票"""
    stocks = MonitorStockRepository.get_all()
    return jsonify({
        'status': 'success',
        'data':  [
            {
                'id': s[0], 'code': s[1], 'name': s[2], 'timeframe': s[3],
                'reasonable_pe_min': s[4], 'reasonable_pe_max': s[5], 'enabled': bool(s[6])
            }
            for s in stocks
        ]
    })

@admin_routes.route('/monitor-stocks', methods=['POST'])
def create_monitor_stock():
    """创建监控股票"""
    data = request.get_json()
    success, msg = MonitorStockRepository.add(
        data['code'], data['name'], data['timeframe'],
        float(data.get('reasonable_pe_min', 15)), float(data.get('reasonable_pe_max', 20))
    )
    return jsonify({'status': 'success' if success else 'error', 'message': msg})

@admin_routes.route('/monitor-stocks/<code>', methods=['PUT'])
def update_monitor_stock(code):
    """更新监控股票"""
    data = request.get_json()
    success = MonitorStockRepository.update(
        code, data['name'], data['timeframe'],
        float(data.get('reasonable_pe_min', 15)), float(data.get('reasonable_pe_max', 20))
    )
    return jsonify({'status': 'success' if success else 'error'})

@admin_routes.route('/monitor-stocks/<code>', methods=['DELETE'])
def delete_monitor_stock(code):
    """删除监控股票"""
    success = MonitorStockRepository.delete(code)
    return jsonify({'status': 'success' if success else 'error'})

@admin_routes.route('/monitor-stocks/<code>/toggle', methods=['POST'])
def toggle_monitor_stock(code):
    """启用/禁用监控股票"""
    data = request.get_json()
    success = MonitorStockRepository.toggle_enabled(code, data.get('enabled', True))
    return jsonify({'status': 'success' if success else 'error'})