# api/portfolio_routes.py
from flask import Blueprint, request, jsonify
from models.db import StockRepository
from services.portfolio_service import PortfolioService
from datetime import datetime

portfolio_routes = Blueprint('portfolio', __name__)


@portfolio_routes.route('', methods=['GET'])
def get_portfolio():
    """获取投资组合数据"""
    try:
        rows, summary = PortfolioService.get_portfolio_data()
        return jsonify({
            'status': 'success',
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'rows': rows,
            'summary':  summary
        })
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500


@portfolio_routes.route('', methods=['POST'])
def create_stock():
    """创建股票"""
    data = request.get_json()
    success, msg = StockRepository.add(
        data.get('code'),
        data.get('name'),
                float(data.get('cost_price')),
                int(data.get('shares'))
    )
    
    return jsonify({
        'status':  'success' if success else 'error',
        'message': msg
    })


@portfolio_routes.route('/<code>', methods=['PUT'])
def update_stock(code):
    """更新股票"""
    data = request.get_json()
    success = StockRepository.update(
        code,
        data.get('name'),
        float(data. get('cost_price')),
        int(data.get('shares'))
    )
    
    return jsonify({
        'status':  'success' if success else 'error',
        'message':  '更新成功' if success else '更新失败'
    })


@portfolio_routes.route('/<code>', methods=['DELETE'])
def delete_stock(code):
    """删除股票"""
    success = StockRepository.delete(code)
    
    return jsonify({
        'status': 'success' if success else 'error',
        'message': '删除成功' if success else '删除失败'
    })