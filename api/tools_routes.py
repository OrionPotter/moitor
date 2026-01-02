from flask import Blueprint, request, jsonify, send_file
from io import BytesIO
from datetime import datetime
import pandas as pd

tools_routes = Blueprint('tools', __name__)


@tools_routes.route('/calculate-cost', methods=['POST'])
def calculate_cost():
    try:
        data = request.get_json()
        positions = data.get('positions', [])
        
        if not positions:
            return jsonify({'status': 'error', 'message': '请提供买入记录'}), 400
        
        total_shares = 0
        total_cost = 0
        
        for pos in positions:
            price = float(pos.get('price', 0))
            shares = int(pos.get('shares', 0))
            
            if price <= 0 or shares <= 0:
                return jsonify({'status': 'error', 'message': '价格和股数必须大于0'}), 400
            
            total_shares += shares
            total_cost += price * shares
        
        if total_shares == 0:
            return jsonify({'status': 'error', 'message': '总持仓数不能为0'}), 400
        
        avg_cost = round(total_cost / total_shares, 2)
        
        return jsonify({
            'status': 'success',
            'data': {
                'total_shares': total_shares,
                'average_cost': avg_cost,
                'total_cost': round(total_cost, 2)
            }
        })
    
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500


@tools_routes.route('/export-kline/stocks', methods=['GET'])
def get_export_stocks():
    """获取可导出K线数据的股票列表"""
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

        return jsonify({
            'status': 'success',
            'data': result
        })

    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500


@tools_routes.route('/export-kline', methods=['POST'])
def export_kline():
    """导出K线数据"""
    try:
        data = request.get_json()
        code = data.get('code')
        format_type = data.get('format', 'csv')  # csv 或 excel
        start_date = data.get('start_date')
        end_date = data.get('end_date')
        
        if not code:
            return jsonify({'status': 'error', 'message': '请选择股票'}), 400
        
        if format_type not in ['csv', 'excel']:
            return jsonify({'status': 'error', 'message': '不支持的导出格式'}), 400

        from repositories.kline_repository import KlineRepository
        from repositories.monitor_repository import MonitorStockRepository

        # 获取股票名称
        stock = MonitorStockRepository.get_by_code(code)
        stock_name = stock.name if stock else code

        # 获取K线数据
        df = KlineRepository.export_kline_data(code, start_date, end_date)
        
        if df is None or df.empty:
            return jsonify({'status': 'error', 'message': '没有可导出的数据'}), 400
        
        # 生成文件名
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        if format_type == 'csv':
            filename = f'{stock_name}_{code}_K线_{timestamp}.csv'
            
            # 创建CSV文件
            output = BytesIO()
            df.to_csv(output, index=False, encoding='utf-8-sig')
            output.seek(0)
            
            return send_file(
                output,
                mimetype='text/csv',
                as_attachment=True,
                download_name=filename
            )
        
        else:  # excel
            filename = f'{stock_name}_{code}_K线_{timestamp}.xlsx'
            
            # 创建Excel文件
            output = BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                df.to_excel(writer, index=False, sheet_name='K线数据')
            output.seek(0)
            
            return send_file(
                output,
                mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                as_attachment=True,
                download_name=filename
            )
    
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500
