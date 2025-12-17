from flask import Blueprint, request, jsonify

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
