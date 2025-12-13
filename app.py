# app.py
from flask import Flask, render_template, jsonify, request
from flask_cors import CORS
from data_fetcher import get_portfolio_data
from db import get_all_stocks, add_stock, update_stock, delete_stock, get_stock_by_code
import datetime

# 初始化 Flask 应用
app = Flask(__name__)
# 启用 CORS，解决开发时可能的跨域请求问题
CORS(app)

# 路由：首页
# 当访问根路径 '/' 时，渲染 'index.html' 模板
@app.route('/')
def index():
    return render_template('index.html')

# 路由：管理页面
@app.route('/admin')
def admin():
    return render_template('admin.html')

# 路由：监控页面
@app.route('/monitor')
def monitor():
    return render_template('monitor.html')

# 路由：API 数据接口
# 前端页面会通过 AJAX 请求这个地址来获取最新的 JSON 数据
@app.route('/api/portfolio')
def api_portfolio():
    # 调用核心模块进行计算
    rows, summary = get_portfolio_data()
    
    # 构建返回给前端的 JSON 数据包
    response_data = {
        'status': 'success',
        'timestamp': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'rows': rows,
        'summary': summary
    }
    return jsonify(response_data)

# 路由：管理API - 获取所有股票
@app.route('/api/stocks', methods=['GET'])
def api_stocks():
    stocks = get_all_stocks()
    # 转换为字典格式
    stocks_dict = []
    for stock in stocks:
        stock_dict = {
            'id': stock[0],
            'code': stock[1],
            'name': stock[2],
            'cost_price': stock[3],
            'shares': stock[4]
        }
        stocks_dict.append(stock_dict)
    
    return jsonify(stocks_dict)

# 路由：管理API - 添加股票
@app.route('/api/stocks', methods=['POST'])
def api_add_stock():
    data = request.get_json()
    code = data.get('code')
    name = data.get('name')
    cost_price = float(data.get('cost_price'))
    shares = int(data.get('shares'))
    
    success = add_stock(code, name, cost_price, shares)
    if success:
        return jsonify({'status': 'success', 'message': '股票添加成功'})
    else:
        return jsonify({'status': 'error', 'message': '股票代码已存在'})

# 路由：管理API - 更新股票
@app.route('/api/stocks/<code>', methods=['PUT'])
def api_update_stock(code):
    data = request.get_json()
    name = data.get('name')
    cost_price = float(data.get('cost_price'))
    shares = int(data.get('shares'))
    
    update_stock(code, name, cost_price, shares)
    return jsonify({'status': 'success', 'message': '股票更新成功'})

# 路由：管理API - 删除股票
@app.route('/api/stocks/<code>', methods=['DELETE'])
def api_delete_stock(code):
    success = delete_stock(code)
    if success:
        return jsonify({'status': 'success', 'message': '股票删除成功'})
    else:
        return jsonify({'status': 'error', 'message': '股票不存在'})

# ========== 监控股票API ==========

# 路由：监控API - 获取所有监控股票
@app.route('/api/monitor-stocks', methods=['GET'])
def api_monitor_stocks():
    from db import get_all_monitor_stocks
    stocks = get_all_monitor_stocks()
    # 转换为字典格式
    stocks_dict = []
    for stock in stocks:
        stock_dict = {
            'id': stock[0],
            'code': stock[1],
            'name': stock[2],
            'timeframe': stock[3],
            'reasonable_pe_min': stock[4] if len(stock) > 4 and stock[4] is not None else 15,
            'reasonable_pe_max': stock[5] if len(stock) > 5 and stock[5] is not None else 20,
            'enabled': bool(stock[6]),
            'created_at': stock[7],
            'updated_at': stock[8]
        }
        stocks_dict.append(stock_dict)
    
    return jsonify(stocks_dict)

# 路由：监控API - 添加监控股票
@app.route('/api/monitor-stocks', methods=['POST'])
def api_add_monitor_stock():
    data = request.get_json()
    code = data.get('code')
    name = data.get('name')
    timeframe = data.get('timeframe')
    reasonable_pe_min = data.get('reasonable_pe_min', 15)
    reasonable_pe_max = data.get('reasonable_pe_max', 20)
    
    from db import add_monitor_stock
    success = add_monitor_stock(code, name, timeframe, reasonable_pe_min, reasonable_pe_max)
    if success:
        return jsonify({'status': 'success', 'message': '监控股票添加成功'})
    else:
        return jsonify({'status': 'error', 'message': '监控股票代码已存在'})

# 路由：监控API - 更新监控股票
@app.route('/api/monitor-stocks/<code>', methods=['PUT'])
def api_update_monitor_stock(code):
    data = request.get_json()
    name = data.get('name')
    timeframe = data.get('timeframe')
    reasonable_pe_min = data.get('reasonable_pe_min')
    reasonable_pe_max = data.get('reasonable_pe_max')
    enabled = data.get('enabled')
    
    from db import update_monitor_stock
    success = update_monitor_stock(code, name, timeframe, reasonable_pe_min, reasonable_pe_max, enabled)
    if success:
        return jsonify({'status': 'success', 'message': '监控股票更新成功'})
    else:
        return jsonify({'status': 'error', 'message': '监控股票不存在'})

# 路由：监控API - 删除监控股票
@app.route('/api/monitor-stocks/<code>', methods=['DELETE'])
def api_delete_monitor_stock(code):
    from db import delete_monitor_stock
    success = delete_monitor_stock(code)
    if success:
        return jsonify({'status': 'success', 'message': '监控股票删除成功'})
    else:
        return jsonify({'status': 'error', 'message': '监控股票不存在'})

# 路由：监控API - 启用/禁用监控股票
@app.route('/api/monitor-stocks/<code>/toggle', methods=['POST'])
def api_toggle_monitor_stock(code):
    data = request.get_json()
    enabled = data.get('enabled', True)
    
    from db import toggle_monitor_stock
    success = toggle_monitor_stock(code, enabled)
    if success:
        status_text = '启用' if enabled else '禁用'
        return jsonify({'status': 'success', 'message': f'监控股票{status_text}成功'})
    else:
        return jsonify({'status': 'error', 'message': '监控股票不存在'})

# 路由：监控API - 获取EMA监控数据
@app.route('/api/monitor')
def api_monitor():
    from data_fetcher import get_monitor_data
    from db import get_monitor_stock_by_code
    stocks = get_monitor_data()
    
    # 为每只股票添加合理估值数据（EPS预测已在data_fetcher中异步获取）
    for stock in stocks:
        try:
            # 获取合理估值PE范围
            monitor_stock = get_monitor_stock_by_code(stock['code'])
            if monitor_stock:
                reasonable_pe_min = monitor_stock[4] if len(monitor_stock) > 4 and monitor_stock[4] is not None else 15
                reasonable_pe_max = monitor_stock[5] if len(monitor_stock) > 5 and monitor_stock[5] is not None else 20
            else:
                reasonable_pe_min = 15
                reasonable_pe_max = 20
            
            stock['reasonable_pe_min'] = reasonable_pe_min
            stock['reasonable_pe_max'] = reasonable_pe_max
            
            # 计算合理价格 = 合理估值PE最小值 * EPS预测
            eps_forecast = stock.get('eps_forecast')
            if eps_forecast is not None and reasonable_pe_min is not None:
                stock['reasonable_price'] = round(eps_forecast * reasonable_pe_min, 2)
            else:
                stock['reasonable_price'] = None
            
            print(f"{stock['name']} EPS:{eps_forecast}, PE范围:{reasonable_pe_min}-{reasonable_pe_max}, 合理价格:{stock['reasonable_price']}")
            
        except Exception as e:
            print(f"获取 {stock['code']} 合理估值数据失败: {e}")
            stock['reasonable_pe_min'] = 15
            stock['reasonable_pe_max'] = 20
            stock['reasonable_price'] = None
    
    response_data = {
        'status': 'success',
        'timestamp': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'stocks': stocks
    }
    return jsonify(response_data)

if __name__ == '__main__':
    # 启动服务，debug=True 模式下修改代码会自动重启，方便开发
    # host='0.0.0.0' 使局域网内其他设备也能访问
    print("Flask 服务器正在启动...")
    print("请在浏览器中访问: http://localhost:5000")
    app.run(host='0.0.0.0', port=5000, debug=True)