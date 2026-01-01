# app.py
import os
import datetime
import threading
from flask import Flask, render_template
from flask_cors import CORS
from dotenv import load_dotenv
from utils.logger import get_logger

load_dotenv()

# 获取日志实例
logger = get_logger('app')

def create_app():
    """应用工厂"""
    app = Flask(__name__)
    CORS(app)
    
    # 注册蓝图
    from api.portfolio_routes import portfolio_routes
    from api.monitor_routes import monitor_routes
    from api.admin_routes import admin_routes
    from api.tools_routes import tools_routes
    from api.xueqiu_routes import xueqiu_routes
    
    app.register_blueprint(portfolio_routes, url_prefix='/api/portfolio')
    app.register_blueprint(monitor_routes, url_prefix='/api/monitor')
    app.register_blueprint(admin_routes, url_prefix='/api/admin')
    app.register_blueprint(tools_routes, url_prefix='/api/tools')
    app.register_blueprint(xueqiu_routes, url_prefix='/api/xueqiu')
    
    # 页面路由
    @app.route('/')
    def index():
        return render_template('index.html')
    
    @app.route('/admin')
    def admin():
        return render_template('admin.html')
    
    @app.route('/monitor')
    def monitor():
        return render_template('monitor.html')
    
    @app.route('/tools')
    def tools():
        return render_template('tools.html')
    
    @app.route('/xueqiu')
    def xueqiu():
        return render_template('xueqiu.html')
    
    return app


def start_background_tasks(app):
    """启动后台任务"""
    if os.getenv('AUTO_UPDATE_KLINE', 'true').lower() != 'true':
        logger.warning("已禁用自动K线更新")
        return
    
    from services.kline_manager import KlineService
    
    def auto_update():
        with app.app_context():
            KlineService.auto_update_kline_data()
    
    t = threading.Thread(target=auto_update, daemon=True)
    t.start()
    logger.info("K线更新后台线程已启动")


if __name__ == '__main__':
    app = create_app()
    
    if os.environ.get('WERKZEUG_RUN_MAIN') == 'true':
        start_background_tasks(app)
    
    logger.info("Flask 启动中：http://localhost:5000")
    app.run(host='0.0.0.0', port=5000, debug=True, threaded=True)