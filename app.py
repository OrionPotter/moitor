# app.py
import os
import datetime
import threading
from flask import Flask, render_template
from flask_cors import CORS
from dotenv import load_dotenv

load_dotenv()

def create_app():
    """应用工厂"""
    app = Flask(__name__)
    CORS(app)
    
    # 注册蓝图
    from api.portfolio_routes import portfolio_routes
    from api.monitor_routes import monitor_routes
    from api.admin_routes import admin_routes
    
    app.register_blueprint(portfolio_routes, url_prefix='/api/portfolio')
    app.register_blueprint(monitor_routes, url_prefix='/api/monitor')
    app.register_blueprint(admin_routes, url_prefix='/api/admin')
    
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
    
    return app


def start_background_tasks(app):
    """启动后台任务"""
    if os.getenv('AUTO_UPDATE_KLINE', 'true').lower() != 'true':
        print("⚠️ 已禁用自动K线更新")
        return
    
    from services.kline_manager import KlineService
    
    def auto_update():
        with app.app_context():
            KlineService.auto_update_kline_data()
    
    t = threading.Thread(target=auto_update, daemon=True)
    t.start()
    print("🧵 K线更新后台线程已启动")


if __name__ == '__main__':
    # 初始化数据库
    from models.db import init_db, populate_initial_data
    init_db()
    populate_initial_data()
    
    app = create_app()
    
    if os.environ.get('WERKZEUG_RUN_MAIN') == 'true':
        start_background_tasks(app)
    
    print("🚀 Flask 启动中：http://localhost:5000")
    app.run(host='0.0.0.0', port=5000, debug=True)