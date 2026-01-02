# Gunicorn 生产环境配置
import multiprocessing

# 服务器socket
bind = "0.0.0.0:5000"
backlog = 2048

# Worker进程数（建议CPU核心数 * 2 + 1）
workers = multiprocessing.cpu_count() * 2 + 1

# Worker类型（gevent适合IO密集型应用）
worker_class = "gevent"
worker_connections = 1000

# 每个worker的线程数
threads = 2

# 超时设置
timeout = 30
keepalive = 2

# 日志配置
accesslog = "-"
errorlog = "-"
loglevel = "info"

# 进程命名
proc_name = "tidewatch"

# 最大请求数后重启worker（防止内存泄漏）
max_requests = 1000
max_requests_jitter = 50

# 优雅重启
preload_app = True

# 环境变量
raw_env = [
    'FLASK_ENV=production',
    'PYTHONUNBUFFERED=1'
]

# 启动命令示例:
# gunicorn -c gunicorn_config.py run_production:app