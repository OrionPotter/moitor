# Waitress 生产环境配置
# Windows友好的WSGI服务器

import multiprocessing

# 服务器socket
host = "0.0.0.0"
port = 5000

# 线程数（建议CPU核心数 * 2 + 1）
threads = multiprocessing.cpu_count() * 2 + 1

# URL前缀
url_prefix = ''

# 日志配置
log_format = '%(asctime)s %(message)s'
log_output = '-'

# 超时设置
socket_timeout = 60
send_bytes = 1073741824  # 1GB

# 启动命令示例:
# waitress-serve --port=5000 --threads=4 app:app