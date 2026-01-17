# models/db.py
import os
from contextlib import contextmanager
from utils.logger import get_logger
import psycopg2
from psycopg2.extras import RealDictCursor

# 获取日志实例
logger = get_logger('db')

# PostgreSQL 配置
PG_HOST = os.getenv('PG_HOST', 'localhost')
PG_PORT = os.getenv('PG_PORT', '5432')
PG_DATABASE = os.getenv('PG_DATABASE', 'tidewatch')
PG_USER = os.getenv('PG_USER', 'postgres')
PG_PASSWORD = os.getenv('PG_PASSWORD', 'tidewatch990')


@contextmanager
def get_db_conn():
    """上下文管理器：自动管理数据库连接"""
    conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        database=PG_DATABASE,
        user=PG_USER,
        password=PG_PASSWORD,
        cursor_factory=RealDictCursor
    )
    
    try:
        yield conn
    finally:
        conn.close()