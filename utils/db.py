# models/db.py
import sqlite3
from contextlib import contextmanager
from utils.logger import get_logger

# 获取日志实例
logger = get_logger('db')

DB_PATH = 'portfolio.db'


@contextmanager
def get_db_conn():
    """上下文管理器：自动管理数据库连接"""
    conn = sqlite3.connect(DB_PATH)
    try:
        yield conn
    finally:
        conn.close()