import os
from contextlib import asynccontextmanager, contextmanager
from utils.logger import get_logger
import asyncpg

# 获取日志实例
logger = get_logger('db')

# PostgreSQL 配置
PG_HOST = os.getenv('PG_HOST', 'localhost')
PG_PORT = os.getenv('PG_PORT', '5432')
PG_DATABASE = os.getenv('PG_DATABASE', 'tidewatch')
PG_USER = os.getenv('PG_USER', 'postgres')
PG_PASSWORD = os.getenv('PG_PASSWORD', 'tidewatch990')
PG_MIN_CONN = int(os.getenv('PG_MIN_CONN', '5'))
PG_MAX_CONN = int(os.getenv('PG_MAX_CONN', '50'))

# 全局连接池
_pool = None


async def init_db_pool():
    """初始化数据库连接池"""
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(
            host=PG_HOST,
            port=PG_PORT,
            database=PG_DATABASE,
            user=PG_USER,
            password=PG_PASSWORD,
            min_size=PG_MIN_CONN,
            max_size=PG_MAX_CONN,
            command_timeout=60
        )
        logger.info(f"数据库连接池初始化成功: min={PG_MIN_CONN}, max={PG_MAX_CONN}")


async def close_db_pool():
    """关闭数据库连接池"""
    global _pool
    if _pool:
        await _pool.close()
        _pool = None
        logger.info("数据库连接池已关闭")


async def get_pool():
    """获取数据库连接池"""
    global _pool
    if _pool is None:
        await init_db_pool()
    return _pool


@asynccontextmanager
async def get_db_conn():
    """异步上下文管理器：自动管理数据库连接"""
    pool = await get_pool()
    conn = await pool.acquire()
    
    try:
        yield conn
    finally:
        await pool.release(conn)


def get_db_conn_sync():
    """同步数据库连接（用于向后兼容，使用 psycopg2）"""
    from psycopg2 import connect
    from psycopg2.extras import RealDictCursor
    
    return connect(
        host=PG_HOST,
        port=PG_PORT,
        database=PG_DATABASE,
        user=PG_USER,
        password=PG_PASSWORD,
        cursor_factory=RealDictCursor
    )


@contextmanager
def get_db_conn_context():
    """同步上下文管理器：用于向后兼容"""
    conn = get_db_conn_sync()
    
    try:
        yield conn
    finally:
        conn.close()