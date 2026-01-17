# repositories/eps_cache_repository.py
from utils.db import get_db_conn
from utils.logger import get_logger

logger = get_logger('eps_cache_repository')


class EpsCacheRepository:
    """EPS 预测缓存仓储层（异步版本）"""

    @staticmethod
    async def get(code):
        """获取 EPS 缓存"""
        async with get_db_conn() as conn:
            row = await conn.fetchrow(
                '''SELECT eps_value, updated_at 
                   FROM eps_cache 
                   WHERE code = $1''',
                code
            )
            
            if row:
                # 检查是否过期（24小时）
                from datetime import datetime, timedelta
                if datetime.now() - row['updated_at'] < timedelta(hours=24):
                    eps_value = row['eps_value']
                    logger.debug(f"从数据库缓存获取 {code} 的 EPS: {eps_value}")
                    return eps_value
            
            return None

    @staticmethod
    async def set(code, eps_value):
        """设置 EPS 缓存"""
        async with get_db_conn() as conn:
            try:
                await conn.execute(
                    '''INSERT INTO eps_cache (code, eps_value)
                       VALUES ($1, $2)
                       ON CONFLICT (code) DO UPDATE
                       SET eps_value = EXCLUDED.eps_value,
                           updated_at = CURRENT_TIMESTAMP''',
                    code, eps_value
                )
                logger.debug(f"已缓存 {code} 的 EPS: {eps_value}")
                return True
            except Exception as e:
                logger.error(f"保存 EPS 缓存失败: {e}")
                return False

    @staticmethod
    async def get_batch(codes):
        """批量获取 EPS 缓存"""
        if not codes:
            return {}
        
        async with get_db_conn() as conn:
            rows = await conn.fetch(
                '''SELECT code, eps_value, updated_at
                   FROM eps_cache
                   WHERE code = ANY($1)''',
                codes
            )
            
            from datetime import datetime, timedelta
            result = {}
            for row in rows:
                # 检查是否过期
                if datetime.now() - row['updated_at'] < timedelta(hours=24):
                    eps_value = row['eps_value']
                    result[row['code']] = eps_value
            
            return result

    @staticmethod
    async def clean_old_data(hours=24):
        """清理过期数据"""
        async with get_db_conn() as conn:
            result = await conn.execute(
                "DELETE FROM eps_cache WHERE updated_at < NOW() - INTERVAL '1 hour' * $1",
                hours
            )
            deleted_count = int(result.split()[-1]) if result else 0
            logger.info(f"清理了 {deleted_count} 条过期 EPS 缓存")
            return deleted_count