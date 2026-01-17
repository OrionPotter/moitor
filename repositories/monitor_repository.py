from utils.db import get_db_conn
from models.monitor_stock import MonitorStock
from datetime import datetime
from utils.logger import get_logger

logger = get_logger('monitor_repository')


class MonitorStockRepository:
    """监控股票数据仓储层（异步版本）"""

    @staticmethod
    async def get_all():
        """获取所有监控股票"""
        logger.debug("SQL: SELECT * FROM monitor_stocks ORDER BY code")
        async with get_db_conn() as conn:
            rows = await conn.fetch('SELECT * FROM monitor_stocks ORDER BY code')
            logger.debug(f"SQL: 查询返回 {len(rows)} 条记录")
            return [
                MonitorStock(
                    id=row['id'],
                    code=row['code'],
                    name=row['name'],
                    timeframe=row['timeframe'],
                    reasonable_pe_min=row['reasonable_pe_min'],
                    reasonable_pe_max=row['reasonable_pe_max'],
                    enabled=bool(row['enabled']),
                    created_at=row['created_at'],
                    updated_at=row['updated_at']
                )
                for row in rows
            ]

    @staticmethod
    async def get_enabled():
        """获取所有启用的监控股票"""
        logger.debug("SQL: SELECT * FROM monitor_stocks WHERE enabled = 1 ORDER BY code")
        async with get_db_conn() as conn:
            rows = await conn.fetch('SELECT * FROM monitor_stocks WHERE enabled = 1 ORDER BY code')
            logger.debug(f"SQL: 查询返回 {len(rows)} 条记录")
            return [
                MonitorStock(
                    id=row['id'],
                    code=row['code'],
                    name=row['name'],
                    timeframe=row['timeframe'],
                    reasonable_pe_min=row['reasonable_pe_min'],
                    reasonable_pe_max=row['reasonable_pe_max'],
                    enabled=bool(row['enabled']),
                    created_at=row['created_at'],
                    updated_at=row['updated_at']
                )
                for row in rows
            ]

    @staticmethod
    async def get_by_code(code):
        """根据代码获取监控股票"""
        logger.debug(f"SQL: SELECT * FROM monitor_stocks WHERE code = '{code}'")
        async with get_db_conn() as conn:
            row = await conn.fetchrow('SELECT * FROM monitor_stocks WHERE code = $1', code)
            if row:
                logger.debug("SQL: 查询返回 1 条记录")
                return MonitorStock(
                    id=row['id'],
                    code=row['code'],
                    name=row['name'],
                    timeframe=row['timeframe'],
                    reasonable_pe_min=row['reasonable_pe_min'],
                    reasonable_pe_max=row['reasonable_pe_max'],
                    enabled=bool(row['enabled']),
                    created_at=row['created_at'],
                    updated_at=row['updated_at']
                )
            logger.debug("SQL: 查询返回 0 条记录")
            return None

    @staticmethod
    async def add(code, name, timeframe, reasonable_pe_min=15, reasonable_pe_max=20):
        """添加监控股票"""
        logger.info(f"SQL: INSERT INTO monitor_stocks (code, name, timeframe, reasonable_pe_min, reasonable_pe_max) VALUES ('{code}', '{name}', '{timeframe}', {reasonable_pe_min}, {reasonable_pe_max})")
        async with get_db_conn() as conn:
            try:
                await conn.execute(
                    '''INSERT INTO monitor_stocks
                       (code, name, timeframe, reasonable_pe_min, reasonable_pe_max)
                       VALUES ($1, $2, $3, $4, $5)''',
                    code, name, timeframe, reasonable_pe_min, reasonable_pe_max
                )
                logger.info(f"SQL: 插入成功")
                return True, "添加成功"
            except Exception as e:
                logger.error(f"SQL: 插入失败: {str(e)}")
                if 'unique' in str(e).lower() or 'duplicate' in str(e).lower():
                    return False, "监控股票已存在"
                return False, str(e)

    @staticmethod
    async def update(code, name, timeframe, reasonable_pe_min, reasonable_pe_max):
        """更新监控股票"""
        logger.info(f"SQL: UPDATE monitor_stocks SET name = '{name}', timeframe = '{timeframe}', reasonable_pe_min = {reasonable_pe_min}, reasonable_pe_max = {reasonable_pe_max}, updated_at = CURRENT_TIMESTAMP WHERE code = '{code}'")
        async with get_db_conn() as conn:
            result = await conn.execute(
                '''UPDATE monitor_stocks
                   SET name = $1, timeframe = $2, reasonable_pe_min = $3,
                       reasonable_pe_max = $4, updated_at = CURRENT_TIMESTAMP
                   WHERE code = $5''',
                name, timeframe, reasonable_pe_min, reasonable_pe_max, code
            )
            logger.info(f"SQL: 更新成功")
            return result == 'UPDATE 1'

    @staticmethod
    async def delete(code):
        """删除监控股票"""
        logger.info(f"SQL: DELETE FROM monitor_stocks WHERE code = '{code}'")
        async with get_db_conn() as conn:
            result = await conn.execute('DELETE FROM monitor_stocks WHERE code = $1', code)
            logger.info(f"SQL: 删除成功")
            return result == 'DELETE 1'

    @staticmethod
    async def toggle_enabled(code, enabled):
        """启用/禁用监控股票"""
        logger.info(f"SQL: UPDATE monitor_stocks SET enabled = {int(enabled)}, updated_at = CURRENT_TIMESTAMP WHERE code = '{code}'")
        async with get_db_conn() as conn:
            result = await conn.execute(
                'UPDATE monitor_stocks SET enabled = $1, updated_at = CURRENT_TIMESTAMP WHERE code = $2',
                int(enabled), code
            )
            logger.info(f"SQL: 更新成功")
            return result == 'UPDATE 1'