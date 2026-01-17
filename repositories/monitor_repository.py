# repositories/monitor_repository.py
from utils.db import get_db_conn
from models.monitor_stock import MonitorStock
from datetime import datetime
from utils.logger import get_logger

logger = get_logger('monitor_repository')


class MonitorStockRepository:
    """监控股票数据仓储层"""

    @staticmethod
    def get_all():
        """获取所有监控股票"""
        logger.info("SQL: SELECT * FROM monitor_stocks ORDER BY code")
        with get_db_conn() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM monitor_stocks ORDER BY code')
            rows = cursor.fetchall()
            logger.info(f"SQL: 查询返回 {len(rows)} 条记录")
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
    def get_enabled():
        """获取所有启用的监控股票"""
        logger.info("SQL: SELECT * FROM monitor_stocks WHERE enabled = 1 ORDER BY code")
        with get_db_conn() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM monitor_stocks WHERE enabled = 1 ORDER BY code')
            rows = cursor.fetchall()
            logger.info(f"SQL: 查询返回 {len(rows)} 条记录")
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
    def get_by_code(code):
        """根据代码获取监控股票"""
        logger.info(f"SQL: SELECT * FROM monitor_stocks WHERE code = '{code}'")
        with get_db_conn() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM monitor_stocks WHERE code = %s', (code,))
            row = cursor.fetchone()
            if row:
                logger.info("SQL: 查询返回 1 条记录")
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
            logger.info("SQL: 查询返回 0 条记录")
            return None

    @staticmethod
    def add(code, name, timeframe, reasonable_pe_min=15, reasonable_pe_max=20):
        """添加监控股票"""
        logger.info(f"SQL: INSERT INTO monitor_stocks (code, name, timeframe, reasonable_pe_min, reasonable_pe_max) VALUES ('{code}', '{name}', '{timeframe}', {reasonable_pe_min}, {reasonable_pe_max})")
        with get_db_conn() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    '''INSERT INTO monitor_stocks
                       (code, name, timeframe, reasonable_pe_min, reasonable_pe_max)
                       VALUES (%s, %s, %s, %s, %s)''',
                    (code, name, timeframe, reasonable_pe_min, reasonable_pe_max)
                )
                conn.commit()
                logger.info(f"SQL: 插入成功，影响行数: {cursor.rowcount}")
                return True, "添加成功"
            except Exception as e:
                logger.error(f"SQL: 插入失败: {str(e)}")
                if 'unique' in str(e).lower() or 'duplicate' in str(e).lower():
                    return False, "监控股票已存在"
                return False, str(e)

    @staticmethod
    def update(code, name, timeframe, reasonable_pe_min, reasonable_pe_max):
        """更新监控股票"""
        logger.info(f"SQL: UPDATE monitor_stocks SET name = '{name}', timeframe = '{timeframe}', reasonable_pe_min = {reasonable_pe_min}, reasonable_pe_max = {reasonable_pe_max}, updated_at = CURRENT_TIMESTAMP WHERE code = '{code}'")
        with get_db_conn() as conn:
            cursor = conn.cursor()
            cursor.execute(
                '''UPDATE monitor_stocks
                   SET name = %s, timeframe = %s, reasonable_pe_min = %s,
                       reasonable_pe_max = %s, updated_at = CURRENT_TIMESTAMP
                   WHERE code = %s''',
                (name, timeframe, reasonable_pe_min, reasonable_pe_max, code)
            )
            conn.commit()
            logger.info(f"SQL: 更新成功，影响行数: {cursor.rowcount}")
            return cursor.rowcount > 0

    @staticmethod
    def delete(code):
        """删除监控股票"""
        logger.info(f"SQL: DELETE FROM monitor_stocks WHERE code = '{code}'")
        with get_db_conn() as conn:
            cursor = conn.cursor()
            cursor.execute('DELETE FROM monitor_stocks WHERE code = %s', (code,))
            conn.commit()
            logger.info(f"SQL: 删除成功，影响行数: {cursor.rowcount}")
            return cursor.rowcount > 0

    @staticmethod
    def toggle_enabled(code, enabled):
        """启用/禁用监控股票"""
        logger.info(f"SQL: UPDATE monitor_stocks SET enabled = {int(enabled)}, updated_at = CURRENT_TIMESTAMP WHERE code = '{code}'")
        with get_db_conn() as conn:
            cursor = conn.cursor()
            cursor.execute(
                'UPDATE monitor_stocks SET enabled = %s, updated_at = CURRENT_TIMESTAMP WHERE code = %s',
                (int(enabled), code)
            )
            conn.commit()
            logger.info(f"SQL: 更新成功，影响行数: {cursor.rowcount}")
            return cursor.rowcount > 0