# repositories/monitor_repository.py
from utils.db import get_db_conn
from models.monitor_stock import MonitorStock
from datetime import datetime
import sqlite3


class MonitorStockRepository:
    """监控股票数据仓储层"""

    @staticmethod
    def get_all():
        """获取所有监控股票"""
        with get_db_conn() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM monitor_stocks ORDER BY code')
            rows = cursor.fetchall()
            return [
                MonitorStock(
                    id=row[0],
                    code=row[1],
                    name=row[2],
                    timeframe=row[3],
                    reasonable_pe_min=row[4],
                    reasonable_pe_max=row[5],
                    enabled=bool(row[6]),
                    created_at=datetime.strptime(row[7], '%Y-%m-%d %H:%M:%S') if row[7] else None,
                    updated_at=datetime.strptime(row[8], '%Y-%m-%d %H:%M:%S') if row[8] else None
                )
                for row in rows
            ]

    @staticmethod
    def get_enabled():
        """获取所有启用的监控股票"""
        with get_db_conn() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM monitor_stocks WHERE enabled = 1 ORDER BY code')
            rows = cursor.fetchall()
            return [
                MonitorStock(
                    id=row[0],
                    code=row[1],
                    name=row[2],
                    timeframe=row[3],
                    reasonable_pe_min=row[4],
                    reasonable_pe_max=row[5],
                    enabled=bool(row[6]),
                    created_at=datetime.strptime(row[7], '%Y-%m-%d %H:%M:%S') if row[7] else None,
                    updated_at=datetime.strptime(row[8], '%Y-%m-%d %H:%M:%S') if row[8] else None
                )
                for row in rows
            ]

    @staticmethod
    def get_by_code(code):
        """根据代码获取监控股票"""
        with get_db_conn() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM monitor_stocks WHERE code = ?', (code,))
            row = cursor.fetchone()
            if row:
                return MonitorStock(
                    id=row[0],
                    code=row[1],
                    name=row[2],
                    timeframe=row[3],
                    reasonable_pe_min=row[4],
                    reasonable_pe_max=row[5],
                    enabled=bool(row[6]),
                    created_at=datetime.strptime(row[7], '%Y-%m-%d %H:%M:%S') if row[7] else None,
                    updated_at=datetime.strptime(row[8], '%Y-%m-%d %H:%M:%S') if row[8] else None
                )
            return None

    @staticmethod
    def add(code, name, timeframe, reasonable_pe_min=15, reasonable_pe_max=20):
        """添加监控股票"""
        with get_db_conn() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    '''INSERT INTO monitor_stocks
                       (code, name, timeframe, reasonable_pe_min, reasonable_pe_max)
                       VALUES (?, ?, ?, ?, ?)''',
                    (code, name, timeframe, reasonable_pe_min, reasonable_pe_max)
                )
                conn.commit()
                return True, "添加成功"
            except sqlite3.IntegrityError:
                return False, "监控股票已存在"
            except Exception as e:
                return False, str(e)

    @staticmethod
    def update(code, name, timeframe, reasonable_pe_min, reasonable_pe_max):
        """更新监控股票"""
        with get_db_conn() as conn:
            cursor = conn.cursor()
            cursor.execute(
                '''UPDATE monitor_stocks
                   SET name = ?, timeframe = ?, reasonable_pe_min = ?,
                       reasonable_pe_max = ?, updated_at = CURRENT_TIMESTAMP
                   WHERE code = ?''',
                (name, timeframe, reasonable_pe_min, reasonable_pe_max, code)
            )
            conn.commit()
            return cursor.rowcount > 0

    @staticmethod
    def delete(code):
        """删除监控股票"""
        with get_db_conn() as conn:
            cursor = conn.cursor()
            cursor.execute('DELETE FROM monitor_stocks WHERE code = ?', (code,))
            conn.commit()
            return cursor.rowcount > 0

    @staticmethod
    def toggle_enabled(code, enabled):
        """启用/禁用监控股票"""
        with get_db_conn() as conn:
            cursor = conn.cursor()
            cursor.execute(
                'UPDATE monitor_stocks SET enabled = ?, updated_at = CURRENT_TIMESTAMP WHERE code = ?',
                (int(enabled), code)
            )
            conn.commit()
            return cursor.rowcount > 0