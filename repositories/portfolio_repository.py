# repositories/portfolio_repository.py
from utils.db import get_db_conn
from models.stock import Stock
import sqlite3


class StockRepository:
    """股票持仓数据仓储层"""

    @staticmethod
    def get_all():
        """获取所有股票"""
        with get_db_conn() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT id, code, name, cost_price, shares FROM portfolio ORDER BY code')
            rows = cursor.fetchall()
            return [
                Stock(id=row[0], code=row[1], name=row[2], cost_price=row[3], shares=row[4])
                for row in rows
            ]

    @staticmethod
    def get_by_code(code):
        """根据代码获取单只股票"""
        with get_db_conn() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT id, code, name, cost_price, shares FROM portfolio WHERE code = ?', (code,))
            row = cursor.fetchone()
            if row:
                return Stock(id=row[0], code=row[1], name=row[2], cost_price=row[3], shares=row[4])
            return None

    @staticmethod
    def add(code, name, cost_price, shares):
        """添加股票"""
        with get_db_conn() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    'INSERT INTO portfolio (code, name, cost_price, shares) VALUES (?, ?, ?, ?)',
                    (code, name, cost_price, shares)
                )
                conn.commit()
                return True, "添加成功"
            except sqlite3.IntegrityError:
                return False, "股票代码已存在"
            except Exception as e:
                return False, str(e)

    @staticmethod
    def update(code, name, cost_price, shares):
        """更新股票"""
        with get_db_conn() as conn:
            cursor = conn.cursor()
            cursor.execute(
                'UPDATE portfolio SET name = ?, cost_price = ?, shares = ? WHERE code = ?',
                (name, cost_price, shares, code)
            )
            conn.commit()
            return cursor.rowcount > 0

    @staticmethod
    def delete(code):
        """删除股票"""
        with get_db_conn() as conn:
            cursor = conn.cursor()
            cursor.execute('DELETE FROM portfolio WHERE code = ?', (code,))
            conn.commit()
            return cursor.rowcount > 0