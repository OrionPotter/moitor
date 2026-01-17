# repositories/portfolio_repository.py
from utils.db import get_db_conn
from models.stock import Stock
from utils.logger import get_logger

logger = get_logger('portfolio_repository')


class StockRepository:
    """股票持仓数据仓储层"""

    @staticmethod
    def get_all():
        """获取所有股票"""
        logger.info("SQL: SELECT id, code, name, cost_price, shares FROM portfolio ORDER BY code")
        with get_db_conn() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT id, code, name, cost_price, shares FROM portfolio ORDER BY code')
            rows = cursor.fetchall()
            logger.info(f"SQL: 查询返回 {len(rows)} 条记录")
            return [
                Stock(id=row['id'], code=row['code'], name=row['name'], cost_price=row['cost_price'], shares=row['shares'])
                for row in rows
            ]

    @staticmethod
    def get_by_code(code):
        """根据代码获取单只股票"""
        logger.info(f"SQL: SELECT id, code, name, cost_price, shares FROM portfolio WHERE code = '{code}'")
        with get_db_conn() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT id, code, name, cost_price, shares FROM portfolio WHERE code = %s', (code,))
            row = cursor.fetchone()
            if row:
                logger.info(f"SQL: 查询返回 1 条记录")
                return Stock(id=row['id'], code=row['code'], name=row['name'], cost_price=row['cost_price'], shares=row['shares'])
            logger.info("SQL: 查询返回 0 条记录")
            return None

    @staticmethod
    def add(code, name, cost_price, shares):
        """添加股票"""
        logger.info(f"SQL: INSERT INTO portfolio (code, name, cost_price, shares) VALUES ('{code}', '{name}', {cost_price}, {shares})")
        with get_db_conn() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    'INSERT INTO portfolio (code, name, cost_price, shares) VALUES (%s, %s, %s, %s)',
                    (code, name, cost_price, shares)
                )
                conn.commit()
                logger.info(f"SQL: 插入成功，影响行数: {cursor.rowcount}")
                return True, "添加成功"
            except Exception as e:
                logger.error(f"SQL: 插入失败: {str(e)}")
                if 'unique' in str(e).lower() or 'duplicate' in str(e).lower():
                    return False, "股票代码已存在"
                return False, str(e)

    @staticmethod
    def update(code, name, cost_price, shares):
        """更新股票"""
        logger.info(f"SQL: UPDATE portfolio SET name = '{name}', cost_price = {cost_price}, shares = {shares} WHERE code = '{code}'")
        with get_db_conn() as conn:
            cursor = conn.cursor()
            cursor.execute(
                'UPDATE portfolio SET name = %s, cost_price = %s, shares = %s WHERE code = %s',
                (name, cost_price, shares, code)
            )
            conn.commit()
            logger.info(f"SQL: 更新成功，影响行数: {cursor.rowcount}")
            return cursor.rowcount > 0

    @staticmethod
    def delete(code):
        """删除股票"""
        logger.info(f"SQL: DELETE FROM portfolio WHERE code = '{code}'")
        with get_db_conn() as conn:
            cursor = conn.cursor()
            cursor.execute('DELETE FROM portfolio WHERE code = %s', (code,))
            conn.commit()
            logger.info(f"SQL: 删除成功，影响行数: {cursor.rowcount}")
            return cursor.rowcount > 0