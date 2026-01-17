# repositories/portfolio_repository.py
from utils.db import get_db_conn
from models.stock import Stock
from utils.logger import get_logger

logger = get_logger('portfolio_repository')


class StockRepository:
    """股票持仓数据仓储层（异步版本）"""

    @staticmethod
    async def get_all():
        """获取所有股票"""
        logger.info("SQL: SELECT id, code, name, cost_price, shares FROM portfolio ORDER BY code")
        async with get_db_conn() as conn:
            rows = await conn.fetch('SELECT id, code, name, cost_price, shares FROM portfolio ORDER BY code')
            logger.info(f"SQL: 查询返回 {len(rows)} 条记录")
            return [
                Stock(id=row['id'], code=row['code'], name=row['name'], cost_price=row['cost_price'], shares=row['shares'])
                for row in rows
            ]

    @staticmethod
    async def get_by_code(code):
        """根据代码获取单只股票"""
        logger.info(f"SQL: SELECT id, code, name, cost_price, shares FROM portfolio WHERE code = '{code}'")
        async with get_db_conn() as conn:
            row = await conn.fetchrow('SELECT id, code, name, cost_price, shares FROM portfolio WHERE code = $1', code)
            if row:
                logger.info(f"SQL: 查询返回 1 条记录")
                return Stock(id=row['id'], code=row['code'], name=row['name'], cost_price=row['cost_price'], shares=row['shares'])
            logger.info("SQL: 查询返回 0 条记录")
            return None

    @staticmethod
    async def add(code, name, cost_price, shares):
        """添加股票"""
        logger.info(f"SQL: INSERT INTO portfolio (code, name, cost_price, shares) VALUES ('{code}', '{name}', {cost_price}, {shares})")
        async with get_db_conn() as conn:
            try:
                await conn.execute(
                    'INSERT INTO portfolio (code, name, cost_price, shares) VALUES ($1, $2, $3, $4)',
                    code, name, cost_price, shares
                )
                logger.info(f"SQL: 插入成功")
                return True, "添加成功"
            except Exception as e:
                logger.error(f"SQL: 插入失败: {str(e)}")
                if 'unique' in str(e).lower() or 'duplicate' in str(e).lower():
                    return False, "股票代码已存在"
                return False, str(e)

    @staticmethod
    async def update(code, name, cost_price, shares):
        """更新股票"""
        logger.info(f"SQL: UPDATE portfolio SET name = '{name}', cost_price = {cost_price}, shares = {shares} WHERE code = '{code}'")
        async with get_db_conn() as conn:
            result = await conn.execute(
                'UPDATE portfolio SET name = $1, cost_price = $2, shares = $3 WHERE code = $4',
                name, cost_price, shares, code
            )
            logger.info(f"SQL: 更新成功")
            return 'UPDATE 1' in result

    @staticmethod
    async def delete(code):
        """删除股票"""
        logger.info(f"SQL: DELETE FROM portfolio WHERE code = '{code}'")
        async with get_db_conn() as conn:
            result = await conn.execute('DELETE FROM portfolio WHERE code = $1', code)
            logger.info(f"SQL: 删除成功")
            return 'DELETE 1' in result