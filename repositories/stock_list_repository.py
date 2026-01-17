from utils.db import get_db_conn
from utils.logger import get_logger
from datetime import datetime, timedelta

logger = get_logger('stock_list_repository')


class StockListRepository:
    """股票代码仓储层（异步版本）"""

    @staticmethod
    async def get_all():
        """获取所有股票代码"""
        logger.debug("SQL: SELECT code, name, last_update, created_at, updated_at FROM stock_list ORDER BY code")
        async with get_db_conn() as conn:
            rows = await conn.fetch('SELECT code, name, last_update, created_at, updated_at FROM stock_list ORDER BY code')
            logger.debug(f"SQL: 查询返回 {len(rows)} 条记录")
            from models.stock_list import StockList
            return [
                StockList(
                    code=row['code'],
                    name=row['name'],
                    last_update=row['last_update'],
                    created_at=row['created_at'],
                    updated_at=row['updated_at']
                )
                for row in rows
            ]

    @staticmethod
    async def get_pending_update(limit=10):
        """获取需要更新的股票（每次最多 limit 条）

        判断规则：
        1. last_update 为空（从未更新过）
        2. 当前时间 - last_update > 12小时

        Args:
            limit: 每次返回的最大数量

        Returns:
            list: StockList 对象列表
        """
        twelve_hours_ago = datetime.now() - timedelta(hours=12)

        logger.debug(f"SQL: 获取需要更新的股票，limit={limit}, 12小时前={twelve_hours_ago}")

        async with get_db_conn() as conn:
            rows = await conn.fetch(
                '''SELECT code, name, last_update, created_at, updated_at
                   FROM stock_list
                   WHERE last_update IS NULL
                      OR last_update < $1
                   ORDER BY
                       CASE WHEN last_update IS NULL THEN 0 ELSE 1 END,
                       last_update ASC
                   LIMIT $2''',
                twelve_hours_ago, limit
            )

            logger.info(f"SQL: 查询返回 {len(rows)} 条记录")
            from models.stock_list import StockList
            return [
                StockList(
                    code=row['code'],
                    name=row['name'],
                    last_update=row['last_update'],
                    created_at=row['created_at'],
                    updated_at=row['updated_at']
                )
                for row in rows
            ]

    @staticmethod
    async def update_last_update(codes):
        """更新股票的最后更新时间

        Args:
            codes: 股票代码列表
        """
        if not codes:
            return

        logger.info(f"SQL: 批量更新 {len(codes)} 只股票的 last_update 时间")

        async with get_db_conn() as conn:
            await conn.execute(
                '''UPDATE stock_list
                   SET last_update = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
                   WHERE code = ANY($1)''',
                codes
            )
            logger.info(f"SQL: 批量更新成功")

    @staticmethod
    async def get_by_code(code):
        """根据代码获取股票"""
        logger.debug(f"SQL: SELECT code, name, created_at, updated_at FROM stock_list WHERE code = '{code}'")
        async with get_db_conn() as conn:
            row = await conn.fetchrow('SELECT code, name, created_at, updated_at FROM stock_list WHERE code = $1', code)
            if row:
                logger.debug("SQL: 查询返回 1 条记录")
                from models.stock_list import StockList
                return StockList(
                    code=row['code'],
                    name=row['name'],
                    created_at=row['created_at'],
                    updated_at=row['updated_at']
                )
            logger.debug("SQL: 查询返回 0 条记录")
            return None

    @staticmethod
    async def batch_upsert(stock_list):
        """批量插入或更新股票列表"""
        logger.info(f"SQL: 批量插入/更新股票列表，数据量: {len(stock_list)}")
        async with get_db_conn() as conn:
            try:
                # 准备数据
                data = [(stock['code'], stock['name']) for stock in stock_list]

                # 使用 ON CONFLICT 实现插入或更新
                await conn.executemany(
                    '''INSERT INTO stock_list (code, name)
                       VALUES ($1, $2)
                       ON CONFLICT (code) DO UPDATE
                       SET name = EXCLUDED.name, updated_at = CURRENT_TIMESTAMP''',
                    data
                )
                logger.info(f"SQL: 批量插入/更新成功")
                return True, len(data)
            except Exception as e:
                logger.error(f"SQL: 批量插入/更新失败: {str(e)}")
                return False, str(e)

    @staticmethod
    async def get_count():
        """获取股票总数"""
        logger.debug("SQL: SELECT COUNT(*) FROM stock_list")
        async with get_db_conn() as conn:
            count = await conn.fetchval('SELECT COUNT(*) FROM stock_list')
            logger.debug(f"SQL: 查询返回股票总数: {count}")
            return count

    @staticmethod
    async def search_by_name(keyword):
        """根据名称搜索股票"""
        logger.debug(f"SQL: SELECT code, name, created_at, updated_at FROM stock_list WHERE name LIKE '%{keyword}%'")
        async with get_db_conn() as conn:
            rows = await conn.fetch(
                'SELECT code, name, created_at, updated_at FROM stock_list WHERE name LIKE $1 ORDER BY code',
                f'%{keyword}%'
            )
            logger.debug(f"SQL: 查询返回 {len(rows)} 条记录")
            from models.stock_list import StockList
            return [
                StockList(
                    code=row['code'],
                    name=row['name'],
                    created_at=row['created_at'],
                    updated_at=row['updated_at']
                )
                for row in rows
            ]