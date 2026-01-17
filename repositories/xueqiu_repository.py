from utils.db import get_db_conn
from utils.logger import get_logger

logger = get_logger('xueqiu_repository')


class XueqiuCubeRepository:
    """雪球组合数据仓库（异步版本）"""

    @staticmethod
    async def get_all() -> list:
        """获取所有雪球组合"""
        logger.info("SQL: SELECT id, cube_symbol, cube_name, enabled, created_at, updated_at FROM xueqiu_cubes ORDER BY id")
        async with get_db_conn() as conn:
            rows = await conn.fetch('''
                SELECT id, cube_symbol, cube_name, enabled, created_at, updated_at
                FROM xueqiu_cubes
                ORDER BY id
            ''')
            logger.info(f"SQL: 查询返回 {len(rows)} 条记录")
            from models.xueqiu_cube import XueqiuCube
            return [
                XueqiuCube(
                    id=row['id'],
                    cube_symbol=row['cube_symbol'],
                    cube_name=row['cube_name'],
                    enabled=bool(row['enabled']),
                    created_at=row['created_at'],
                    updated_at=row['updated_at']
                )
                for row in rows
            ]

    @staticmethod
    async def get_by_symbol(cube_symbol: str):
        """根据组合ID获取组合"""
        logger.info(f"SQL: SELECT id, cube_symbol, cube_name, enabled, created_at, updated_at FROM xueqiu_cubes WHERE cube_symbol = '{cube_symbol}'")
        async with get_db_conn() as conn:
            row = await conn.fetchrow('''
                SELECT id, cube_symbol, cube_name, enabled, created_at, updated_at
                FROM xueqiu_cubes
                WHERE cube_symbol = $1
            ''', cube_symbol)
            if row:
                logger.info("SQL: 查询返回 1 条记录")
                from models.xueqiu_cube import XueqiuCube
                return XueqiuCube(
                    id=row['id'],
                    cube_symbol=row['cube_symbol'],
                    cube_name=row['cube_name'],
                    enabled=bool(row['enabled']),
                    created_at=row['created_at'],
                    updated_at=row['updated_at']
                )
            logger.info("SQL: 查询返回 0 条记录")
            return None

    @staticmethod
    async def add(cube_symbol: str, cube_name: str, enabled: bool = True):
        """添加雪球组合"""
        logger.info(f"SQL: INSERT INTO xueqiu_cubes (cube_symbol, cube_name, enabled) VALUES ('{cube_symbol}', '{cube_name}', {1 if enabled else 0})")
        async with get_db_conn() as conn:
            try:
                await conn.execute('''
                    INSERT INTO xueqiu_cubes (cube_symbol, cube_name, enabled)
                    VALUES ($1, $2, $3)
                ''', cube_symbol, cube_name, 1 if enabled else 0)
                logger.info(f"SQL: 插入成功")
                return True, "添加成功"
            except Exception as e:
                logger.error(f"SQL: 插入失败: {str(e)}")
                return False, f"添加失败: {str(e)}"

    @staticmethod
    async def update(cube_symbol: str, cube_name: str, enabled: bool):
        """更新雪球组合"""
        logger.info(f"SQL: UPDATE xueqiu_cubes SET cube_name = '{cube_name}', enabled = {1 if enabled else 0} WHERE cube_symbol = '{cube_symbol}'")
        async with get_db_conn() as conn:
            try:
                result = await conn.execute('''
                    UPDATE xueqiu_cubes
                    SET cube_name = $1, enabled = $2
                    WHERE cube_symbol = $3
                ''', cube_name, 1 if enabled else 0, cube_symbol)
                logger.info(f"SQL: 更新成功")
                return 'UPDATE 1' in result
            except Exception as e:
                logger.error(f"SQL: 更新失败: {str(e)}")
                return False

    @staticmethod
    async def delete(cube_symbol: str):
        """删除雪球组合"""
        logger.info(f"SQL: DELETE FROM xueqiu_cubes WHERE cube_symbol = '{cube_symbol}'")
        async with get_db_conn() as conn:
            try:
                result = await conn.execute('DELETE FROM xueqiu_cubes WHERE cube_symbol = $1', cube_symbol)
                logger.info(f"SQL: 删除成功")
                return 'DELETE 1' in result
            except Exception as e:
                logger.error(f"SQL: 删除失败: {str(e)}")
                return False

    @staticmethod
    async def toggle_enabled(cube_symbol: str, enabled: bool):
        """启用/禁用雪球组合"""
        logger.info(f"SQL: UPDATE xueqiu_cubes SET enabled = {1 if enabled else 0} WHERE cube_symbol = '{cube_symbol}'")
        async with get_db_conn() as conn:
            try:
                result = await conn.execute('''
                    UPDATE xueqiu_cubes
                    SET enabled = $1
                    WHERE cube_symbol = $2
                ''', 1 if enabled else 0, cube_symbol)
                logger.info(f"SQL: 更新成功")
                return 'UPDATE 1' in result
            except Exception as e:
                logger.error(f"SQL: 更新失败: {str(e)}")
                return False

    @staticmethod
    async def get_enabled_symbols() -> list:
        """获取所有启用的组合ID列表"""
        logger.info("SQL: SELECT cube_symbol FROM xueqiu_cubes WHERE enabled = 1")
        async with get_db_conn() as conn:
            rows = await conn.fetch('''
                SELECT cube_symbol FROM xueqiu_cubes WHERE enabled = 1
            ''')
            logger.info(f"SQL: 查询返回 {len(rows)} 条记录")
            return [row['cube_symbol'] for row in rows]