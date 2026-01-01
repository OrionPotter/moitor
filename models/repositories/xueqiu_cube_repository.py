from models.db import get_db_conn


class XueqiuCubeRepository:
    """雪球组合数据仓库"""
    
    @staticmethod
    def get_all() -> list:
        """获取所有雪球组合"""
        with get_db_conn() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT id, cube_symbol, cube_name, enabled, created_at, updated_at
                FROM xueqiu_cubes
                ORDER BY id
            ''')
            rows = cursor.fetchall()
            from models.entities.xueqiu_cube import XueqiuCube
            return [
                XueqiuCube(
                    id=row[0],
                    cube_symbol=row[1],
                    cube_name=row[2],
                    enabled=bool(row[3]),
                    created_at=row[4],
                    updated_at=row[5]
                )
                for row in rows
            ]
    
    @staticmethod
    def get_by_symbol(cube_symbol: str):
        """根据组合ID获取组合"""
        with get_db_conn() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT id, cube_symbol, cube_name, enabled, created_at, updated_at
                FROM xueqiu_cubes
                WHERE cube_symbol = ?
            ''', (cube_symbol,))
            row = cursor.fetchone()
            if row:
                from models.entities.xueqiu_cube import XueqiuCube
                return XueqiuCube(
                    id=row[0],
                    cube_symbol=row[1],
                    cube_name=row[2],
                    enabled=bool(row[3]),
                    created_at=row[4],
                    updated_at=row[5]
                )
            return None
    
    @staticmethod
    def add(cube_symbol: str, cube_name: str, enabled: bool = True):
        """添加雪球组合"""
        with get_db_conn() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute('''
                    INSERT INTO xueqiu_cubes (cube_symbol, cube_name, enabled)
                    VALUES (?, ?, ?)
                ''', (cube_symbol, cube_name, 1 if enabled else 0))
                conn.commit()
                return True, "添加成功"
            except Exception as e:
                conn.rollback()
                return False, f"添加失败: {str(e)}"
    
    @staticmethod
    def update(cube_symbol: str, cube_name: str, enabled: bool):
        """更新雪球组合"""
        with get_db_conn() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute('''
                    UPDATE xueqiu_cubes
                    SET cube_name = ?, enabled = ?
                    WHERE cube_symbol = ?
                ''', (cube_name, 1 if enabled else 0, cube_symbol))
                conn.commit()
                return cursor.rowcount > 0
            except Exception as e:
                conn.rollback()
                return False
    
    @staticmethod
    def delete(cube_symbol: str):
        """删除雪球组合"""
        with get_db_conn() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute('DELETE FROM xueqiu_cubes WHERE cube_symbol = ?', (cube_symbol,))
                conn.commit()
                return cursor.rowcount > 0
            except Exception as e:
                conn.rollback()
                return False
    
    @staticmethod
    def toggle_enabled(cube_symbol: str, enabled: bool):
        """启用/禁用雪球组合"""
        with get_db_conn() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute('''
                    UPDATE xueqiu_cubes
                    SET enabled = ?
                    WHERE cube_symbol = ?
                ''', (1 if enabled else 0, cube_symbol))
                conn.commit()
                return cursor.rowcount > 0
            except Exception as e:
                conn.rollback()
                return False
    
    @staticmethod
    def get_enabled_symbols() -> list:
        """获取所有启用的组合ID列表"""
        with get_db_conn() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT cube_symbol FROM xueqiu_cubes WHERE enabled = 1
            ''')
            rows = cursor.fetchall()
            return [row[0] for row in rows]