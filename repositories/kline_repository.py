# repositories/kline_repository.py
from utils.db import get_db_conn
from utils.logger import get_logger
from datetime import datetime
import pandas as pd

logger = get_logger('kline_repository')


class KlineRepository:
    """K线数据仓储层"""

    @staticmethod
    def save_batch(code, kline_data):
        """批量保存K线数据"""
        logger.info(f"SQL: 批量插入/更新 {code} 的 K线数据，数据量: {len(kline_data)}")
        with get_db_conn() as conn:
            cursor = conn.cursor()
            try:
                insert_data = [
                    (code, row['日期'], row['开盘'], row['收盘'],
                     row['最高'], row['最低'], 0, row.get('amount', 0))
                    for _, row in kline_data.iterrows()
                ]
                cursor.executemany(
                    '''INSERT INTO stock_kline_data
                       (code, date, open, close, high, low, volume, amount, updated_at)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                       ON CONFLICT (code, date) DO UPDATE
                       SET open = EXCLUDED.open, close = EXCLUDED.close, high = EXCLUDED.high,
                           low = EXCLUDED.low, volume = EXCLUDED.volume, amount = EXCLUDED.amount,
                           updated_at = CURRENT_TIMESTAMP''',
                    insert_data
                )
                conn.commit()
                logger.info(f"SQL: 批量插入/更新成功，影响行数: {cursor.rowcount}")
                return True, len(insert_data)
            except Exception as e:
                logger.error(f"SQL: 批量插入/更新失败: {str(e)}")
                return False, str(e)

    @staticmethod
    def get_by_code(code, limit=250):
        """获取K线数据（返回DataFrame）"""
        logger.info(f"SQL: SELECT date, open, close, high, low, volume, amount FROM stock_kline_data WHERE code = '{code}' ORDER BY date DESC LIMIT {limit}")
        with get_db_conn() as conn:
            cursor = conn.cursor()
            cursor.execute(
                '''SELECT date, open, close, high, low, volume, amount
                   FROM stock_kline_data
                   WHERE code = %s
                   ORDER BY date DESC LIMIT %s''',
                (code, limit)
            )
            rows = cursor.fetchall()
            logger.info(f"SQL: 查询返回 {len(rows)} 条记录")

            if rows:
                # 将 RealDictRow 转换为字典列表
                data = [dict(row) for row in rows]
                df = pd.DataFrame(data)
                df.columns = ['日期', '开盘', '收盘', '最高', '最低', 'volume', 'amount']
                return df.iloc[::-1]  # 反转回正序
            return None

    @staticmethod
    def get_kline_objects(code, limit=250):
        """获取K线数据（返回KlineData对象列表）"""
        with get_db_conn() as conn:
            cursor = conn.cursor()
            cursor.execute(
                '''SELECT id, code, date, open, close, high, low, volume, amount, created_at, updated_at
                   FROM stock_kline_data
                   WHERE code = %s
                   ORDER BY date DESC LIMIT %s''',
                (code, limit)
            )
            rows = cursor.fetchall()

            if rows:
                from models.kline_data import KlineData
                return [
                    KlineData(
                        id=row[0],
                        code=row[1],
                        date=row[2],
                        open=row[3],
                        close=row[4],
                        high=row[5],
                        low=row[6],
                        volume=row[7],
                        amount=row[8],
                        created_at=row[9],
                        updated_at=row[10]
                    )
                    for row in rows[::-1]  # 反转回正序
                ]
            return None

    @staticmethod
    def get_latest_date(code):
        """获取最新K线日期"""
        logger.info(f"SQL: SELECT MAX(date) FROM stock_kline_data WHERE code = '{code}'")
        with get_db_conn() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT MAX(date) FROM stock_kline_data WHERE code = %s', (code,))
            result = cursor.fetchone()
            if result:
                latest_date = result['max'] if isinstance(result, dict) else result[0]
                logger.info(f"SQL: 查询返回最新日期: {latest_date}")
                return latest_date
            logger.info("SQL: 查询返回 None")
            return None

    @staticmethod
    def get_need_update(days=1):
        """获取需要更新K线的股票"""
        from repositories.monitor_repository import MonitorStockRepository
        stocks = MonitorStockRepository.get_enabled()
        codes = [s.code for s in stocks]

        need_update = []
        for code in codes:
            latest = KlineRepository.get_latest_date(code)
            if not latest or (datetime.now() - datetime.strptime(latest, '%Y-%m-%d')).days >= days:
                need_update.append(code)

        return need_update

    @staticmethod
    def has_updated_today():
        """检查今天是否已更新"""
        with get_db_conn() as conn:
            cursor = conn.cursor()
            today = datetime.now().strftime('%Y-%m-%d')
            cursor.execute(
                "SELECT status FROM kline_update_log WHERE update_date = %s AND status = 'success'",
                (today,)
            )
            return cursor.fetchone() is not None

    @staticmethod
    def record_update(success_count, total_count, status='success'):
        """记录更新日志"""
        with get_db_conn() as conn:
            cursor = conn.cursor()
            today = datetime.now().strftime('%Y-%m-%d')
            try:
                cursor.execute(
                    '''INSERT INTO kline_update_log
                       (update_date, success_count, total_count, status, created_at)
                       VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP)
                       ON CONFLICT (update_date) DO UPDATE
                       SET success_count = EXCLUDED.success_count,
                           total_count = EXCLUDED.total_count,
                           status = EXCLUDED.status''',
                    (today, success_count, total_count, status)
                )
                conn.commit()
                return True
            except Exception as e:
                logger.error(f"记录更新日志失败: {e}")
                return False

    @staticmethod
    def get_last_update_info():
        """获取最近一次更新信息"""
        with get_db_conn() as conn:
            cursor = conn.cursor()
            cursor.execute(
                '''SELECT update_date, success_count, total_count, status, created_at
                   FROM kline_update_log
                   ORDER BY update_date DESC LIMIT 1'''
            )
            return cursor.fetchone()

    @staticmethod
    def export_kline_data(code, start_date=None, end_date=None):
        """获取K线数据用于导出

        Args:
            code: 股票代码
            start_date: 开始日期 (格式: YYYY-MM-DD)，None表示不限制
            end_date: 结束日期 (格式: YYYY-MM-DD)，None表示不限制

        Returns:
            DataFrame: 包含K线数据的DataFrame，列名包括：日期、开盘、收盘、最高、最低、成交量、成交额
        """
        with get_db_conn() as conn:
            cursor = conn.cursor()

            # 构建查询条件
            if start_date and end_date:
                cursor.execute(
                    '''SELECT date, open, close, high, low, volume, amount
                       FROM stock_kline_data
                       WHERE code = %s AND date >= %s AND date <= %s
                       ORDER BY date ASC''',
                    (code, start_date, end_date)
                )
            elif start_date:
                cursor.execute(
                    '''SELECT date, open, close, high, low, volume, amount
                       FROM stock_kline_data
                       WHERE code = %s AND date >= %s
                       ORDER BY date ASC''',
                    (code, start_date)
                )
            elif end_date:
                cursor.execute(
                    '''SELECT date, open, close, high, low, volume, amount
                       FROM stock_kline_data
                       WHERE code = %s AND date <= %s
                       ORDER BY date ASC''',
                    (code, end_date)
                )
            else:
                cursor.execute(
                    '''SELECT date, open, close, high, low, volume, amount
                       FROM stock_kline_data
                       WHERE code = %s
                       ORDER BY date ASC''',
                    (code,)
                )

            rows = cursor.fetchall()

            if rows:
                df = pd.DataFrame(rows, columns=['日期', '开盘', '收盘', '最高', '最低', '成交量', '成交额'])
                return df
            return None