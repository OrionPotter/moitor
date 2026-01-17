from utils.db import get_db_conn
from utils.logger import get_logger
from datetime import datetime
import pandas as pd

logger = get_logger('kline_repository')


class KlineRepository:
    """K线数据仓储层（异步版本）"""

    @staticmethod
    async def save_batch(code, kline_data):
        """批量保存K线数据"""
        logger.info(f"SQL: 批量插入/更新 {code} 的 K线数据，数据量: {len(kline_data)}")
        async with get_db_conn() as conn:
            try:
                insert_data = [
                    (code, row['日期'].strftime('%Y-%m-%d'), row['开盘'], row['收盘'],
                     row['最高'], row['最低'], 0, row.get('amount', 0))
                    for _, row in kline_data.iterrows()
                ]
                
                await conn.executemany(
                    '''INSERT INTO stock_kline_data
                       (code, date, open, close, high, low, volume, amount, updated_at)
                       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, CURRENT_TIMESTAMP)
                       ON CONFLICT (code, date) DO UPDATE
                       SET open = EXCLUDED.open, close = EXCLUDED.close, high = EXCLUDED.high,
                           low = EXCLUDED.low, volume = EXCLUDED.volume, amount = EXCLUDED.amount,
                           updated_at = CURRENT_TIMESTAMP''',
                    insert_data
                )
                logger.info(f"SQL: 批量插入/更新成功")
                return True, len(insert_data)
            except Exception as e:
                logger.error(f"SQL: 批量插入/更新失败: {str(e)}")
                return False, str(e)

    @staticmethod
    async def save_all_batch(kline_data_dict):
        """批量保存多只股票的K线数据
        
        Args:
            kline_data_dict: {code: DataFrame} 的字典
            
        Returns:
            tuple: (success_count, total_count, total_records)
        """
        if not kline_data_dict:
            return 0, 0, 0
        
        logger.info(f"SQL: 批量保存 {len(kline_data_dict)} 只股票的K线数据")
        
        async with get_db_conn() as conn:
            try:
                all_insert_data = []
                total_records = 0
                saved_count = 0
                
                for code, df in kline_data_dict.items():
                    if df is None or df.empty:
                        continue
                    
                    insert_data = [
                        (code, row['日期'].strftime('%Y-%m-%d'), row['开盘'], row['收盘'],
                         row['最高'], row['最低'], 0, row.get('amount', 0))
                        for _, row in df.iterrows()
                    ]
                    all_insert_data.extend(insert_data)
                    total_records += len(insert_data)
                    saved_count += 1
                
                if not all_insert_data:
                    return 0, len(kline_data_dict), 0
                
                await conn.executemany(
                    '''INSERT INTO stock_kline_data
                       (code, date, open, close, high, low, volume, amount, updated_at)
                       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, CURRENT_TIMESTAMP)
                       ON CONFLICT (code, date) DO UPDATE
                       SET open = EXCLUDED.open, close = EXCLUDED.close, high = EXCLUDED.high,
                           low = EXCLUDED.low, volume = EXCLUDED.volume, amount = EXCLUDED.amount,
                           updated_at = CURRENT_TIMESTAMP''',
                    all_insert_data
                )
                logger.info(f"SQL: 批量保存成功，{saved_count} 只股票，{total_records} 条记录")
                return saved_count, len(kline_data_dict), total_records
            except Exception as e:
                logger.error(f"SQL: 批量保存失败: {str(e)}")
                return 0, len(kline_data_dict), 0

    @staticmethod
    async def get_batch_by_codes(codes, limit=250):
        """批量获取多只股票的K线数据

        Args:
            codes: 股票代码列表
            limit: 每只股票返回的最大记录数

        Returns:
            dict: {code: DataFrame} 的字典
        """
        if not codes:
            return {}

        logger.info(f"SQL: 批量查询 {len(codes)} 只股票的K线数据，每只最多 {limit} 条")

        async with get_db_conn() as conn:
            # 使用 IN 子句批量查询
            rows = await conn.fetch(
                '''SELECT code, date, open, close, high, low, volume, amount
                   FROM stock_kline_data
                   WHERE code = ANY($1)
                   ORDER BY code, date DESC''',
                codes
            )

            logger.info(f"SQL: 批量查询返回 {len(rows)} 条记录")

            # 按 code 分组数据
            code_data = {}
            for row in rows:
                code = row['code']
                if code not in code_data:
                    code_data[code] = []
                code_data[code].append({
                    'date': row['date'],
                    'open': row['open'],
                    'close': row['close'],
                    'high': row['high'],
                    'low': row['low'],
                    'volume': row['volume'],
                    'amount': row['amount']
                })

            # 转换为 DataFrame 并限制数量
            result = {}
            for code in codes:
                if code in code_data and code_data[code]:
                    data = code_data[code][:limit]  # 限制数量
                    df = pd.DataFrame(data)
                    df.columns = ['日期', '开盘', '收盘', '最高', '最低', 'volume', 'amount']
                    result[code] = df.iloc[::-1]  # 反转回正序
                else:
                    result[code] = None

            return result

    @staticmethod
    async def get_by_code(code, limit=250):
        """获取K线数据（返回DataFrame）"""
        logger.debug(f"SQL: SELECT date, open, close, high, low, volume, amount FROM stock_kline_data WHERE code = '{code}' ORDER BY date DESC LIMIT {limit}")
        async with get_db_conn() as conn:
            rows = await conn.fetch(
                '''SELECT date, open, close, high, low, volume, amount
                   FROM stock_kline_data
                   WHERE code = $1
                   ORDER BY date DESC LIMIT $2''',
                code, limit
            )
            logger.debug(f"SQL: 查询返回 {len(rows)} 条记录")

            if rows:
                data = [dict(row) for row in rows]
                df = pd.DataFrame(data)
                df.columns = ['日期', '开盘', '收盘', '最高', '最低', 'volume', 'amount']
                return df.iloc[::-1]  # 反转回正序
            return None

    @staticmethod
    async def get_kline_objects(code, limit=250):
        """获取K线数据（返回KlineData对象列表）"""
        async with get_db_conn() as conn:
            rows = await conn.fetch(
                '''SELECT id, code, date, open, close, high, low, volume, amount, created_at, updated_at
                   FROM stock_kline_data
                   WHERE code = $1
                   ORDER BY date DESC LIMIT $2''',
                code, limit
            )

            if rows:
                from models.kline_data import KlineData
                return [
                    KlineData(
                        id=row['id'],
                        code=row['code'],
                        date=row['date'],
                        open=row['open'],
                        close=row['close'],
                        high=row['high'],
                        low=row['low'],
                        volume=row['volume'],
                        amount=row['amount'],
                        created_at=row['created_at'],
                        updated_at=row['updated_at']
                    )
                    for row in rows[::-1]  # 反转回正序
                ]
            return None

    @staticmethod
    async def get_latest_date(code):
        """获取最新K线日期"""
        logger.debug(f"SQL: SELECT MAX(date) FROM stock_kline_data WHERE code = '{code}'")
        async with get_db_conn() as conn:
            result = await conn.fetchval(
                'SELECT MAX(date) FROM stock_kline_data WHERE code = $1',
                code
            )
            logger.debug(f"SQL: 查询返回最新日期: {result}")
            return result

    @staticmethod
    async def get_latest_dates_batch(codes):
        """批量获取多只股票的最新K线日期
        
        Args:
            codes: 股票代码列表
            
        Returns:
            dict: {code: latest_date} 的字典
        """
        if not codes:
            return {}
        
        logger.debug(f"SQL: 批量查询 {len(codes)} 只股票的最新日期")
        
        async with get_db_conn() as conn:
            # 使用 ANY 子句批量查询
            results = await conn.fetch(
                '''SELECT code, MAX(date) as max_date 
                   FROM stock_kline_data 
                   WHERE code = ANY($1) 
                   GROUP BY code''',
                codes
            )
            
            # 构建结果字典
            latest_dates = {row['code']: row['max_date'] for row in results}
            
            # 为没有数据的股票返回 None
            for code in codes:
                if code not in latest_dates:
                    latest_dates[code] = None
            
            logger.debug(f"SQL: 批量查询完成，返回 {len([v for v in latest_dates.values() if v is not None])} 条有效记录")
            return latest_dates

    @staticmethod
    async def get_need_update(days=1):
        """获取需要更新K线的股票"""
        from repositories.monitor_repository import MonitorStockRepository
        stocks = await MonitorStockRepository.get_enabled()
        codes = [s.code for s in stocks]

        if not codes:
            return []

        # 使用批量查询一次性获取所有股票的最新日期
        latest_dates_dict = await KlineRepository.get_latest_dates_batch(codes)
        
        # 过滤出需要更新的股票
        need_update = []
        now = datetime.now()
        
        for code in codes:
            latest = latest_dates_dict.get(code)
            if not latest:
                need_update.append(code)
            else:
                latest_dt = datetime.strptime(latest, '%Y-%m-%d')
                if (now - latest_dt).days >= days:
                    need_update.append(code)
        
        logger.info(f"SQL: 筛选出 {len(need_update)} 只股票需要更新")
        return need_update

    @staticmethod
    async def has_updated_today():
        """检查今天是否已更新"""
        async with get_db_conn() as conn:
            today = datetime.now().strftime('%Y-%m-%d')
            result = await conn.fetchval(
                "SELECT status FROM kline_update_log WHERE update_date = $1 AND status = 'success'",
                today
            )
            return result is not None

    @staticmethod
    async def record_update(success_count, total_count, status='success'):
        """记录更新日志"""
        async with get_db_conn() as conn:
            today = datetime.now().strftime('%Y-%m-%d')
            try:
                await conn.execute(
                    '''INSERT INTO kline_update_log
                       (update_date, success_count, total_count, status, created_at)
                       VALUES ($1, $2, $3, $4, CURRENT_TIMESTAMP)
                       ON CONFLICT (update_date) DO UPDATE
                       SET success_count = EXCLUDED.success_count,
                           total_count = EXCLUDED.total_count,
                           status = EXCLUDED.status''',
                    today, success_count, total_count, status
                )
                return True
            except Exception as e:
                logger.error(f"记录更新日志失败: {e}")
                return False

    @staticmethod
    async def get_last_update_info():
        """获取最近一次更新信息"""
        async with get_db_conn() as conn:
            row = await conn.fetchrow(
                '''SELECT update_date, success_count, total_count, status, created_at
                   FROM kline_update_log
                   ORDER BY update_date DESC LIMIT 1'''
            )
            return row

    @staticmethod
    async def export_kline_data(code, start_date=None, end_date=None):
        """获取K线数据用于导出

        Args:
            code: 股票代码
            start_date: 开始日期 (格式: YYYY-MM-DD)，None表示不限制
            end_date: 结束日期 (格式: YYYY-MM-DD)，None表示不限制

        Returns:
            DataFrame: 包含K线数据的DataFrame，列名包括：日期、开盘、收盘、最高、最低、成交量、成交额
        """
        async with get_db_conn() as conn:
            # 构建查询条件
            if start_date and end_date:
                rows = await conn.fetch(
                    '''SELECT date, open, close, high, low, volume, amount
                       FROM stock_kline_data
                       WHERE code = $1 AND date >= $2 AND date <= $3
                       ORDER BY date ASC''',
                    code, start_date, end_date
                )
            elif start_date:
                rows = await conn.fetch(
                    '''SELECT date, open, close, high, low, volume, amount
                       FROM stock_kline_data
                       WHERE code = $1 AND date >= $2
                       ORDER BY date ASC''',
                    code, start_date
                )
            elif end_date:
                rows = await conn.fetch(
                    '''SELECT date, open, close, high, low, volume, amount
                       FROM stock_kline_data
                       WHERE code = $1 AND date <= $2
                       ORDER BY date ASC''',
                    code, end_date
                )
            else:
                rows = await conn.fetch(
                    '''SELECT date, open, close, high, low, volume, amount
                       FROM stock_kline_data
                       WHERE code = $1
                       ORDER BY date ASC''',
                    code
                )

            if rows:
                df = pd.DataFrame(rows, columns=['日期', '开盘', '收盘', '最高', '最低', '成交量', '成交额'])
                return df
            return None