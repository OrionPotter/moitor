# repositories/cache_repository.py
from utils.db import get_db_conn
from utils.logger import get_logger
from datetime import datetime, timezone

logger = get_logger('cache_repository')


class MonitorDataCacheRepository:
    """监控数据缓存仓储层（异步版本）"""

    @staticmethod
    def convert_value(val):
        """转换 numpy 类型为 Python 原生类型"""
        if val is None:
            return None
        if hasattr(val, 'item'):
            return val.item()
        return float(val) if isinstance(val, (int, float)) and not isinstance(val, bool) else val

    @staticmethod
    async def save_batch(cache_data_list):
        """批量保存或更新监控缓存数据

        Args:
            cache_data_list: 列表，每个元素是字典，包含:
                {
                    'code': str,
                    'timeframe': str,
                    'current_price': float,
                    'ema144': float,
                    'ema188': float,
                    'ema5': float,
                    'ema10': float,
                    'ema20': float,
                    'ema30': float,
                    'ema60': float,
                    'ema7': float,
                    'ema21': float,
                    'ema42': float,
                    'eps_forecast': float
                }
        """
        if not cache_data_list:
            return True

        logger.info(f"SQL: 批量插入/更新 {len(cache_data_list)} 条缓存数据")

        async with get_db_conn() as conn:
            try:
                # 准备所有数据
                all_values = []
                for data in cache_data_list:
                    all_values.append((
                        data['code'],
                        data['timeframe'],
                        MonitorDataCacheRepository.convert_value(data['current_price']),
                        MonitorDataCacheRepository.convert_value(data['ema144']),
                        MonitorDataCacheRepository.convert_value(data['ema188']),
                        MonitorDataCacheRepository.convert_value(data['ema5']),
                        MonitorDataCacheRepository.convert_value(data['ema10']),
                        MonitorDataCacheRepository.convert_value(data['ema20']),
                        MonitorDataCacheRepository.convert_value(data['ema30']),
                        MonitorDataCacheRepository.convert_value(data['ema60']),
                        MonitorDataCacheRepository.convert_value(data['ema7']),
                        MonitorDataCacheRepository.convert_value(data['ema21']),
                        MonitorDataCacheRepository.convert_value(data['ema42']),
                        MonitorDataCacheRepository.convert_value(data['eps_forecast'])
                    ))

                # 批量执行
                await conn.executemany(
                    '''INSERT INTO monitor_data_cache
                       (code, timeframe, current_price, ema144, ema188, ema5, ema10, ema20,
                        ema30, ema60, ema7, ema21, ema42, eps_forecast, created_at)
                       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, CURRENT_TIMESTAMP)
                       ON CONFLICT (code, timeframe) DO UPDATE
                       SET current_price = EXCLUDED.current_price,
                           ema144 = EXCLUDED.ema144,
                           ema188 = EXCLUDED.ema188,
                           ema5 = EXCLUDED.ema5,
                           ema10 = EXCLUDED.ema10,
                           ema20 = EXCLUDED.ema20,
                           ema30 = EXCLUDED.ema30,
                           ema60 = EXCLUDED.ema60,
                           ema7 = EXCLUDED.ema7,
                           ema21 = EXCLUDED.ema21,
                           ema42 = EXCLUDED.ema42,
                           eps_forecast = EXCLUDED.eps_forecast,
                           created_at = CURRENT_TIMESTAMP''',
                    all_values
                )
                logger.info(f"SQL: 批量保存成功，{len(cache_data_list)} 条记录")
                return True
            except Exception as e:
                logger.error(f"SQL: 批量保存失败: {e}")
                return False

    @staticmethod
    async def save(code, timeframe, current_price, ema144, ema188,
             ema5=None, ema10=None, ema20=None, ema30=None, ema60=None,
             ema7=None, ema21=None, ema42=None, eps_forecast=None):
        """保存或更新监控缓存数据（单条，用于向后兼容）"""
        cache_data = {
            'code': code,
            'timeframe': timeframe,
            'current_price': current_price,
            'ema144': ema144,
            'ema188': ema188,
            'ema5': ema5,
            'ema10': ema10,
            'ema20': ema20,
            'ema30': ema30,
            'ema60': ema60,
            'ema7': ema7,
            'ema21': ema21,
            'ema42': ema42,
            'eps_forecast': eps_forecast
        }
        return await MonitorDataCacheRepository.save_batch([cache_data])

    @staticmethod
    async def get_batch_by_code_and_timeframe(code_timeframe_pairs, max_age_minutes=5):
        """批量获取缓存数据

        Args:
            code_timeframe_pairs: 列表，每个元素是 (code, timeframe) 元组
            max_age_minutes: 缓存最大有效期（分钟）

        Returns:
            dict: {(code, timeframe): MonitorDataCache}
        """
        if not code_timeframe_pairs:
            return {}

        logger.info(f"SQL: 批量查询 {len(code_timeframe_pairs)} 条缓存数据")
        from models.monitor_data_cache import MonitorDataCache

        async with get_db_conn() as conn:
            # 使用 IN 子句批量查询
            codes = [pair[0] for pair in code_timeframe_pairs]
            timeframes = [pair[1] for pair in code_timeframe_pairs]

            # 查询所有匹配的记录
            rows = await conn.fetch(
                '''SELECT id, code, timeframe, current_price, ema144, ema188,
                          ema5, ema10, ema20, ema30, ema60, ema7, ema21, ema42, eps_forecast, created_at
                   FROM monitor_data_cache
                   WHERE code = ANY($1) AND timeframe = ANY($2)
                   ORDER BY code, timeframe, created_at DESC''',
                codes, timeframes
            )

            logger.info(f"SQL: 批量查询返回 {len(rows)} 条记录")

            # 构建结果字典，只保留最新的记录
            result = {}
            now = datetime.now()

            for row in rows:
                key = (row['code'], row['timeframe'])

                # 检查时间有效性
                created_at = row['created_at']
                if created_at:
                    age_minutes = (now - created_at).total_seconds() / 60
                    if age_minutes > max_age_minutes:
                        continue

                # 如果该 key 还没有记录，或者当前记录更新，则保存
                if key not in result or row['created_at'] > result[key].created_at:
                    result[key] = MonitorDataCache(
                        id=row['id'],
                        code=row['code'],
                        timeframe=row['timeframe'],
                        current_price=row['current_price'],
                        ema144=row['ema144'],
                        ema188=row['ema188'],
                        ema5=row['ema5'],
                        ema10=row['ema10'],
                        ema20=row['ema20'],
                        ema30=row['ema30'],
                        ema60=row['ema60'],
                        ema7=row['ema7'],
                        ema21=row['ema21'],
                        ema42=row['ema42'],
                        eps_forecast=row['eps_forecast'],
                        created_at=row['created_at']
                    )

            return result

    @staticmethod
    async def get_by_code_and_timeframe(code, timeframe, max_age_minutes=5):
        """获取缓存数据（单条，用于向后兼容）"""
        result = await MonitorDataCacheRepository.get_batch_by_code_and_timeframe(
            [(code, timeframe)], max_age_minutes
        )
        return result.get((code, timeframe))

    @staticmethod
    async def clean_old_data(hours=1):
        """清理过期数据"""
        async with get_db_conn() as conn:
            result = await conn.execute(
                "DELETE FROM monitor_data_cache WHERE created_at < NOW() - INTERVAL '1 hour' * $1",
                hours
            )
            return int(result.split()[-1])