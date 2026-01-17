# repositories/cache_repository.py
from utils.db import get_db_conn
from utils.logger import get_logger
from datetime import datetime, timezone

logger = get_logger('cache_repository')


class MonitorDataCacheRepository:
    """监控数据缓存仓储层"""

    @staticmethod
    def save(code, timeframe, current_price, ema144, ema188,
             ema5=None, ema10=None, ema20=None, ema30=None, ema60=None,
             ema7=None, ema21=None, ema42=None, eps_forecast=None):
        """保存或更新监控缓存数据"""
        logger.info(f"SQL: INSERT/UPDATE monitor_data_cache: code={code}, timeframe={timeframe}, current_price={current_price}")
        with get_db_conn() as conn:
            cursor = conn.cursor()
            try:
                # 转换 numpy 类型为 Python 原生类型
                def convert_value(val):
                    if val is None:
                        return None
                    if hasattr(val, 'item'):
                        return val.item()
                    return float(val) if isinstance(val, (int, float)) and not isinstance(val, bool) else val

                cursor.execute(
                    '''INSERT INTO monitor_data_cache
                       (code, timeframe, current_price, ema144, ema188, ema5, ema10, ema20,
                        ema30, ema60, ema7, ema21, ema42, eps_forecast, created_at)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
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
                    (code, timeframe,
                     convert_value(current_price),
                     convert_value(ema144),
                     convert_value(ema188),
                     convert_value(ema5),
                     convert_value(ema10),
                     convert_value(ema20),
                     convert_value(ema30),
                     convert_value(ema60),
                     convert_value(ema7),
                     convert_value(ema21),
                     convert_value(ema42),
                     convert_value(eps_forecast))
                )
                conn.commit()
                logger.info(f"SQL: 缓存保存成功，影响行数: {cursor.rowcount}")
                return True
            except Exception as e:
                logger.error(f"SQL: 缓存保存失败: {e}")
                return False

    @staticmethod
    def get_by_code_and_timeframe(code, timeframe, max_age_minutes=5):
        """获取缓存数据（检查时间有效性）"""
        logger.info(f"SQL: SELECT * FROM monitor_data_cache WHERE code='{code}' AND timeframe='{timeframe}' ORDER BY created_at DESC LIMIT 1")
        from models.monitor_data_cache import MonitorDataCache

        with get_db_conn() as conn:
            cursor = conn.cursor()
            cursor.execute(
                '''SELECT id, code, timeframe, current_price, ema144, ema188,
                          ema5, ema10, ema20, ema30, ema60, ema7, ema21, ema42, eps_forecast, created_at
                   FROM monitor_data_cache
                   WHERE code = %s AND timeframe = %s
                   ORDER BY created_at DESC LIMIT 1''',
                (code, timeframe)
            )
            result = cursor.fetchone()
            logger.info(f"SQL: 查询返回 {'1' if result else '0'} 条记录")

            if result:
                created_at = result['created_at']
                if created_at:
                    try:
                        now = datetime.now()
                        age_minutes = (now - created_at).total_seconds() / 60

                        if age_minutes > max_age_minutes:
                            return None
                    except Exception as e:
                        logger.error(f"解析缓存时间失败: {e}")
                        return None

                return MonitorDataCache(
                    id=result['id'],
                    code=result['code'],
                    timeframe=result['timeframe'],
                    current_price=result['current_price'],
                    ema144=result['ema144'],
                    ema188=result['ema188'],
                    ema5=result['ema5'],
                    ema10=result['ema10'],
                    ema20=result['ema20'],
                    ema30=result['ema30'],
                    ema60=result['ema60'],
                    ema7=result['ema7'],
                    ema21=result['ema21'],
                    ema42=result['ema42'],
                    eps_forecast=result['eps_forecast'],
                    created_at=result['created_at']
                )

            return None

    @staticmethod
    def clean_old_data(hours=1):
        """清理过期数据"""
        with get_db_conn() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "DELETE FROM monitor_data_cache WHERE created_at < NOW() - INTERVAL '%s hours'",
                (hours,)
            )
            conn.commit()
            return cursor.rowcount