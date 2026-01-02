# repositories/cache_repository.py
from utils.db import get_db_conn, logger
from datetime import datetime, timezone


class MonitorDataCacheRepository:
    """监控数据缓存仓储层"""

    @staticmethod
    def save(code, timeframe, current_price, ema144, ema188,
             ema5=None, ema10=None, ema20=None, ema30=None, ema60=None,
             ema7=None, ema21=None, ema42=None, eps_forecast=None):
        """保存或更新监控缓存数据"""
        with get_db_conn() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    '''INSERT OR REPLACE INTO monitor_data_cache
                       (code, timeframe, current_price, ema144, ema188, ema5, ema10, ema20,
                        ema30, ema60, ema7, ema21, ema42, eps_forecast, created_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)''',
                    (code, timeframe, current_price, ema144, ema188, ema5, ema10, ema20,
                     ema30, ema60, ema7, ema21, ema42, eps_forecast)
                )
                conn.commit()
                return True
            except Exception as e:
                logger.error(f"保存监控缓存失败: {e}")
                return False

    @staticmethod
    def get_by_code_and_timeframe(code, timeframe, max_age_minutes=5):
        """获取缓存数据（检查时间有效性）"""
        from models.monitor_data_cache import MonitorDataCache

        with get_db_conn() as conn:
            cursor = conn.cursor()
            cursor.execute(
                '''SELECT id, code, timeframe, current_price, ema144, ema188,
                          ema5, ema10, ema20, ema30, ema60, ema7, ema21, ema42, eps_forecast, created_at
                   FROM monitor_data_cache
                   WHERE code = ? AND timeframe = ?
                   ORDER BY created_at DESC LIMIT 1''',
                (code, timeframe)
            )
            result = cursor.fetchone()

            if result:
                created_at_str = result[15]
                if created_at_str:
                    try:
                        created_at = datetime.strptime(created_at_str, '%Y-%m-%d %H:%M:%S')
                        # 缓存时间是UTC，需要转换为本地时间（+8小时）
                        created_at = created_at.replace(tzinfo=timezone.utc).astimezone()
                        now = datetime.now(timezone.utc).astimezone()
                        age_minutes = (now - created_at).total_seconds() / 60

                        if age_minutes > max_age_minutes:
                            return None
                    except Exception as e:
                        logger.error(f"解析缓存时间失败: {e}")
                        return None

                return MonitorDataCache(
                    id=result[0],
                    code=result[1],
                    timeframe=result[2],
                    current_price=result[3],
                    ema144=result[4],
                    ema188=result[5],
                    ema5=result[6],
                    ema10=result[7],
                    ema20=result[8],
                    ema30=result[9],
                    ema60=result[10],
                    ema7=result[11],
                    ema21=result[12],
                    ema42=result[13],
                    eps_forecast=result[14],
                    created_at=datetime.strptime(result[15], '%Y-%m-%d %H:%M:%S') if result[15] else None
                )

            return None

    @staticmethod
    def clean_old_data(hours=1):
        """清理过期数据"""
        with get_db_conn() as conn:
            cursor = conn.cursor()
            cursor.execute(
                f"DELETE FROM monitor_data_cache WHERE datetime(created_at) < datetime('now', '-{hours} hours')"
            )
            conn.commit()
            return cursor.rowcount