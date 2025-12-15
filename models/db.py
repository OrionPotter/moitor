# models/db.py
import sqlite3
from contextlib import contextmanager
from datetime import datetime
import os
import pandas as pd
from utils.logger import get_logger

# 获取日志实例
logger = get_logger('db')

DB_PATH = 'portfolio.db'

@contextmanager
def get_db_conn():
    """上下文管理器：自动管理数据库连接"""
    conn = sqlite3.connect(DB_PATH)
    try:
        yield conn
    finally:
        conn. close()


def init_db():
    """初始化数据库"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # 创建股票持仓表
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS portfolio (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code TEXT UNIQUE NOT NULL,
            name TEXT NOT NULL,
            cost_price REAL NOT NULL,
            shares INTEGER NOT NULL
        )
    ''')
    
    # 创建监控股票表
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS monitor_stocks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code TEXT UNIQUE NOT NULL,
            name TEXT NOT NULL,
            timeframe TEXT NOT NULL,
            reasonable_pe_min REAL DEFAULT 15,
            reasonable_pe_max REAL DEFAULT 20,
            enabled INTEGER DEFAULT 1,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # 创建监控数据缓存表
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS monitor_data_cache (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code TEXT NOT NULL,
            timeframe TEXT NOT NULL,
            current_price REAL NOT NULL,
            ema144 REAL NOT NULL,
            ema188 REAL NOT NULL,
            ema5 REAL,
            ema10 REAL,
            ema20 REAL,
            ema30 REAL,
            ema60 REAL,
            ema7 REAL,
            ema21 REAL,
            ema42 REAL,
            eps_forecast REAL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(code, timeframe)
        )
    ''')
    
    # 创建K线数据表
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS stock_kline_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code TEXT NOT NULL,
            date TEXT NOT NULL,
            open REAL NOT NULL,
            close REAL NOT NULL,
            high REAL NOT NULL,
            low REAL NOT NULL,
            volume INTEGER NOT NULL,
            amount REAL NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(code, date)
        )
    ''')
    
    # 创建索引
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_portfolio_code ON portfolio(code)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_monitor_code ON monitor_stocks(code)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_monitor_enabled ON monitor_stocks(enabled)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_monitor_cache_code ON monitor_data_cache(code)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_monitor_cache_timeframe ON monitor_data_cache(timeframe)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_monitor_cache_created ON monitor_data_cache(created_at)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_kline_code ON stock_kline_data(code)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_kline_code_date ON stock_kline_data(code, date)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_kline_date ON stock_kline_data(date)')
    
    conn.commit()
    conn.close()


class StockRepository:
    """股票持仓数据仓储层"""
    
    @staticmethod
    def get_all():
        """获取所有股票"""
        with get_db_conn() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT id, code, name, cost_price, shares FROM portfolio ORDER BY code')
            return cursor. fetchall()
    
    @staticmethod
    def get_by_code(code):
        """根据代码获取单只股票"""
        with get_db_conn() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT id, code, name, cost_price, shares FROM portfolio WHERE code = ? ', (code,))
            return cursor.fetchone()
    
    @staticmethod
    def add(code, name, cost_price, shares):
        """添加股票"""
        with get_db_conn() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    'INSERT INTO portfolio (code, name, cost_price, shares) VALUES (?, ?, ?, ?)',
                    (code, name, cost_price, shares)
                )
                conn.commit()
                return True, "添加成功"
            except sqlite3.IntegrityError:
                return False, "股票代码已存在"
            except Exception as e:
                return False, str(e)
    
    @staticmethod
    def update(code, name, cost_price, shares):
        """更新股票"""
        with get_db_conn() as conn:
            cursor = conn.cursor()
            cursor.execute(
                'UPDATE portfolio SET name = ?, cost_price = ?, shares = ? WHERE code = ?',
                (name, cost_price, shares, code)
            )
            conn.commit()
            return cursor.rowcount > 0
    
    @staticmethod
    def delete(code):
        """删除股票"""
        with get_db_conn() as conn:
            cursor = conn.cursor()
            cursor.execute('DELETE FROM portfolio WHERE code = ?', (code,))
            conn.commit()
            return cursor.rowcount > 0


class MonitorStockRepository: 
    """监控股票数据仓储层"""
    
    @staticmethod
    def get_all():
        """获取所有监控股票"""
        with get_db_conn() as conn:
            cursor = conn.cursor()
            cursor. execute('SELECT * FROM monitor_stocks ORDER BY code')
            return cursor.fetchall()
    
    @staticmethod
    def get_enabled():
        """获取所有启用的监控股票"""
        with get_db_conn() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM monitor_stocks WHERE enabled = 1 ORDER BY code')
            return cursor. fetchall()
    
    @staticmethod
    def get_by_code(code):
        """根据代码获取监控股票"""
        with get_db_conn() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM monitor_stocks WHERE code = ? ', (code,))
            return cursor.fetchone()
    
    @staticmethod
    def add(code, name, timeframe, reasonable_pe_min=15, reasonable_pe_max=20):
        """添加监控股票"""
        with get_db_conn() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    '''INSERT INTO monitor_stocks 
                       (code, name, timeframe, reasonable_pe_min, reasonable_pe_max)
                       VALUES (?, ?, ?, ?, ?)''',
                    (code, name, timeframe, reasonable_pe_min, reasonable_pe_max)
                )
                conn.commit()
                return True, "添加成功"
            except sqlite3.IntegrityError:
                return False, "监控股票已存在"
            except Exception as e:
                return False, str(e)
    
    @staticmethod
    def update(code, name, timeframe, reasonable_pe_min, reasonable_pe_max):
        """更新监控股票"""
        with get_db_conn() as conn:
            cursor = conn.cursor()
            cursor. execute(
                '''UPDATE monitor_stocks 
                   SET name = ?, timeframe = ?, reasonable_pe_min = ?, 
                       reasonable_pe_max = ?, updated_at = CURRENT_TIMESTAMP
                   WHERE code = ?''',
                (name, timeframe, reasonable_pe_min, reasonable_pe_max, code)
            )
            conn.commit()
            return cursor.rowcount > 0
    
    @staticmethod
    def delete(code):
        """删除监控股票"""
        with get_db_conn() as conn:
            cursor = conn.cursor()
            cursor.execute('DELETE FROM monitor_stocks WHERE code = ? ', (code,))
            conn.commit()
            return cursor. rowcount > 0
    
    @staticmethod
    def toggle_enabled(code, enabled):
        """启用/禁用监控股票"""
        with get_db_conn() as conn:
            cursor = conn.cursor()
            cursor.execute(
                'UPDATE monitor_stocks SET enabled = ?, updated_at = CURRENT_TIMESTAMP WHERE code = ?',
                (int(enabled), code)
            )
            conn.commit()
            return cursor.rowcount > 0


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
                        from datetime import timedelta, timezone
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
            
            return result
    
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


class KlineRepository:
    """K线数据仓储层"""
    
    @staticmethod
    def save_batch(code, kline_data):
        """批量保存K线数据"""
        with get_db_conn() as conn:
            cursor = conn.cursor()
            try:
                insert_data = [
                    (code, row['日期'], row['开盘'], row['收盘'], 
                     row['最高'], row['最低'], 0, row. get('amount', 0))
                    for _, row in kline_data. iterrows()
                ]
                cursor.executemany(
                    '''INSERT OR REPLACE INTO stock_kline_data 
                       (code, date, open, close, high, low, volume, amount, updated_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)''',
                    insert_data
                )
                conn.commit()
                return True, len(insert_data)
            except Exception as e:
                return False, str(e)
    
    @staticmethod
    def get_by_code(code, limit=250):
        """获取K线数据"""
        with get_db_conn() as conn:
            cursor = conn.cursor()
            cursor.execute(
                '''SELECT date, open, close, high, low, volume, amount
                   FROM stock_kline_data
                   WHERE code = ?  
                   ORDER BY date DESC LIMIT ?''',
                (code, limit)
            )
            rows = cursor.fetchall()
            
            if rows:
                df = pd.DataFrame(rows, columns=['日期', '开盘', '收盘', '最高', '最低', 'volume', 'amount'])
                return df. iloc[: :-1]  # 反转回正序
            return None
    
    @staticmethod
    def get_latest_date(code):
        """获取最新K线日期"""
        with get_db_conn() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT MAX(date) FROM stock_kline_data WHERE code = ? ', (code,))
            result = cursor.fetchone()
            return result[0] if result and result[0] else None
    
    @staticmethod
    def get_need_update(days=1):
        """获取需要更新K线的股票"""
        from models.db import MonitorStockRepository
        stocks = MonitorStockRepository.get_enabled()
        codes = [s[1] for s in stocks]
        
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
                'SELECT status FROM kline_update_log WHERE update_date = ? AND status = "success"',
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
                    '''INSERT OR REPLACE INTO kline_update_log 
                       (update_date, success_count, total_count, status, created_at)
                       VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)''',
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


def populate_initial_data():
    """从config.py导入初始数据"""
    with get_db_conn() as conn:
        cursor = conn.cursor()
        
        # 检查portfolio表是否为空
        cursor.execute('SELECT COUNT(*) FROM portfolio')
        if cursor.fetchone()[0] == 0:
            try:
                from config import PORTFOLIO_CONFIG
                for code, info in PORTFOLIO_CONFIG.items():
                    cursor.execute(
                        'INSERT INTO portfolio (code, name, cost_price, shares) VALUES (?, ?, ?, ?)',
                        (code, info['name'], info['cost_price'], info['shares'])
                    )
                conn.commit()
                logger.info(f"已从config.py导入 {len(PORTFOLIO_CONFIG)} 条初始数据")
            except ImportError: 
                logger.warning("未找到config.py文件")
        
        # 检查monitor_stocks表是否为空
        cursor.execute('SELECT COUNT(*) FROM monitor_stocks')
        if cursor.fetchone()[0] == 0:
            try:
                from sql.config import MONITOR_STOCKS_CONFIG
                for code, name, timeframe, pe_min, pe_max in MONITOR_STOCKS_CONFIG: 
                    cursor.execute(
                        '''INSERT INTO monitor_stocks 
                           (code, name, timeframe, reasonable_pe_min, reasonable_pe_max)
                           VALUES (?, ?, ?, ?, ?)''',
                        (code, name, timeframe, pe_min, pe_max)
                    )
                conn.commit()
                logger.info(f"已从config.py导入 {len(MONITOR_STOCKS_CONFIG)} 条监控股票数据")
            except ImportError: 
                logger.warning("未找到config.py文件，使用默认数据")
                default = [
                    ('sh601919', '中远海控', '1d', 15, 20),
                    ('sz000895', '双汇发展', '1d', 18, 25),
                ]
                for code, name, timeframe, pe_min, pe_max in default:
                    cursor.execute(
                        '''INSERT INTO monitor_stocks 
                           (code, name, timeframe, reasonable_pe_min, reasonable_pe_max)
                           VALUES (?, ?, ?, ?, ?)''',
                        (code, name, timeframe, pe_min, pe_max)
                    )
                conn.commit()