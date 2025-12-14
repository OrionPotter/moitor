import sqlite3
import os
from datetime import datetime

# 数据库文件路径
DB_PATH = 'portfolio.db'

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
    
    # 创建K线数据表（存储近三年的日K线数据）
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
    
    # 创建索引以提高查询性能
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_portfolio_code ON portfolio(code)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_monitor_code ON monitor_stocks(code)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_monitor_enabled ON monitor_stocks(enabled)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_monitor_cache_code ON monitor_data_cache(code)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_monitor_cache_timeframe ON monitor_data_cache(timeframe)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_monitor_cache_created ON monitor_data_cache(created_at)')
    
    # K线数据表索引
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_kline_code ON stock_kline_data(code)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_kline_code_date ON stock_kline_data(code, date)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_kline_date ON stock_kline_data(date)')
    
    conn.commit()
    conn.close()

def get_all_stocks():
    """获取所有股票数据"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute('SELECT * FROM portfolio ORDER BY code')
    stocks = cursor.fetchall()
    
    conn.close()
    return stocks

def get_stock_by_code(code):
    """根据代码获取单个股票数据"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute('SELECT * FROM portfolio WHERE code = ?', (code,))
    stock = cursor.fetchone()
    
    conn.close()
    return stock

def add_stock(code, name, cost_price, shares):
    """添加股票"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    try:
        cursor.execute('''
            INSERT INTO portfolio (code, name, cost_price, shares)
            VALUES (?, ?, ?, ?)
        ''', (code, name, cost_price, shares))
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        # 如果股票代码已存在，返回False
        return False
    finally:
        conn.close()

def update_stock(code, name, cost_price, shares):
    """更新股票信息"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute('''
        UPDATE portfolio
        SET name = ?, cost_price = ?, shares = ?
        WHERE code = ?
    ''', (name, cost_price, shares, code))
    
    changed = cursor.rowcount > 0
    conn.commit()
    conn.close()
    
    return changed

def delete_stock(code):
    """删除股票"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute('DELETE FROM portfolio WHERE code = ?', (code,))
    changed = cursor.rowcount > 0
    conn.commit()
    conn.close()
    
    return changed

# ========== 监控股票相关操作 ==========

def get_all_monitor_stocks():
    """获取所有监控股票数据"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute('SELECT * FROM monitor_stocks ORDER BY code')
    stocks = cursor.fetchall()
    
    conn.close()
    return stocks

def get_enabled_monitor_stocks():
    """获取所有启用的监控股票数据"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute('SELECT * FROM monitor_stocks WHERE enabled = 1 ORDER BY code')
    stocks = cursor.fetchall()
    
    conn.close()
    return stocks

def get_monitor_stock_by_code(code):
    """根据代码获取单个监控股票数据"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute('SELECT * FROM monitor_stocks WHERE code = ?', (code,))
    stock = cursor.fetchone()
    
    conn.close()
    return stock

def add_monitor_stock(code, name, timeframe, reasonable_pe_min=15, reasonable_pe_max=20):
    """添加监控股票"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    try:
        cursor.execute('''
            INSERT INTO monitor_stocks (code, name, timeframe, reasonable_pe_min, reasonable_pe_max)
            VALUES (?, ?, ?, ?, ?)
        ''', (code, name, timeframe, reasonable_pe_min, reasonable_pe_max))
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        # 如果股票代码已存在，返回False
        return False
    finally:
        conn.close()

def update_monitor_stock(code, name, timeframe, reasonable_pe_min=None, reasonable_pe_max=None, enabled=None):
    """更新监控股票信息"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    if enabled is not None and reasonable_pe_min is not None and reasonable_pe_max is not None:
        cursor.execute('''
            UPDATE monitor_stocks
            SET name = ?, timeframe = ?, reasonable_pe_min = ?, reasonable_pe_max = ?, enabled = ?, updated_at = CURRENT_TIMESTAMP
            WHERE code = ?
        ''', (name, timeframe, reasonable_pe_min, reasonable_pe_max, enabled, code))
    elif reasonable_pe_min is not None and reasonable_pe_max is not None:
        cursor.execute('''
            UPDATE monitor_stocks
            SET name = ?, timeframe = ?, reasonable_pe_min = ?, reasonable_pe_max = ?, updated_at = CURRENT_TIMESTAMP
            WHERE code = ?
        ''', (name, timeframe, reasonable_pe_min, reasonable_pe_max, code))
    elif enabled is not None:
        cursor.execute('''
            UPDATE monitor_stocks
            SET name = ?, timeframe = ?, enabled = ?, updated_at = CURRENT_TIMESTAMP
            WHERE code = ?
        ''', (name, timeframe, enabled, code))
    else:
        cursor.execute('''
            UPDATE monitor_stocks
            SET name = ?, timeframe = ?, updated_at = CURRENT_TIMESTAMP
            WHERE code = ?
        ''', (name, timeframe, code))
    
    changed = cursor.rowcount > 0
    conn.commit()
    conn.close()
    
    return changed

def delete_monitor_stock(code):
    """删除监控股票"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute('DELETE FROM monitor_stocks WHERE code = ?', (code,))
    changed = cursor.rowcount > 0
    conn.commit()
    conn.close()
    
    return changed

def toggle_monitor_stock(code, enabled):
    """启用/禁用监控股票"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute('''
        UPDATE monitor_stocks
        SET enabled = ?, updated_at = CURRENT_TIMESTAMP
        WHERE code = ?
    ''', (enabled, code))
    
    changed = cursor.rowcount > 0
    conn.commit()
    conn.close()
    
    return changed

# ========== 监控数据缓存相关操作 ==========

def save_monitor_data(code, timeframe, current_price, ema144, ema188, 
                     ema5=None, ema10=None, ema20=None, ema30=None, ema60=None,
                     ema7=None, ema21=None, ema42=None, eps_forecast=None):
    """保存监控数据到缓存表"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    try:
        cursor.execute('''
            INSERT OR REPLACE INTO monitor_data_cache 
            (code, timeframe, current_price, ema144, ema188, ema5, ema10, ema20, 
             ema30, ema60, ema7, ema21, ema42, eps_forecast, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ''', (code, timeframe, current_price, ema144, ema188, ema5, ema10, ema20, 
              ema30, ema60, ema7, ema21, ema42, eps_forecast))
        conn.commit()
        return True
    except Exception as e:
        print(f"保存监控数据失败: {e}")
        return False
    finally:
        conn.close()

def get_cached_monitor_data(code, timeframe, max_age_minutes=5):
    """获取缓存的监控数据，如果数据太旧则返回None"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    try:
        # 首先获取最新的缓存数据，不使用时间限制
        cursor.execute('''
            SELECT id, code, timeframe, current_price, ema144, ema188, 
                   ema5, ema10, ema20, ema30, ema60, ema7, ema21, ema42, eps_forecast, created_at
            FROM monitor_data_cache
            WHERE code = ? AND timeframe = ?
            ORDER BY created_at DESC
            LIMIT 1
        ''', (code, timeframe))
        
        result = cursor.fetchone()
        
        # 如果有缓存数据，使用Python检查时间是否在有效期内
        if result:
            created_at_str = result[15]  # created_at字段
            if created_at_str:
                try:
                    from datetime import datetime, timedelta
                    created_at = datetime.strptime(created_at_str, '%Y-%m-%d %H:%M:%S')
                    # 数据库时间是UTC时间，需要转换为北京时间
                    created_at_beijing = created_at + timedelta(hours=8)
                    now = datetime.now()
                    age_minutes = (now - created_at_beijing).total_seconds() / 60
                    
                    # 如果缓存时间超过指定时间，返回None
                    if age_minutes > max_age_minutes:
                        return None
                except Exception as e:
                    print(f"解析缓存时间失败: {e}")
                    return None
        
        return result
    except Exception as e:
        print(f"获取缓存监控数据失败: {e}")
        return None
    finally:
        conn.close()

def get_all_cached_monitor_data():
    """获取所有有效的缓存监控数据"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    try:
        cursor.execute('''
            SELECT code, timeframe, current_price, ema144, ema188, created_at
            FROM monitor_data_cache
            WHERE datetime(created_at) > datetime('now', '-10 minutes')
            ORDER BY code
        ''')
        
        results = cursor.fetchall()
        return results
    except Exception as e:
        print(f"获取所有缓存监控数据失败: {e}")
        return []
    finally:
        conn.close()

def clean_old_monitor_data():
    """清理过期的监控数据缓存（保留最近1小时）"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    try:
        cursor.execute('''
            DELETE FROM monitor_data_cache
            WHERE datetime(created_at) < datetime('now', '-1 hour')
        ''')
        
        deleted_count = cursor.rowcount
        conn.commit()
        return deleted_count
    except Exception as e:
        print(f"清理过期监控数据失败: {e}")
        return 0
    finally:
        conn.close()

# ========== K线数据相关操作 ==========

def save_kline_data(code, kline_data):
    """保存K线数据到数据库（批量插入/更新）"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    try:
        # 准备批量插入数据
        insert_data = []
        for _, row in kline_data.iterrows():
            insert_data.append((
                code,
                row['日期'],
                row['开盘'],
                row['收盘'],
                row['最高'],
                row['最低'],
                0,  # volume，腾讯API不提供
                row['amount'] if 'amount' in row else 0
            ))
        
        # 使用INSERT OR REPLACE进行批量插入
        cursor.executemany('''
            INSERT OR REPLACE INTO stock_kline_data 
            (code, date, open, close, high, low, volume, amount, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ''', insert_data)
        
        conn.commit()
        print(f"[{datetime.now().strftime('%H:%M:%S')}] 成功保存 {len(insert_data)} 条 {code} K线数据")
        return True
    except Exception as e:
        print(f"保存K线数据失败: {e}")
        return False
    finally:
        conn.close()

def get_kline_data_from_db(code, start_date=None, end_date=None, limit=None):
    """从数据库获取K线数据"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    try:
        query = '''
            SELECT date, open, close, high, low, volume, amount
            FROM stock_kline_data
            WHERE code = ?
        '''
        params = [code]
        
        if start_date:
            query += ' AND date >= ?'
            params.append(start_date)
        
        if end_date:
            query += ' AND date <= ?'
            params.append(end_date)
        
        query += ' ORDER BY date DESC'
        
        if limit:
            query += ' LIMIT ?'
            params.append(limit)
        
        cursor.execute(query, params)
        rows = cursor.fetchall()
        
        if rows:
            # 转换为DataFrame格式以保持兼容性
            import pandas as pd
            df = pd.DataFrame(rows, columns=['日期', '开盘', '收盘', '最高', '最低', 'volume', 'amount'])
            return df
        else:
            return None
            
    except Exception as e:
        print(f"从数据库获取K线数据失败: {e}")
        return None
    finally:
        conn.close()

def get_latest_kline_date(code):
    """获取某只股票最新的K线数据日期"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    try:
        cursor.execute('''
            SELECT MAX(date) FROM stock_kline_data WHERE code = ?
        ''', (code,))
        result = cursor.fetchone()
        return result[0] if result and result[0] else None
    except Exception as e:
        print(f"获取最新K线日期失败: {e}")
        return None
    finally:
        conn.close()

def get_stocks_need_update(days=1):
    """获取需要更新K线数据的股票列表（超过指定天数未更新）"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    try:
        cursor.execute('''
            SELECT DISTINCT code FROM monitor_stocks WHERE enabled = 1
        ''')
        monitor_stocks = [row[0] for row in cursor.fetchall()]
        
        stocks_need_update = []
        for code in monitor_stocks:
            latest_date = get_latest_kline_date(code)
            if not latest_date or (datetime.now() - datetime.strptime(latest_date, '%Y-%m-%d')).days >= days:
                stocks_need_update.append(code)
        
        return stocks_need_update
    except Exception as e:
        print(f"获取需要更新的股票列表失败: {e}")
        return []
    finally:
        conn.close()

def populate_initial_data():
    """从config.py导入初始数据（仅在数据库为空时）"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # 检查portfolio表是否为空
    cursor.execute('SELECT COUNT(*) FROM portfolio')
    count = cursor.fetchone()[0]
    
    if count == 0:
        # 从config.py导入初始数据
        try:
            from config import PORTFOLIO_CONFIG
            for code, info in PORTFOLIO_CONFIG.items():
                cursor.execute('''
                    INSERT INTO portfolio (code, name, cost_price, shares)
                    VALUES (?, ?, ?, ?)
                ''', (code, info['name'], info['cost_price'], info['shares']))
            
            conn.commit()
            print(f"已从config.py导入 {len(PORTFOLIO_CONFIG)} 条初始数据")
        except ImportError:
            print("未找到config.py文件，跳过portfolio初始数据导入")
    
    # 检查monitor_stocks表是否为空
    cursor.execute('SELECT COUNT(*) FROM monitor_stocks')
    monitor_count = cursor.fetchone()[0]
    
    if monitor_count == 0:
        # 导入默认监控股票数据
        try:
            from config import MONITOR_STOCKS_CONFIG
            for code, name, timeframe, reasonable_pe_min, reasonable_pe_max in MONITOR_STOCKS_CONFIG:
                cursor.execute('''
                    INSERT INTO monitor_stocks (code, name, timeframe, reasonable_pe_min, reasonable_pe_max)
                    VALUES (?, ?, ?, ?, ?)
                ''', (code, name, timeframe, reasonable_pe_min, reasonable_pe_max))
            print(f"已从config.py导入 {len(MONITOR_STOCKS_CONFIG)} 条监控股票数据")
        except ImportError:
            print("未找到config.py文件，使用默认监控股票数据")
            # 使用默认数据作为备选
            default_monitor_stocks = [
                ('sh601919', '中远海控', '1d', 15, 20),
                ('sz000895', '双汇发展', '1d', 18, 25),
                ('sh600938', '中国海油', '2d', 10, 15),
                ('sh600886', '国投电力', '3d', 15, 22),
                ('sh601169', '北京银行', '2d', 6, 10)
            ]
            
            for code, name, timeframe, reasonable_pe_min, reasonable_pe_max in default_monitor_stocks:
                cursor.execute('''
                    INSERT INTO monitor_stocks (code, name, timeframe, reasonable_pe_min, reasonable_pe_max)
                    VALUES (?, ?, ?, ?, ?)
                ''', (code, name, timeframe, reasonable_pe_min, reasonable_pe_max))
            print(f"已导入 {len(default_monitor_stocks)} 条默认监控股票数据")
        
        conn.commit()
    
    conn.close()

# 初始化数据库
if __name__ == '__main__':
    init_db()
    populate_initial_data()
    print("数据库初始化完成")
    print("当前股票数据:")
    for stock in get_all_stocks():
        print(stock)