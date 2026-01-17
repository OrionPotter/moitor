-- Tidewatch PostgreSQL 数据库初始化脚本
-- 创建时间: 2024-12-24
-- 版本: 2.0.0

-- 删除已存在的表（如果需要重新初始化）
-- DROP TABLE IF EXISTS kline_update_log CASCADE;
-- DROP TABLE IF EXISTS monitor_data_cache CASCADE;
-- DROP TABLE IF EXISTS stock_kline_data CASCADE;
-- DROP TABLE IF EXISTS monitor_stocks CASCADE;
-- DROP TABLE IF EXISTS portfolio CASCADE;
-- DROP TABLE IF EXISTS xueqiu_cubes CASCADE;

-- 创建股票持仓表
CREATE TABLE IF NOT EXISTS portfolio (
    id SERIAL PRIMARY KEY,
    code TEXT UNIQUE NOT NULL,
    name TEXT NOT NULL,
    cost_price REAL NOT NULL,
    shares INTEGER NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 创建监控股票表
CREATE TABLE IF NOT EXISTS monitor_stocks (
    id SERIAL PRIMARY KEY,
    code TEXT UNIQUE NOT NULL,
    name TEXT NOT NULL,
    timeframe TEXT NOT NULL CHECK (timeframe IN ('1d', '2d', '3d')),
    reasonable_pe_min REAL DEFAULT 15 CHECK (reasonable_pe_min > 0),
    reasonable_pe_max REAL DEFAULT 20 CHECK (reasonable_pe_max > 0),
    enabled INTEGER DEFAULT 1 CHECK (enabled IN (0, 1)),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT chk_pe_range CHECK (reasonable_pe_max > reasonable_pe_min)
);

-- 创建监控数据缓存表
CREATE TABLE IF NOT EXISTS monitor_data_cache (
    id SERIAL PRIMARY KEY,
    code TEXT NOT NULL,
    timeframe TEXT NOT NULL,
    current_price REAL NOT NULL CHECK (current_price > 0),
    ema144 REAL,
    ema188 REAL,
    ema5 REAL,
    ema10 REAL,
    ema20 REAL,
    ema30 REAL,
    ema60 REAL,
    ema7 REAL,
    ema21 REAL,
    ema42 REAL,
    eps_forecast REAL CHECK (eps_forecast IS NULL OR eps_forecast > 0),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(code, timeframe)
);

-- 创建K线数据表
CREATE TABLE IF NOT EXISTS stock_kline_data (
    id SERIAL PRIMARY KEY,
    code TEXT NOT NULL,
    date TEXT NOT NULL,
    open REAL NOT NULL CHECK (open > 0),
    close REAL NOT NULL CHECK (close > 0),
    high REAL NOT NULL CHECK (high > 0),
    low REAL NOT NULL CHECK (low > 0),
    volume INTEGER NOT NULL CHECK (volume >= 0),
    amount REAL NOT NULL CHECK (amount >= 0),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(code, date),
    CONSTRAINT chk_ohlc_valid CHECK (high >= open AND high >= close AND high >= low AND low <= open AND low <= close)
);

-- 创建K线更新日志表
CREATE TABLE IF NOT EXISTS kline_update_log (
    id SERIAL PRIMARY KEY,
    update_date TEXT UNIQUE NOT NULL,
    success_count INTEGER NOT NULL DEFAULT 0 CHECK (success_count >= 0),
    total_count INTEGER NOT NULL DEFAULT 0 CHECK (total_count >= 0),
    status TEXT NOT NULL CHECK (status IN ('success', 'partial', 'failed')),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT chk_count_valid CHECK (success_count <= total_count)
);

-- 创建雪球组合表
CREATE TABLE IF NOT EXISTS xueqiu_cubes (
    id SERIAL PRIMARY KEY,
    cube_symbol TEXT UNIQUE NOT NULL,
    cube_name TEXT NOT NULL,
    enabled INTEGER DEFAULT 1 CHECK (enabled IN (0, 1)),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 创建索引以提高查询性能
-- portfolio表索引
CREATE INDEX IF NOT EXISTS idx_portfolio_code ON portfolio(code);
CREATE INDEX IF NOT EXISTS idx_portfolio_created ON portfolio(created_at);

-- monitor_stocks表索引
CREATE INDEX IF NOT EXISTS idx_monitor_code ON monitor_stocks(code);
CREATE INDEX IF NOT EXISTS idx_monitor_enabled ON monitor_stocks(enabled);
CREATE INDEX IF NOT EXISTS idx_monitor_timeframe ON monitor_stocks(timeframe);

-- monitor_data_cache表索引
CREATE INDEX IF NOT EXISTS idx_monitor_cache_code ON monitor_data_cache(code);
CREATE INDEX IF NOT EXISTS idx_monitor_cache_timeframe ON monitor_data_cache(timeframe);
CREATE INDEX IF NOT EXISTS idx_monitor_cache_created ON monitor_data_cache(created_at);
CREATE INDEX IF NOT EXISTS idx_monitor_cache_unique ON monitor_data_cache(code, timeframe);

-- stock_kline_data表索引
CREATE INDEX IF NOT EXISTS idx_kline_code ON stock_kline_data(code);
CREATE INDEX IF NOT EXISTS idx_kline_code_date ON stock_kline_data(code, date);
CREATE INDEX IF NOT EXISTS idx_kline_date ON stock_kline_data(date);
CREATE INDEX IF NOT EXISTS idx_kline_created ON stock_kline_data(created_at);

-- kline_update_log表索引
CREATE INDEX IF NOT EXISTS idx_update_log_date ON kline_update_log(update_date);
CREATE INDEX IF NOT EXISTS idx_update_log_status ON kline_update_log(status);

-- xueqiu_cubes表索引
CREATE INDEX IF NOT EXISTS idx_xueqiu_cube_symbol ON xueqiu_cubes(cube_symbol);
CREATE INDEX IF NOT EXISTS idx_xueqiu_enabled ON xueqiu_cubes(enabled);

-- 创建触发器以自动更新 updated_at 字段
-- portfolio表触发器
CREATE OR REPLACE FUNCTION update_portfolio_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trigger_update_portfolio_updated_at ON portfolio;
CREATE TRIGGER trigger_update_portfolio_updated_at
    BEFORE UPDATE ON portfolio
    FOR EACH ROW
    EXECUTE FUNCTION update_portfolio_updated_at();

-- monitor_stocks表触发器
CREATE OR REPLACE FUNCTION update_monitor_stocks_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trigger_update_monitor_stocks_updated_at ON monitor_stocks;
CREATE TRIGGER trigger_update_monitor_stocks_updated_at
    BEFORE UPDATE ON monitor_stocks
    FOR EACH ROW
    EXECUTE FUNCTION update_monitor_stocks_updated_at();

-- stock_kline_data表触发器
CREATE OR REPLACE FUNCTION update_stock_kline_data_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trigger_update_stock_kline_data_updated_at ON stock_kline_data;
CREATE TRIGGER trigger_update_stock_kline_data_updated_at
    BEFORE UPDATE ON stock_kline_data
    FOR EACH ROW
    EXECUTE FUNCTION update_stock_kline_data_updated_at();

-- xueqiu_cubes表触发器
CREATE OR REPLACE FUNCTION update_xueqiu_cubes_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trigger_update_xueqiu_cubes_updated_at ON xueqiu_cubes;
CREATE TRIGGER trigger_update_xueqiu_cubes_updated_at
    BEFORE UPDATE ON xueqiu_cubes
    FOR EACH ROW
    EXECUTE FUNCTION update_xueqiu_cubes_updated_at();

-- 插入初始化数据
-- 投资组合初始数据
INSERT INTO portfolio (code, name, cost_price, shares) VALUES
('sh600900', '长江电力', 27.69, 2000),
('sz000895', '双汇发展', 4.95, 100),
('sh601169', '北京银行', 5.66, 100),
('sh601919', '中远海控', 11.99, 5800),
('sh600886', '国投电力', 14.55, 1400)
ON CONFLICT (code) DO NOTHING;

-- 监控股票初始数据
INSERT INTO monitor_stocks (code, name, timeframe, reasonable_pe_min, reasonable_pe_max) VALUES
('sh600036', '招商银行', '2d', 6.5, 9.0),
('sh600096', '云天化', '1d', 8.0, 12.0),
('sh600177', '雅戈尔', '2d', 8.0, 12.0),
('sh600282', '南钢股份', '2d', 11.5, 16.0),
('sh600350', '山东高速', '3d', 12.0, 18.0),
('sh600690', '海尔智家', '2d', 11.0, 16.0),
('sh600886', '国投电力', '3d', 15.0, 22.0),
('sh600900', '长江电力', '1d', 20.0, 25.0),
('sh600938', '中国海油', '2d', 9.0, 14.0),
('sh601006', '大秦铁路', '3d', 11.0, 16.0),
('sh601088', '中国神华', '1d', 12.5, 18.0),
('sh601169', '北京银行', '2d', 4.5, 7.0),
('sh601225', '陕西煤业', '1d', 10.0, 15.0),
('sh601899', '紫金矿业', '2d', 14.0, 20.0),
('sh601919', '中远海控', '1d', 7.0, 12.0),
('sh603565', '中谷物流', '2d', 9.0, 14.0),
('sz000895', '双汇发展', '1d', 16.5, 22.0),
('sz000915', '华特达因', '2d', 15.0, 20.0),
('sz002142', '宁波银行', '2d', 6.0, 9.0)
ON CONFLICT (code) DO NOTHING;

-- 雪球组合初始数据
INSERT INTO xueqiu_cubes (cube_symbol, cube_name, enabled) VALUES
('ZH2363479', '万万没想到', 1),
('ZH3154960', '知行合一', 1),
('ZH1759090', '控鹤', 1),
('ZH2043700', '打野题材', 1),
('ZH1350829', '大匡哥', 1)
ON CONFLICT (cube_symbol) DO NOTHING;

-- 创建视图以便查询
-- 投资组合汇总视图
CREATE OR REPLACE VIEW portfolio_summary AS
SELECT 
    COUNT(*) as total_stocks,
    SUM(cost_price * shares) as total_cost,
    SUM(shares) as total_shares,
    AVG(cost_price) as avg_cost_price
FROM portfolio;

-- 监控股票汇总视图
CREATE OR REPLACE VIEW monitor_stocks_summary AS
SELECT 
    COUNT(*) as total_stocks,
    SUM(CASE WHEN enabled = 1 THEN 1 ELSE 0 END) as enabled_stocks,
    COUNT(DISTINCT timeframe) as unique_timeframes
FROM monitor_stocks;

-- 最近K线更新状态视图
CREATE OR REPLACE VIEW latest_kline_update AS
SELECT 
    update_date,
    success_count,
    total_count,
    status,
    CASE 
        WHEN total_count = 0 THEN 0
        ELSE ROUND((success_count * 100.0 / total_count), 2)
    END as success_rate
FROM kline_update_log 
ORDER BY update_date DESC 
LIMIT 1;