-- 添加 last_update 字段到 stock_list 表
-- 执行时间: 2026-01-17

-- 添加 last_update 字段
ALTER TABLE stock_list ADD COLUMN IF NOT EXISTS last_update TIMESTAMP;

-- 添加索引
CREATE INDEX IF NOT EXISTS idx_stock_list_last_update ON stock_list(last_update);

-- 验证字段是否添加成功
SELECT column_name, data_type, is_nullable
FROM information_schema.columns
WHERE table_name = 'stock_list'
ORDER BY ordinal_position;