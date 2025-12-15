import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from models.db import init_db, populate_initial_data
from utils.logger import get_logger

logger = get_logger('init_db')

def main():
    try:
        logger.info("开始初始化数据库...")
        init_db()
        logger.info("数据库表结构创建成功")
        
        populate_initial_data()
        logger.info("初始数据导入成功")
        
        logger.info("数据库初始化完成！")
        return True
    except Exception as e:
        logger.error(f"数据库初始化失败: {e}")
        return False

if __name__ == '__main__':
    success = main()
    sys.exit(0 if success else 1)
