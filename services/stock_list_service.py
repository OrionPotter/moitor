import akshare as ak
import pandas as pd
from datetime import datetime
import os
import asyncio
from repositories.stock_list_repository import StockListRepository
from utils.logger import get_logger

# 清除代理设置
os.environ.pop('http_proxy', None)
os.environ.pop('https_proxy', None)
os.environ.pop('all_proxy', None)

# 获取日志实例
logger = get_logger('stock_list_service')


class StockListService:
    """股票代码服务（异步版本）"""

    @staticmethod
    def fetch_stock_list_from_akshare():
        """从 akshare 获取沪深京 A 股列表"""
        logger.info("开始从 akshare 获取沪深京 A 股列表")
        try:
            # 获取实时行情数据
            df = ak.stock_zh_a_spot_em()

            # 提取代码和名称列
            stock_list = df[['代码', '名称']].copy()
            stock_list.columns = ['code', 'name']

            # 转换为字典列表
            result = stock_list.to_dict('records')

            logger.info(f"成功获取 {len(result)} 只股票")
            return result
        except Exception as e:
            logger.error(f"从 akshare 获取股票列表失败: {str(e)}")
            return None

    @staticmethod
    async def update_stock_list_async():
        """异步更新股票列表到数据库"""
        logger.info("开始更新股票列表")
        start_time = datetime.now()

        # 从 akshare 获取最新数据
        stock_list = StockListService.fetch_stock_list_from_akshare()

        if stock_list is None:
            logger.error("获取股票列表失败，更新终止")
            return False, "获取股票列表失败"

        # 批量插入或更新到数据库
        success, result = await StockListRepository.batch_upsert(stock_list)

        if success:
            elapsed = (datetime.now() - start_time).total_seconds()
            logger.info(f"股票列表更新成功，共 {result} 条记录，耗时: {elapsed:.2f}秒")
            return True, f"更新成功，共 {result} 条记录"
        else:
            logger.error(f"股票列表更新失败: {result}")
            return False, f"更新失败: {result}"

    @staticmethod
    def update_stock_list():
        """同步包装器，用于向后兼容"""
        return asyncio.run(StockListService.update_stock_list_async())

    @staticmethod
    async def get_all_stocks_async():
        """异步获取所有股票"""
        return await StockListRepository.get_all()

    @staticmethod
    def get_all_stocks():
        """获取所有股票（同步包装器）"""
        return asyncio.run(StockListService.get_all_stocks_async())

    @staticmethod
    async def get_stock_by_code_async(code):
        """异步根据代码获取股票"""
        return await StockListRepository.get_by_code(code)

    @staticmethod
    def get_stock_by_code(code):
        """根据代码获取股票（同步包装器）"""
        return asyncio.run(StockListService.get_stock_by_code_async(code))

    @staticmethod
    async def search_stocks_async(keyword):
        """异步搜索股票"""
        return await StockListRepository.search_by_name(keyword)

    @staticmethod
    def search_stocks(keyword):
        """搜索股票（同步包装器）"""
        return asyncio.run(StockListService.search_stocks_async(keyword))

    @staticmethod
    async def get_stock_count_async():
        """异步获取股票总数"""
        return await StockListRepository.get_count()

    @staticmethod
    def get_stock_count():
        """获取股票总数（同步包装器）"""
        return asyncio.run(StockListService.get_stock_count_async())

    @staticmethod
    async def auto_update_stock_list_async():
        """异步自动更新股票列表（定时任务调用）"""
        logger.info("定时任务：自动更新股票列表")
        try:
            success, message = await StockListService.update_stock_list_async()
            if success:
                logger.info(f"定时任务完成: {message}")
            else:
                logger.error(f"定时任务失败: {message}")
        except Exception as e:
            logger.error(f"定时任务异常: {str(e)}")

    @staticmethod
    def auto_update_stock_list():
        """自动更新股票列表（同步包装器）"""
        asyncio.run(StockListService.auto_update_stock_list_async())