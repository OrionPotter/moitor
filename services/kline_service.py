import akshare as ak
import pandas as pd
from datetime import datetime, timedelta
import os
import asyncio
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from repositories.kline_repository import KlineRepository
from repositories.monitor_repository import MonitorStockRepository
from repositories.stock_list_repository import StockListRepository
from utils.logger import get_logger


os.environ.pop('http_proxy', None)
os.environ.pop('https_proxy', None)
os.environ.pop('all_proxy', None)

# 获取日志实例
logger = get_logger('kline_service')


class KlineService:
    """K线管理服务（异步版本）"""
    
    @staticmethod
    async def update_single_kline_async(code, force_update=False, latest_date=None):
        """异步更新单只股票K线数据（只获取数据，不保存）
        
        Args:
            code: 股票代码
            force_update: 是否强制更新
            latest_date: 预查询的最新日期，避免重复查询数据库
        """
        try:
            # 转换代码格式
            if code.startswith('sh'):
                symbol = 'sh' + code[2:]
            elif code.startswith('sz'):
                symbol = 'sz' + code[2:]
            else:
                symbol = 'sh' + code if code.startswith('6') else 'sz' + code
            
            # 确定数据范围
            if force_update:
                start_date = "20200101"
            else:
                if latest_date:
                    latest = latest_date
                else:
                    latest = await KlineRepository.get_latest_date(code)
                
                if latest:
                    next_day = (datetime.strptime(latest, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y%m%d')
                    start_date = next_day
                else:
                    # 没有历史数据，从2020年开始获取
                    start_date = "20200101"
            
            end_date = datetime.now().strftime('%Y%m%d')
            
            if start_date >= end_date:
                return True, code, None
            
            # 在线程池中执行阻塞的 akshare 调用，添加120秒超时
            loop = asyncio.get_event_loop()
            df = await asyncio.wait_for(
                loop.run_in_executor(
                    None, 
                    lambda: ak.stock_zh_a_hist_tx(symbol=symbol, start_date=start_date, end_date=end_date, adjust="qfq")
                ),
                timeout=120
            )
            
            if df is None or df.empty:
                return True, code, None
            
            # 转换列名
            if 'date' in df.columns and 'close' in df.columns:
                df = df.rename(columns={'date': '日期', 'open': '开盘', 'close':  '收盘', 'high': '最高', 'low': '最低'})
            
            return True, code, df
        
        except asyncio.TimeoutError:
            logger.warning(f"获取 {code} 数据超时")
            return False, code, None
        except Exception as e:
            logger.error(f"获取 {code} 数据失败: {str(e)}")
            return False, code, None

    @staticmethod
    def update_single_kline(code, force_update=False):
        """同步包装器，用于向后兼容"""
        return asyncio.run(KlineService.update_single_kline_async(code, force_update))[0]
    
    @staticmethod
    def _add_prefix_to_code(code):
        """为股票代码添加前缀（sh/sz/bj）"""
        if code.startswith('6') or code.startswith('5'):
            return f'sh{code}'
        elif code.startswith('0') or code.startswith('3'):
            return f'sz{code}'
        elif code.startswith('8') or code.startswith('4') or code.startswith('9'):
            return f'bj{code}'
        return code

    @staticmethod
    async def batch_update_kline_async(force_update=False, max_concurrent=None):
        """异步批量更新K线数据"""
        # 从环境变量获取并发数，默认为 10
        if max_concurrent is None:
            max_concurrent = int(os.getenv('KLINE_UPDATE_CONCURRENT', '10'))

        # 检查是否更新所有股票
        update_all = os.getenv('UPDATE_ALL_STOCKS', 'false').lower() == 'true'

        if force_update:
            if update_all:
                # 循环获取需要更新的股票（每次10条）
                logger.info("开始强制更新所有股票的K线（分批处理）")
                total_processed = 0
                batch_count = 0

                while True:
                    # 获取需要更新的股票（每次默认10条）
                    stocks = await StockListRepository.get_pending_update(limit=max_concurrent)

                    if not stocks:
                        logger.info("所有股票已处理完成")
                        break

                    batch_count += 1
                    codes = [KlineService._add_prefix_to_code(s.code) for s in stocks]
                    logger.info(f"批次 {batch_count}: 处理 {len(codes)} 只股票")

                    # 处理这批股票
                    batch_result = await KlineService._process_batch(codes, max_concurrent, force_update)

                    # 更新这些股票的 last_update 时间
                    updated_codes = [s.code for s in stocks]
                    await StockListRepository.update_last_update(updated_codes)

                    total_processed += len(codes)
                    logger.info(f"批次 {batch_count} 完成，累计处理 {total_processed} 只股票")

                    # 如果返回 False，说明有错误，可以选择继续或停止
                    if not batch_result and force_update:
                        logger.warning(f"批次 {batch_count} 处理出现错误，继续处理下一批")

                logger.info(f"强制更新完成，共处理 {total_processed} 只股票")
                return True
            else:
                # 只更新监控股票
                stocks = await MonitorStockRepository.get_enabled()
                codes = [s.code for s in stocks]
                logger.info(f"强制更新 {len(codes)} 只监控股票的K线")
                return await KlineService._process_batch(codes, max_concurrent, force_update)
        else:
            if update_all:
                # 循环获取需要更新的股票（每次10条）
                logger.info("开始增量更新所有股票的K线（分批处理）")
                total_processed = 0
                batch_count = 0

                while True:
                    # 获取需要更新的股票（每次10条）
                    stocks = await StockListRepository.get_pending_update(limit=max_concurrent)

                    if not stocks:
                        logger.info("没有更多股票需要更新")
                        break

                    batch_count += 1
                    codes = [KlineService._add_prefix_to_code(s.code) for s in stocks]
                    logger.info(f"批次 {batch_count}: 处理 {len(codes)} 只股票")

                    # 处理这批股票
                    batch_result = await KlineService._process_batch(codes, max_concurrent, force_update)

                    # 更新这些股票的 last_update 时间
                    updated_codes = [s.code for s in stocks]
                    await StockListRepository.update_last_update(updated_codes)

                    total_processed += len(codes)
                    logger.info(f"批次 {batch_count} 完成，累计处理 {total_processed} 只股票")

                    # 如果返回 False，说明有错误，可以选择继续或停止
                    if not batch_result:
                        logger.warning(f"批次 {batch_count} 处理出现错误，继续处理下一批")

                logger.info(f"增量更新完成，共处理 {total_processed} 只股票")
                return True
            else:
                # 只更新需要更新的监控股票
                codes = await KlineRepository.get_need_update(days=1)
                logger.info(f"增量更新 {len(codes)} 只监控股票的K线")
                return await KlineService._process_batch(codes, max_concurrent, force_update)

    @staticmethod
    async def _process_batch(codes, max_concurrent, force_update):
        """处理一批股票的K线更新

        Args:
            codes: 股票代码列表
            max_concurrent: 最大并发数
            force_update: 是否强制更新

        Returns:
            bool: 是否全部成功
        """
        if not codes:
            return True

        # 批量查询所有股票的最新日期（非强制更新时）
        latest_dates = {}
        if not force_update:
            latest_dates = await KlineRepository.get_latest_dates_batch(codes)

        # 使用信号量控制并发数量
        semaphore = asyncio.Semaphore(max_concurrent)

        async def update_with_semaphore(code):
            async with semaphore:
                return await KlineService.update_single_kline_async(code, force_update, latest_dates.get(code))

        # 创建所有任务
        tasks = [update_with_semaphore(code) for code in codes]

        # 并发执行所有任务，只获取数据不保存
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # 收集所有需要保存的数据
        kline_data_dict = {}
        success_count = 0
        total = len(codes)
        no_data_count = 0
        error_count = 0

        for result in results:
            if isinstance(result, Exception):
                error_count += 1
            else:
                success, code, df = result
                if success:
                    if df is not None and not df.empty:
                        kline_data_dict[code] = df
                        success_count += 1
                    else:
                        no_data_count += 1
                else:
                    error_count += 1

        logger.info(f"数据获取完成: {success_count} 只有新数据, {no_data_count} 只无新数据, {error_count} 只失败")

        # 一次性保存所有数据到数据库
        if kline_data_dict:
            save_start = time.time()
            saved_count, _, records = await KlineRepository.save_all_batch(kline_data_dict)
            save_time = time.time() - save_start
            logger.info(f"批量保存完成: {saved_count} 只股票，{records} 条记录，耗时: {save_time:.2f}秒")
        else:
            logger.info("没有新数据需要保存")

        status = 'success' if success_count == total else 'partial'
        await KlineRepository.record_update(success_count, total, status)

        logger.info(f"批次处理完成: {success_count}/{total}")
        return success_count == total

    @staticmethod
    def batch_update_kline(force_update=False, max_workers=3):
        """同步包装器，用于向后兼容"""
        # 从环境变量获取并发数，默认为 50
        max_concurrent = int(os.getenv('KLINE_UPDATE_CONCURRENT', '50'))
        return asyncio.run(KlineService.batch_update_kline_async(force_update, max_concurrent=max_concurrent))
    
    @staticmethod
    async def get_kline_with_cache(code, period='daily', count=250):
        """从本地数据库获取K线数据"""
        try:
            df = await KlineRepository.get_by_code(code, limit=1000)
            
            if df is None or df.empty:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] 本地无 {code} 的K线数据")
                return None
            
            print(f"[{datetime.now().strftime('%H:%M:%S')}] 从本地获取 {code} 的 {len(df)} 条K线")
            
            # 重采样处理
            if period == '2d':
                df['日期'] = pd.to_datetime(df['日期'], errors='coerce')
                df = df.set_index('日期')
                df = df.resample('2B').agg({
                    '开盘': 'first', '收盘': 'last', '最高': 'max', '最低': 'min', 'amount': 'sum'
                }).dropna()
                df = df.reset_index()
                df['日期'] = df['日期'].dt.strftime('%Y-%m-%d')
            
            elif period == '3d': 
                df['日期'] = pd.to_datetime(df['日期'], errors='coerce')
                df = df.set_index('日期')
                df = df.resample('3B').agg({
                    '开盘': 'first', '收盘': 'last', '最高': 'max', '最低': 'min', 'amount': 'sum'
                }).dropna()
                df = df.reset_index()
                df['日期'] = df['日期'].dt.strftime('%Y-%m-%d')
            
            if len(df) > count:
                df = df.tail(count)
            
            return df
        
        except Exception as e: 
            print(f"[{datetime.now().strftime('%H:%M:%S')}] 获取 {code} K线失败: {str(e)}")
            return None
    
    @staticmethod
    async def auto_update_kline_data_async():
        """异步自动更新K线数据"""
        try:
            update_all = os.getenv('UPDATE_ALL_STOCKS', 'false').lower() == 'true'
            scope = "所有股票" if update_all else "监控股票"
            
            logger.info(f"检查K线更新条件（{scope}）...")
            
            need, reason = await KlineService.should_auto_update_async()
            
            if not need:
                logger.info(f"无需更新: {reason}")
                return
            
            logger.info(f"需要更新: {reason}，开始更新{scope}")
            await KlineService.batch_update_kline_async(force_update=False)
            logger.info(f"{scope}K线更新完成")
        
        except Exception as e: 
            logger.error(f"自动更新异常: {e}")

    @staticmethod
    def auto_update_kline_data():
        """自动更新K线数据（同步包装器）"""
        asyncio.run(KlineService.auto_update_kline_data_async())
    
    @staticmethod
    async def should_auto_update_async():
        """异步判断是否需要自动更新"""
        try:
            update_all = os.getenv('UPDATE_ALL_STOCKS', 'false').lower() == 'true'
            
            if update_all:
                # 从股票列表获取所有股票
                all_stocks = await StockListRepository.get_all()
                codes = [KlineService._add_prefix_to_code(s.code) for s in all_stocks]
                if not codes:
                    return False, "没有股票数据"
            else:
                # 只检查监控股票
                stocks = await MonitorStockRepository.get_enabled()
                if not stocks:
                    return False, "没有启用的监控股票"
                codes = [s.code for s in stocks]
            
            # 使用批量查询一次性获取所有股票的最新日期
            latest_dates = await KlineRepository.get_latest_dates_batch(codes)
            
            # 过滤出有效的日期
            valid_dates = [date for date in latest_dates.values() if date is not None]
            
            if not valid_dates:
                return True, "没有历史K线数据，需初始化"
            
            latest_dt = datetime.strptime(max(valid_dates), "%Y-%m-%d")
            now = datetime.now()
            hours = (now - latest_dt).total_seconds() / 3600
            
            if hours >= 24:
                return True, f"距离上次更新 {hours:.1f} 小时"
            
            if 9 <= now.hour <= 14 and latest_dt.date() < now.date():
                return True, "交易时段需更新今日数据"
            
            return False, f"{hours:.1f} 小时内已更新"
        
        except Exception as e:
            logger.error(f"判断更新条件异常: {e}")
            return False, "判断失败"

    @staticmethod
    def should_auto_update():
        """判断是否需要自动更新（同步包装器）"""
        return asyncio.run(KlineService.should_auto_update_async())