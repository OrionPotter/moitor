# services/data_service.py
import akshare as ak
import pandas as pd
from datetime import datetime, timedelta
import os
from concurrent.futures import ThreadPoolExecutor
import concurrent.futures
import asyncio
import time
from dotenv import load_dotenv
from repositories.cache_repository import MonitorDataCacheRepository
from repositories.eps_cache_repository import EpsCacheRepository
from utils.logger import get_logger

load_dotenv()

# 清除代理设置
os.environ.pop('http_proxy', None)
os.environ.pop('https_proxy', None)
os.environ.pop('all_proxy', None)

# 获取日志实例
logger = get_logger('data_service')


class DataService:
    """数据获取服务"""

    @staticmethod
    def calculate_ema(prices, period):
        """计算指数移动平均线 (EMA)"""
        if len(prices) < period:
            return None

        if not isinstance(prices, pd.Series):
            prices = pd.Series(prices)

        ema = prices.ewm(span=period, adjust=False).mean()
        return round(ema.iloc[-1], 2)

    @staticmethod
    async def get_stock_kline_data(stock_code, period='daily', count=250):
        """获取股票K线数据（优先从本地数据库读取）"""
        from services.kline_service import KlineService

        # 优先从本地数据库读取
        df = await KlineService.get_kline_with_cache(stock_code, period, count)

        if df is not None:
            return df

        # 回退到API获取
        try:
            # 转换股票代码格式
            if stock_code.startswith('sh'):
                symbol = 'sh' + stock_code[2:]
            elif stock_code.startswith('sz'):
                symbol = 'sz' + stock_code[2:]
            else:
                symbol = 'sh' + stock_code if stock_code.startswith('6') else 'sz' + stock_code

            logger.info(f"使用API获取 {stock_code} 的K线数据")

            # 在线程池中执行阻塞的 akshare 调用
            loop = asyncio.get_event_loop()
            df = await loop.run_in_executor(
                None,
                lambda: ak.stock_zh_a_hist_tx(symbol=symbol, start_date="20200101", end_date="20500101", adjust="qfq")
            )

            if df is None or df.empty:
                logger.warning(f"获取 {stock_code} K线数据为空")
                return None

            # 重采样处理
            if period == '2d':
                df['date'] = pd.to_datetime(df['date'])
                df = df.set_index('date')
                df = df.resample('2B').agg({
                    'open': 'first', 'close': 'last', 'high': 'max', 'low': 'min', 'amount': 'sum'
                }).dropna()
                df = df.reset_index()
                df['date'] = df['date'].dt.strftime('%Y-%m-%d')
            elif period == '3d':
                df['date'] = pd.to_datetime(df['date'])
                df = df.set_index('date')
                df = df.resample('3B').agg({
                    'open': 'first', 'close': 'last', 'high': 'max', 'low': 'min', 'amount': 'sum'
                }).dropna()
                df = df.reset_index()
                df['date'] = df['date'].dt.strftime('%Y-%m-%d')

            # 转换列名
            if 'date' in df.columns and 'close' in df.columns:
                df = df.rename(columns={'date': '日期', 'open': '开盘', 'close': '收盘', 'high': '最高', 'low': '最低'})

            if len(df) > count:
                df = df.tail(count)

            return df

        except Exception as e:
            logger.error(f"获取 {stock_code} K线数据失败: {str(e)}")
            return None

    @staticmethod
    async def get_eps_forecast_async(stock_code):
        """获取EPS预测（异步，带数据库缓存）"""
        # 检查缓存
        eps_value = await EpsCacheRepository.get(stock_code)
        if eps_value is not None:
            return eps_value
        
        # 转换股票代码
        code = stock_code
        if code.startswith('sh'):
            code = code[2:]
        elif code.startswith('sz'):
            code = code[2:]
        
        try:
            from services.eps_service import get_current_year_eps_forecast
            eps = get_current_year_eps_forecast(code)
            
            # 缓存结果
            if eps is not None:
                await EpsCacheRepository.set(stock_code, eps)
            
            return eps
        except Exception as e: 
            logger.error(f"获取 {stock_code} EPS预测失败: {e}")
            return None
    
    @staticmethod
    def get_eps_forecast_sync(stock_code):
        """获取EPS预测（纯同步版本，用于线程池）"""
        # 转换股票代码
        code = stock_code
        if code.startswith('sh'):
            code = code[2:]
        elif code.startswith('sz'):
            code = code[2:]
        
        try:
            from services.eps_service import get_current_year_eps_forecast
            eps = get_current_year_eps_forecast(code)
            return eps
        except Exception as e: 
            logger.error(f"获取 {stock_code} EPS预测失败: {e}")
            return None
    
    @staticmethod
    def get_eps_forecast(stock_code):
        """获取EPS预测（同步包装器）"""
        return asyncio.run(DataService.get_eps_forecast_async(stock_code))

    @staticmethod
    async def process_monitor_stock_with_data(stock, monitor_config, kline_data, current_price):
        """处理单只监控股票（使用预获取的K线和价格数据）"""
        stock_code = stock.code
        stock_name = stock.name
        timeframe = stock.timeframe

        try:
            if current_price is None:
                logger.warning(f"无法获取 {stock_code} 的当前价格")
                return None

            # 使用预获取的K线数据
            if kline_data is None or len(kline_data) < 188:
                logger.warning(f"无法获取 {stock_code} 的足够K线数据")
                return None

            # 计算EMA
            closing_prices = kline_data['收盘']
            ema144 = DataService.calculate_ema(closing_prices, 144)
            ema188 = DataService.calculate_ema(closing_prices, 188)

            if ema144 is None or ema188 is None:
                logger.warning(f"无法计算 {stock_code} 的EMA值")
                return None

            # 根据时间维度计算趋势EMA
            ema5 = ema10 = ema20 = None
            ema10_2d = ema30 = ema60 = None
            ema7 = ema21 = ema42 = None

            if timeframe == '1d' and len(closing_prices) >= 20:
                ema5 = DataService.calculate_ema(closing_prices, 5)
                ema10 = DataService.calculate_ema(closing_prices, 10)
                ema20 = DataService.calculate_ema(closing_prices, 20)
            elif timeframe == '2d' and len(closing_prices) >= 60:
                ema10_2d = DataService.calculate_ema(closing_prices, 10)
                ema30 = DataService.calculate_ema(closing_prices, 30)
                ema60 = DataService.calculate_ema(closing_prices, 60)
            elif timeframe == '3d' and len(closing_prices) >= 42:
                ema7 = DataService.calculate_ema(closing_prices, 7)
                ema21 = DataService.calculate_ema(closing_prices, 21)
                ema42 = DataService.calculate_ema(closing_prices, 42)
            pe_min = monitor_config.reasonable_pe_min if monitor_config else 15
            pe_max = monitor_config.reasonable_pe_max if monitor_config else 20

            # EPS 预测稍后批量获取
            result = {
                'code': stock_code,
                'name': stock_name,
                'current_price': round(current_price, 2),
                'ema144': ema144,
                'ema188': ema188,
                'ema5': ema5,
                'ema10': ema10_2d if timeframe == '2d' else ema10,
                'ema20': ema20,
                'ema30': ema30,
                'ema60': ema60,
                'ema7': ema7,
                'ema21': ema21,
                'ema42': ema42,
                'eps_forecast': None,  # 稍后批量获取
                'timeframe': timeframe,
                'reasonable_pe_min': pe_min,
                'reasonable_pe_max': pe_max,
            }

            return result

        except Exception as e:
            logger.error(f"处理 {stock_code} 时出错: {str(e)}")
            return None

    @staticmethod
    async def process_monitor_stock_uncached_with_kline(stock, monitor_config, kline_data):
        """处理单只监控股票（不检查缓存，使用预获取的K线数据）"""
        from services.portfolio_service import PortfolioService

        stock_code = stock.code
        stock_name = stock.name
        timeframe = stock.timeframe

        try:
            # 获取实时价格
            _, current_price, _, _ = await PortfolioService.get_real_time_price_async(stock_code)

            if current_price is None:
                logger.warning(f"无法获取 {stock_code} 的当前价格")
                return None

            # 使用预获取的K线数据
            if kline_data is None or len(kline_data) < 188:
                logger.warning(f"无法获取 {stock_code} 的足够K线数据")
                return None

            # 计算EMA
            closing_prices = kline_data['收盘']
            ema144 = DataService.calculate_ema(closing_prices, 144)
            ema188 = DataService.calculate_ema(closing_prices, 188)

            if ema144 is None or ema188 is None:
                logger.warning(f"无法计算 {stock_code} 的EMA值")
                return None

            # 根据时间维度计算趋势EMA
            ema5 = ema10 = ema20 = None
            ema10_2d = ema30 = ema60 = None
            ema7 = ema21 = ema42 = None

            if timeframe == '1d' and len(closing_prices) >= 20:
                ema5 = DataService.calculate_ema(closing_prices, 5)
                ema10 = DataService.calculate_ema(closing_prices, 10)
                ema20 = DataService.calculate_ema(closing_prices, 20)
            elif timeframe == '2d' and len(closing_prices) >= 60:
                ema10_2d = DataService.calculate_ema(closing_prices, 10)
                ema30 = DataService.calculate_ema(closing_prices, 30)
                ema60 = DataService.calculate_ema(closing_prices, 60)
            elif timeframe == '3d' and len(closing_prices) >= 42:
                ema7 = DataService.calculate_ema(closing_prices, 7)
                ema21 = DataService.calculate_ema(closing_prices, 21)
                ema42 = DataService.calculate_ema(closing_prices, 42)
            pe_min = monitor_config.reasonable_pe_min if monitor_config else 15
            pe_max = monitor_config.reasonable_pe_max if monitor_config else 20

            # EPS 预测稍后批量获取
            result = {
                'code': stock_code,
                'name': stock_name,
                'current_price': round(current_price, 2),
                'ema144': ema144,
                'ema188': ema188,
                'ema5': ema5,
                'ema10': ema10_2d if timeframe == '2d' else ema10,
                'ema20': ema20,
                'ema30': ema30,
                'ema60': ema60,
                'ema7': ema7,
                'ema21': ema21,
                'ema42': ema42,
                'eps_forecast': None,  # 稍后批量获取
                'timeframe': timeframe,
                'reasonable_pe_min': pe_min,
                'reasonable_pe_max': pe_max,
            }

            return result

        except Exception as e:
            logger.error(f"处理 {stock_code} 时出错: {str(e)}")
            return None

    @staticmethod
    async def process_monitor_stock_uncached(stock, monitor_config):
        """处理单只监控股票（不检查缓存）"""
        from services.portfolio_service import PortfolioService

        stock_code = stock.code
        stock_name = stock.name
        timeframe = stock.timeframe

        try:
            # 获取实时价格
            _, current_price, _, _ = await PortfolioService.get_real_time_price_async(stock_code)

            if current_price is None:
                logger.warning(f"无法获取 {stock_code} 的当前价格")
                return None

            # 获取K线数据
            kline_data = await DataService.get_stock_kline_data(stock_code, timeframe)

            if kline_data is None or len(kline_data) < 188:
                logger.warning(f"无法获取 {stock_code} 的足够K线数据")
                return None

            # 计算EMA
            closing_prices = kline_data['收盘']
            ema144 = DataService.calculate_ema(closing_prices, 144)
            ema188 = DataService.calculate_ema(closing_prices, 188)

            if ema144 is None or ema188 is None:
                logger.warning(f"无法计算 {stock_code} 的EMA值")
                return None

            # 根据时间维度计算趋势EMA
            ema5 = ema10 = ema20 = None
            ema10_2d = ema30 = ema60 = None
            ema7 = ema21 = ema42 = None

            if timeframe == '1d' and len(closing_prices) >= 20:
                ema5 = DataService.calculate_ema(closing_prices, 5)
                ema10 = DataService.calculate_ema(closing_prices, 10)
                ema20 = DataService.calculate_ema(closing_prices, 20)
            elif timeframe == '2d' and len(closing_prices) >= 60:
                ema10_2d = DataService.calculate_ema(closing_prices, 10)
                ema30 = DataService.calculate_ema(closing_prices, 30)
                ema60 = DataService.calculate_ema(closing_prices, 60)
            elif timeframe == '3d' and len(closing_prices) >= 42:
                ema7 = DataService.calculate_ema(closing_prices, 7)
                ema21 = DataService.calculate_ema(closing_prices, 21)
                ema42 = DataService.calculate_ema(closing_prices, 42)
            pe_min = monitor_config.reasonable_pe_min if monitor_config else 15
            pe_max = monitor_config.reasonable_pe_max if monitor_config else 20

            # 获取EPS预测
            eps_forecast = DataService.get_eps_forecast(stock_code)

            result = {
                'code': stock_code,
                'name': stock_name,
                'current_price': round(current_price, 2),
                'ema144': ema144,
                'ema188': ema188,
                'ema5': ema5,
                'ema10': ema10_2d if timeframe == '2d' else ema10,
                'ema20': ema20,
                'ema30': ema30,
                'ema60': ema60,
                'ema7': ema7,
                'ema21': ema21,
                'ema42': ema42,
                'eps_forecast': eps_forecast,
                'timeframe': timeframe,
                'reasonable_pe_min': pe_min,
                'reasonable_pe_max': pe_max,
            }

            return result

        except Exception as e:
            logger.error(f"处理 {stock_code} 时出错: {str(e)}")
            return None

    @staticmethod
    async def process_monitor_stock(stock, monitor_config):
        """处理单只监控股票"""
        from services.portfolio_service import PortfolioService

        stock_code = stock.code
        stock_name = stock.name
        timeframe = stock.timeframe

        try:
            # 尝试从缓存获取
            cached = await MonitorDataCacheRepository.get_by_code_and_timeframe(stock_code, timeframe, 30)
            if cached:
                return {
                    'code': stock_code,
                    'name': stock_name,
                    'current_price': cached.current_price,
                    'ema144': cached.ema144,
                    'ema188': cached.ema188,
                    'ema5': cached.ema5,
                    'ema10': cached.ema10,
                    'ema20': cached.ema20,
                    'ema30': cached.ema30,
                    'ema60': cached.ema60,
                    'ema7': cached.ema7,
                    'ema21': cached.ema21,
                    'ema42': cached.ema42,
                    'eps_forecast': cached.eps_forecast,
                    'timeframe': timeframe,
                    'reasonable_pe_min': monitor_config.reasonable_pe_min if monitor_config else 15,
                    'reasonable_pe_max': monitor_config.reasonable_pe_max if monitor_config else 20,
                }

            # 获取实时价格
            _, current_price, _, _ = await PortfolioService.get_real_time_price_async(stock_code)

            if current_price is None:
                logger.warning(f"无法获取 {stock_code} 的当前价格")
                return None

            # 获取K线数据
            kline_data = await DataService.get_stock_kline_data(stock_code, timeframe)

            if kline_data is None or len(kline_data) < 188:
                logger.warning(f"无法获取 {stock_code} 的足够K线数据")
                return None

            # 计算EMA
            closing_prices = kline_data['收盘']
            ema144 = DataService.calculate_ema(closing_prices, 144)
            ema188 = DataService.calculate_ema(closing_prices, 188)

            if ema144 is None or ema188 is None:
                logger.warning(f"无法计算 {stock_code} 的EMA值")
                return None

            # 根据时间维度计算趋势EMA
            ema5 = ema10 = ema20 = None
            ema10_2d = ema30 = ema60 = None
            ema7 = ema21 = ema42 = None

            if timeframe == '1d' and len(closing_prices) >= 20:
                ema5 = DataService.calculate_ema(closing_prices, 5)
                ema10 = DataService.calculate_ema(closing_prices, 10)
                ema20 = DataService.calculate_ema(closing_prices, 20)
            elif timeframe == '2d' and len(closing_prices) >= 60:
                ema10_2d = DataService.calculate_ema(closing_prices, 10)
                ema30 = DataService.calculate_ema(closing_prices, 30)
                ema60 = DataService.calculate_ema(closing_prices, 60)
            elif timeframe == '3d' and len(closing_prices) >= 42:
                ema7 = DataService.calculate_ema(closing_prices, 7)
                ema21 = DataService.calculate_ema(closing_prices, 21)
                ema42 = DataService.calculate_ema(closing_prices, 42)
            pe_min = monitor_config.reasonable_pe_min if monitor_config else 15
            pe_max = monitor_config.reasonable_pe_max if monitor_config else 20

            # EPS 预测稍后批量获取
            result = {
                'code': stock_code,
                'name': stock_name,
                'current_price': round(current_price, 2),
                'ema144': ema144,
                'ema188': ema188,
                'ema5': ema5,
                'ema10': ema10_2d if timeframe == '2d' else ema10,
                'ema20': ema20,
                'ema30': ema30,
                'ema60': ema60,
                'ema7': ema7,
                'ema21': ema21,
                'ema42': ema42,
                'eps_forecast': None,  # 稍后批量获取
                'timeframe': timeframe,
                'reasonable_pe_min': pe_min,
                'reasonable_pe_max': pe_max,
            }

            return result

        except Exception as e:
            logger.error(f"处理 {stock_code} 时出错: {str(e)}")
            return None

    @staticmethod
    async def get_monitor_data():
        """获取监控数据"""
        start_time = time.time()
        logger.info("开始获取监控数据...")
        from repositories.monitor_repository import MonitorStockRepository
        from repositories.kline_repository import KlineRepository
        from services.portfolio_service import PortfolioService

        # 清理过期缓存
        deleted = await MonitorDataCacheRepository.clean_old_data(1)
        if deleted > 0:
            logger.info(f"清理了 {deleted} 条过期缓存")

        # 获取启用的监控股票
        monitor_stocks = await MonitorStockRepository.get_enabled()

        logger.info(f"从数据库加载了 {len(monitor_stocks)} 只监控股票")

        # 批量查询缓存
        code_timeframe_pairs = [(stock.code, stock.timeframe) for stock in monitor_stocks]
        cache_results = await MonitorDataCacheRepository.get_batch_by_code_and_timeframe(code_timeframe_pairs, 30)

        # 分离已缓存和未缓存的股票
        cached_results = []
        uncached_stocks = []

        for stock in monitor_stocks:
            key = (stock.code, stock.timeframe)
            if key in cache_results:
                cached = cache_results[key]
                cached_results.append({
                    'code': stock.code,
                    'name': stock.name,
                    'current_price': cached.current_price,
                    'ema144': cached.ema144,
                    'ema188': cached.ema188,
                    'ema5': cached.ema5,
                    'ema10': cached.ema10,
                    'ema20': cached.ema20,
                    'ema30': cached.ema30,
                    'ema60': cached.ema60,
                    'ema7': cached.ema7,
                    'ema21': cached.ema21,
                    'ema42': cached.ema42,
                    'eps_forecast': cached.eps_forecast,
                    'timeframe': stock.timeframe,
                    'reasonable_pe_min': stock.reasonable_pe_min,
                    'reasonable_pe_max': stock.reasonable_pe_max,
                })
            else:
                uncached_stocks.append(stock)

        logger.info(f"从缓存获取 {len(cached_results)} 只股票，需要重新获取 {len(uncached_stocks)} 只股票")

        # 并发处理未缓存的股票
        if uncached_stocks:
            # 先批量获取所有需要的K线数据
            uncached_codes = [stock.code for stock in uncached_stocks]
            kline_data_dict = await KlineRepository.get_batch_by_codes(uncached_codes, limit=1000)

            # 批量获取所有实时价格
            price_start = time.time()
            price_tasks = [
                PortfolioService.get_real_time_price_async(stock.code)
                for stock in uncached_stocks
            ]
            price_results = await asyncio.gather(*price_tasks, return_exceptions=True)

            # 构建价格映射
            price_map = {}
            for stock, result in zip(uncached_stocks, price_results):
                if isinstance(result, Exception):
                    logger.error(f"获取 {stock.code} 实时价格失败: {result}")
                    price_map[stock.code] = None
                else:
                    price_map[stock.code] = result[1]  # (code, current_price, dividend, yield)

            logger.info(f"批量获取 {len(uncached_stocks)} 只股票实时价格，耗时: {time.time() - price_start:.2f}秒")

            # 并发处理每只股票（使用预获取的数据）
            process_start = time.time()
            tasks = [
                DataService.process_monitor_stock_with_data(
                    stock, stock,
                    kline_data_dict.get(stock.code),
                    price_map.get(stock.code)
                )
                for stock in uncached_stocks
            ]

            results = await asyncio.gather(*tasks, return_exceptions=True)
            logger.info(f"并发处理 {len(uncached_stocks)} 只股票，耗时: {time.time() - process_start:.2f}秒")

            # 过滤异常结果
            for result in results:
                if isinstance(result, Exception):
                    logger.error(f"处理异常: {result}")
                elif result:
                    cached_results.append(result)
                    logger.debug(f"成功处理 {result['code']} {result['name']}")

        # 批量保存新获取的缓存数据
        cache_save_start = time.time()
        cache_data_list = []
        for result in cached_results:
            # 只保存非缓存来源的数据（这里简单处理，全部保存）
            cache_data_list.append({
                'code': result['code'],
                'timeframe': result['timeframe'],
                'current_price': result['current_price'],
                'ema144': result['ema144'],
                'ema188': result['ema188'],
                'ema5': result['ema5'],
                'ema10': result['ema10'],
                'ema20': result['ema20'],
                'ema30': result['ema30'],
                'ema60': result['ema60'],
                'ema7': result['ema7'],
                'ema21': result['ema21'],
                'ema42': result['ema42'],
                'eps_forecast': result['eps_forecast']
            })

        if cache_data_list:
            await MonitorDataCacheRepository.save_batch(cache_data_list)
            logger.info(f"批量保存缓存数据，耗时: {time.time() - cache_save_start:.2f}秒")

        # 获取EPS数据（批量并发获取，优先从缓存读取）
        all_stocks_need_eps = [r for r in cached_results if r.get('eps_forecast') is None]

        if all_stocks_need_eps:
            eps_start = time.time()
            logger.info(f"开始批量获取 {len(all_stocks_need_eps)} 只股票的EPS预测...")
            
            # 先批量检查缓存
            codes = [r['code'] for r in all_stocks_need_eps]
            cached_eps = await EpsCacheRepository.get_batch(codes)
            
            # 分离已缓存和未缓存的
            cached_stocks = []
            uncached_stocks = []
            
            for stock in all_stocks_need_eps:
                code = stock['code']
                if code in cached_eps:
                    stock['eps_forecast'] = cached_eps[code]
                    cached_stocks.append(stock)
                else:
                    uncached_stocks.append(stock)
            
            logger.info(f"从缓存获取 {len(cached_stocks)} 只股票的 EPS，需要重新获取 {len(uncached_stocks)} 只")
            
            # 只获取未缓存的股票
            if uncached_stocks:
                loop = asyncio.get_event_loop()
                with ThreadPoolExecutor(max_workers=10) as executor:
                    eps_tasks = [
                        loop.run_in_executor(executor, DataService.get_eps_forecast_sync, r['code'])
                        for r in uncached_stocks
                    ]
                    
                    # 等待所有任务完成
                    eps_results = await asyncio.gather(*eps_tasks, return_exceptions=True)
                    
                    # 将结果赋值给对应股票
                    for stock, eps in zip(uncached_stocks, eps_results):
                        if isinstance(eps, Exception):
                            logger.error(f"获取 {stock['code']} EPS失败: {eps}")
                            stock['eps_forecast'] = None
                        else:
                            # 异步缓存结果
                            if eps is not None:
                                await EpsCacheRepository.set(stock['code'], eps)
                            stock['eps_forecast'] = eps

            logger.info(f"批量获取EPS完成，耗时: {time.time() - eps_start:.2f}秒")

        elapsed = time.time() - start_time
        logger.info(f"获取监控数据完成，共 {len(cached_results)} 只股票，耗时: {elapsed:.2f}秒")
        return cached_results