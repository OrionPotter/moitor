# services/data_service.py
import akshare as ak
import pandas as pd
from datetime import datetime, timedelta
import os
from concurrent.futures import ThreadPoolExecutor
import concurrent.futures
import time
from dotenv import load_dotenv
from repositories.cache_repository import MonitorDataCacheRepository
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
    def get_stock_kline_data(stock_code, period='daily', count=250):
        """获取股票K线数据（优先从本地数据库读取）"""
        from services.kline_service import KlineService
        
        # 优先从本地数据库读取
        df = KlineService.get_kline_with_cache(stock_code, period, count)
        
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
            
            df = ak.stock_zh_a_hist_tx(symbol=symbol, start_date="20200101", end_date="20500101", adjust="qfq")
            
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
    def get_eps_forecast(stock_code):
        """获取EPS预测"""
        try:
            code = stock_code
            if code.startswith('sh'):
                code = code[2:]
            elif code.startswith('sz'):
                code = code[2:]
            
            from services.eps_service import get_current_year_eps_forecast
            eps = get_current_year_eps_forecast(code)
            return eps
        except Exception as e: 
            logger.error(f"获取 {stock_code} EPS预测失败: {e}")
            return None
    
    @staticmethod
    def process_monitor_stock(stock, monitor_config):
        """处理单只监控股票"""
        from services.portfolio_service import PortfolioService

        stock_code = stock.code
        stock_name = stock.name
        timeframe = stock.timeframe

        try:
            # 尝试从缓存获取
            cached = MonitorDataCacheRepository.get_by_code_and_timeframe(stock_code, timeframe, 30)
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
            _, current_price, _, _ = PortfolioService.get_real_time_price(stock_code)
            
            if current_price is None:
                logger.warning(f"无法获取 {stock_code} 的当前价格")
                return None
            
            # 获取K线数据
            kline_data = DataService.get_stock_kline_data(stock_code, timeframe)
            
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
            
            # 保存到缓存
            MonitorDataCacheRepository.save(
                stock_code, timeframe, result['current_price'],
                result['ema144'], result['ema188'],
                result['ema5'], result['ema10'], result['ema20'],
                result['ema30'], result['ema60'],
                result['ema7'], result['ema21'], result['ema42'],
                result['eps_forecast']
            )
            
            return result
        
        except Exception as e: 
            logger.error(f"处理 {stock_code} 时出错: {str(e)}")
            return None
    
    @staticmethod
    def get_monitor_data():
        """获取监控数据"""
        from repositories.monitor_repository import MonitorStockRepository
        from repositories.kline_repository import KlineRepository

        logger.info("开始获取监控数据...")
        
        # 清理过期缓存
        deleted = MonitorDataCacheRepository.clean_old_data(1)
        if deleted > 0:
            logger.info(f"清理了 {deleted} 条过期缓存")

        # 获取启用的监控股票
        monitor_stocks = MonitorStockRepository.get_enabled()

        logger.info(f"从数据库加载了 {len(monitor_stocks)} 只监控股票")
        
        results = []

        # 并发处理，保持原始顺序
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [
                executor.submit(
                    DataService.process_monitor_stock,
                    stock,
                    stock
                ) for stock in monitor_stocks
            ]

            for future in futures:
                try:
                    result = future.result()
                    if result:
                        results.append(result)
                except Exception as e:
                    logger.error(f"并发处理异常: {e}")
        
        # 获取EPS数据
        stocks_need_eps = [r for r in results if r.get('eps_forecast') is None]
        
        if stocks_need_eps:
            logger.info(f"开始获取 {len(stocks_need_eps)} 只股票的EPS预测...")
            
            with ThreadPoolExecutor(max_workers=5) as executor:
                eps_futures = {
                    executor.submit(DataService.get_eps_forecast, r['code']): r
                    for r in stocks_need_eps
                }
                
                for future in concurrent.futures.as_completed(eps_futures):
                    result = eps_futures[future]
                    try:
                        eps = future.result()
                        result['eps_forecast'] = eps
                    except Exception as e: 
                        logger.error(f"获取 {result['code']} EPS失败: {e}")
                        result['eps_forecast'] = None
        
        logger.info(f"获取监控数据完成，共 {len(results)} 只股票")
        return results