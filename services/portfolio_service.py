# services/portfolio_service.py
import os
import asyncio
import aiohttp
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
import akshare as ak
from repositories.portfolio_repository import StockRepository
from utils.logger import get_logger

# 获取日志实例
logger = get_logger('portfolio')

# 清除代理设置
os.environ.pop('http_proxy', None)
os.environ.pop('https_proxy', None)
os.environ.pop('all_proxy', None)


class PortfolioService: 
    """投资组合业务逻辑"""
    
    @staticmethod
    def _get_headers() -> dict:
        """获取请求头，包含cookie"""
        token = os.getenv('AKSHARE_TOKEN', '')
        cookie = f"xq_a_token={token};"
        return {
            'Cookie': cookie,
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Referer': 'https://xueqiu.com/'
        }
    
    @staticmethod
    async def _fetch_stock_price(session: aiohttp.ClientSession, stock_code: str) -> tuple:
        """异步获取单只股票实时价格
        
        Returns:
            tuple: (stock_code, current_price, dividend_ttm, dividend_yield_ttm)
        """
        try:
            # 转换股票代码格式为雪球格式
            if stock_code.startswith('sh'):
                symbol = 'SH' + stock_code[2:]
            elif stock_code.startswith('sz'):
                symbol = 'SZ' + stock_code[2:]
            else:
                symbol = 'SH' + stock_code if stock_code.startswith('6') else 'SZ' + stock_code
            
            # 使用雪球API获取股票数据
            url = f"https://stock.xueqiu.com/v5/stock/quote.json?symbol={symbol}&extend=detail"
            
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as response:
                response.raise_for_status()
                data = await response.json()
                
                if data and 'data' in data and 'quote' in data['data']:
                    quote = data['data']['quote']
                    current_price = quote.get('current')
                    dividend_ttm = quote.get('dividend')
                    dividend_yield_ttm = quote.get('dividend_yield')
                    
                    if current_price and current_price > 0:
                        return stock_code, current_price, dividend_ttm or 0, dividend_yield_ttm or 0
        
        except Exception as e:
            logger.error(f"获取 {stock_code} 实时价格失败: {str(e)[:100]}")
        
        return stock_code, None, None, None
    
    @staticmethod
    def get_real_time_price(stock_code, max_retries=3):
        """获取单只股票实时价格（同步方法，用于向后兼容）
        
        Returns:
            tuple: (stock_code, current_price, dividend_ttm, dividend_yield_ttm)
        """
        async def fetch():
            headers = PortfolioService._get_headers()
            async with aiohttp.ClientSession(headers=headers, trust_env=False) as session:
                return await PortfolioService._fetch_stock_price(session, stock_code)
        
        return asyncio.run(fetch())
    
    @staticmethod
    def get_portfolio_data():
        """获取完整投资组合数据

        Returns:
            tuple: (rows_list, summary_dict)
        """
        import time
        start_time = time.time()

        # 获取所有股票
        stocks = StockRepository.get_all()
        if not stocks:
            logger.warning("投资组合为空")
            return [], {'market_value': 0, 'profit':  0, 'annual_dividend': 0}

        stock_codes = [stock.code for stock in stocks]
        logger.info(f"开始获取 {len(stock_codes)} 只股票的实时价格")

        # 使用asyncio运行异步函数
        async def fetch_all():
            headers = PortfolioService._get_headers()
            connector = aiohttp.TCPConnector(limit=10, ttl_dns_cache=300)

            async with aiohttp.ClientSession(headers=headers, connector=connector, trust_env=False) as session:
                # 创建所有异步任务
                tasks = [PortfolioService._fetch_stock_price(session, code) for code in stock_codes]

                # 并发执行所有任务
                results = await asyncio.gather(*tasks, return_exceptions=True)

                # 处理结果
                processed_results = []
                for result in results:
                    if isinstance(result, Exception):
                        logger.error(f"获取股票数据时发生异常: {result}")
                        processed_results.append((None, None, None, None))
                    else:
                        processed_results.append(result)

                return processed_results

        results = asyncio.run(fetch_all())

        # 构建股票数据映射
        stock_data_map = {
            r[0]: {'price': r[1], 'div':  r[2], 'div_yield': r[3]}
            for r in results if r[0]
        }

        # 计算投资组合数据
        rows = []
        total = {'market_value': 0, 'profit': 0, 'annual_dividend': 0}

        for stock in stocks:
            code = stock.code
            name = stock.name
            cost_price = stock.cost_price
            shares = stock.shares

            data = stock_data_map.get(code, {})
            current_price = data.get('price') or cost_price

            row = {
                'code': code,
                'name': name,
                'cost_price': cost_price,
                'shares':  shares,
                'current_price': current_price,
                'market_value': round(current_price * shares, 2),
                'profit': round((current_price - cost_price) * shares, 2),
                'dividend_per_share': data.get('div') or 0,
                'dividend_yield': data.get('div_yield') or 0,
            }
            row['annual_dividend_income'] = round(row['dividend_per_share'] * shares, 2)

            rows.append(row)
            total['market_value'] += row['market_value']
            total['profit'] += row['profit']
            total['annual_dividend'] += row['annual_dividend_income']

        # 计算总股息率：每年分红金额总计 / 总市值总计
        if total['market_value'] > 0:
            total['dividend_yield'] = round(total['annual_dividend'] / total['market_value'] * 100, 2)
        else:
            total['dividend_yield'] = 0

        elapsed = time.time() - start_time
        logger.info(f"投资组合数据获取完成，总市值: {total['market_value']}, 盈亏: {total['profit']}, 耗时: {elapsed:.2f}秒")

        return rows, total