import akshare as ak
import pandas as pd
from datetime import datetime, timedelta
import os
from db import get_all_stocks
import asyncio
import aiohttp
from concurrent.futures import ThreadPoolExecutor
import concurrent.futures
import time
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()

# 简单的内存缓存，用于存储K线数据
kline_cache = {}
cache_expire_time = {}  # 缓存过期时间

# --- 网络连接设置（解决常见的网络连接问题） ---
# 尝试清除代理设置，这在某些网络环境下可能导致连接问题
os.environ.pop('http_proxy', None)
os.environ.pop('https_proxy', None)
os.environ.pop('all_proxy', None)
# ---------------------------------------------------

def get_real_time_price(stock_code, max_retries=3):
    """
    获取单个股票的实时价格，使用stock_individual_spot_xq接口（雪球）
    返回: (stock_code, current_price, dividend_ttm, dividend_yield_ttm)
    """
    for i in range(max_retries):
        try:
            # 使用 stock_individual_spot_xq API 获取实时价格（雪球接口）
            # 需要添加市场前缀（SH 或 SZ）
            if stock_code.startswith('sh'):
                symbol = 'SH' + stock_code[2:]  # 转换为雪球格式
            elif stock_code.startswith('sz'):
                symbol = 'SZ' + stock_code[2:]  # 转换为雪球格式
            else:
                # 如果没有前缀，根据代码判断市场
                if stock_code.startswith('6'):  # 上交所
                    symbol = 'SH' + stock_code
                elif stock_code.startswith('0') or stock_code.startswith('3'):  # 深交所
                    symbol = 'SZ' + stock_code
                else:
                    symbol = stock_code  # 其他情况，直接使用

            token = os.getenv('AKSHARE_TOKEN')
            if not token:
                print("警告: 未设置AKSHARE_TOKEN环境变量，使用None")
                token = None
            
            spot_data = ak.stock_individual_spot_xq(symbol=symbol, token=token, timeout=10)
            
            if spot_data is not None and not spot_data.empty:
                # 查找现价字段
                current_price = None
                dividend_ttm = None
                dividend_yield_ttm = None
                
                if '现价' in spot_data['item'].values:
                    price_row = spot_data[spot_data['item'] == '现价']
                    if not price_row.empty:
                        price_value = price_row['value'].iloc[0]
                        if price_value is not None and str(price_value) != 'nan' and float(price_value) > 0:
                            current_price = float(price_value)
                
                # 如果没有找到"现价"，尝试找"最新价"
                if current_price is None and '最新价' in spot_data['item'].values:
                    price_row = spot_data[spot_data['item'] == '最新价']
                    if not price_row.empty:
                        price_value = price_row['value'].iloc[0]
                        if price_value is not None and str(price_value) != 'nan' and float(price_value) > 0:
                            current_price = float(price_value)
                
                # 获取股息(TTM)
                if '股息(TTM)' in spot_data['item'].values:
                    dividend_row = spot_data[spot_data['item'] == '股息(TTM)']
                    if not dividend_row.empty:
                        dividend_value = dividend_row['value'].iloc[0]
                        if dividend_value is not None and str(dividend_value) != 'nan':
                            dividend_ttm = float(dividend_value)
                
                # 获取股息率(TTM)
                if '股息率(TTM)' in spot_data['item'].values:
                    yield_row = spot_data[spot_data['item'] == '股息率(TTM)']
                    if not yield_row.empty:
                        yield_value = yield_row['value'].iloc[0]
                        if yield_value is not None and str(yield_value) != 'nan':
                            dividend_yield_ttm = float(yield_value)
                
                if current_price is not None:
                    print(f"通过 stock_individual_spot_xq 成功获取 {stock_code} 的价格: {current_price}, 股息(TTM): {dividend_ttm}, 股息率(TTM): {dividend_yield_ttm}")
                    return stock_code, current_price, dividend_ttm, dividend_yield_ttm
        except Exception as e:
            if i < max_retries - 1:
                time.sleep(2)  # 增加等待时间
            else:
                print(f"获取 {stock_code} 实时价格失败 (雪球接口): {str(e)[:100]}...")
    
    print(f"获取 {stock_code} 实时价格失败，将使用成本价作为当前价格。")
    return stock_code, None, None, None


def get_stock_data_parallel(stock_codes, max_workers=7):
    """
    并发获取多个股票的数据
    """
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # 提交所有任务
        futures = [executor.submit(get_real_time_price, code) for code in stock_codes]
        
        # 收集结果
        results = []
        for future in futures:
            results.append(future.result())
    
    return results

def get_portfolio_data():
    """
    核心函数：获取实时数据并计算投资组合的所有指标。
    返回: (list, dict) -> (表格明细数据列表, 总计数据字典)
    """
    print(f"[{datetime.now().strftime('%H:%M:%S')}] 开始获取雪球数据...")
    
    # 从数据库获取所有股票数据
    stocks = get_all_stocks()
    
    # 提取股票代码列表
    stock_codes = [stock[1] for stock in stocks]  # stock[1] 是股票代码
    
    # 并发获取所有股票的实时数据
    print(f"开始并发获取 {len(stock_codes)} 只股票的数据...")
    start_time = time.time()
    stock_results = get_stock_data_parallel(stock_codes)
    end_time = time.time()
    print(f"并发获取数据完成，耗时 {end_time - start_time:.2f} 秒")
    
    # 创建股票代码到股票信息的映射
    stock_info_map = {}
    for i, stock_row in enumerate(stocks):
        stock_id, stock_code, name, cost_price, shares = stock_row
        stock_info_map[stock_code] = {
            'name': name,
            'cost_price': cost_price,
            'shares': shares,
            'original_index': i
        }
    
    # 创建股票代码到实时数据的映射
    stock_data_map = {}
    for result in stock_results:
        stock_code, current_price, dividend_ttm, dividend_yield_ttm = result
        stock_data_map[stock_code] = {
            'current_price': current_price,
            'dividend_ttm': dividend_ttm,
            'dividend_yield_ttm': dividend_yield_ttm
        }
    
    calculated_rows = []
    
    # 初始化总计变量
    total_summary = {
        "market_value": 0,
        "profit": 0,
        "annual_dividend": 0
    }

    # 处理每只股票的计算
    for stock_row in stocks:
        stock_id, stock_code, name, cost_price, shares = stock_row
        # 创建这一行的数据字典，初始包含静态信息
        row_data = {
            'name': name,
            'cost_price': cost_price,
            'shares': shares
        }
        row_data['code'] = stock_code  # 使用纯数字代码
        
        # 从并发结果中获取股票实时数据
        stock_data = stock_data_map.get(stock_code, {})
        current_price = stock_data.get('current_price')
        dividend_per_share_from_api = stock_data.get('dividend_ttm')
        dividend_yield_from_api = stock_data.get('dividend_yield_ttm')
        
        # 如果API没有返回价格，则使用成本价
        if current_price is None:
            current_price = cost_price
            print(f"警告: {stock_code} 的实时价格获取失败，使用成本价: {current_price}")
        else:
            print(f"成功获取 {stock_code} 的价格: {current_price}, 股息(TTM): {dividend_per_share_from_api}, 股息率(TTM): {dividend_yield_from_api}")

        row_data['current_price'] = current_price

        # --- 核心计算逻辑 ---
        # 总市值 = 实时价格 * 持仓股数
        row_data['market_value'] = current_price * row_data['shares']
        
        # 盈亏 = (实时价格 - 成本价格) * 持仓股数
        row_data['profit'] = (current_price - row_data['cost_price']) * row_data['shares']
        
        # 每股分红 (使用API获取的数据，如果没有则为0)
        if dividend_per_share_from_api is not None:
            row_data['dividend_per_share'] = dividend_per_share_from_api
        else:
            # 如果API没有返回股息数据，则设置为0
            row_data['dividend_per_share'] = 0
        
        # 股息率 (优先使用API获取的数据，如果API没有则根据价格和每股分红计算)
        if dividend_yield_from_api is not None:
            row_data['dividend_yield'] = dividend_yield_from_api
        else:
            # 如果API没有返回股息率数据，则根据价格和每股分红计算
            if current_price > 0:
                row_data['dividend_yield'] = (row_data['dividend_per_share'] / current_price) * 100
            else:
                row_data['dividend_yield'] = 0
            
        # 每年分红金额 = 每股分红 * 持仓股数
        row_data['annual_dividend_income'] = row_data['dividend_per_share'] * row_data['shares']

        # --- 累加到总计 ---
        total_summary["market_value"] += row_data['market_value']
        total_summary["profit"] += row_data['profit']
        total_summary["annual_dividend"] += row_data['annual_dividend_income']

        calculated_rows.append(row_data)
        
    print(f"[{datetime.now().strftime('%H:%M:%S')}] 计算完成。")
    return calculated_rows, total_summary

def calculate_ema(prices, period):
    """
    计算指数移动平均线 (EMA)
    prices: 价格列表 (pandas Series 或 list)
    period: EMA周期
    """
    if len(prices) < period:
        return None
    
    # 转换为pandas Series
    if not isinstance(prices, pd.Series):
        prices = pd.Series(prices)
    
    # 计算EMA
    ema = prices.ewm(span=period, adjust=False).mean()
    return ema.iloc[-1]

def get_stock_kline_data(stock_code, period='daily', count=250):
    """
    获取股票K线数据（优先从本地数据库读取）
    stock_code: 股票代码
    period: 周期 ('daily'=日K, '2d'=2日, '3d'=3日)
    count: 获取的数据条数
    """
    from kline_manager import get_kline_data_with_cache
    
    # 优先从本地数据库读取
    df = get_kline_data_with_cache(stock_code, period, count)
    
    if df is not None:
        # 更新内存缓存
        cache_key = f"{stock_code}_{period}"
        current_time = datetime.now()
        kline_cache[cache_key] = df.copy()
        cache_expire_time[cache_key] = current_time + timedelta(hours=1)
        return df
    
    # 如果本地数据库没有数据，回退到原来的API获取方式
    cache_key = f"{stock_code}_{period}"
    current_time = datetime.now()
    
    print(f"[{datetime.now().strftime('%H:%M:%S')}] 本地数据库无数据，使用API获取 {stock_code} 的K线数据")
    try:
        # 转换股票代码格式为腾讯API格式
        if stock_code.startswith('sh'):
            symbol = 'sh' + stock_code[2:]
        elif stock_code.startswith('sz'):
            symbol = 'sz' + stock_code[2:]
        else:
            # 如果没有前缀，根据代码判断市场
            if stock_code.startswith('6'):  # 上交所
                symbol = 'sh' + stock_code
            elif stock_code.startswith('0') or stock_code.startswith('3'):  # 深交所
                symbol = 'sz' + stock_code
            else:
                symbol = stock_code
        
        # 使用腾讯API获取日K线数据
        df = ak.stock_zh_a_hist_tx(symbol=symbol, start_date="20200101", end_date="20500101", adjust="qfq")
        
        if df is None or df.empty:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] 获取 {stock_code} 腾讯K线数据为空")
            return None
        
        # 根据周期重新采样数据
        if period == '2d':
            df['date'] = pd.to_datetime(df['date'])
            df = df.set_index('date')
            df_2d = df.resample('2B').agg({
                'open': 'first',
                'close': 'last',
                'high': 'max',
                'low': 'min',
                'amount': 'sum'
            }).dropna()
            df = df_2d.reset_index()
            df['date'] = df['date'].dt.strftime('%Y-%m-%d')
        elif period == '3d':
            df['date'] = pd.to_datetime(df['date'])
            df = df.set_index('date')
            df_3d = df.resample('3B').agg({
                'open': 'first',
                'close': 'last',
                'high': 'max',
                'low': 'min',
                'amount': 'sum'
            }).dropna()
            df = df_3d.reset_index()
            df['date'] = df['date'].dt.strftime('%Y-%m-%d')
        
        # 转换列名以保持兼容性
        if 'date' in df.columns and 'close' in df.columns:
            df = df.rename(columns={'date': '日期', 'open': '开盘', 'close': '收盘', 'high': '最高', 'low': '最低'})
        
        if df is not None and not df.empty:
            # 取最近count条数据
            if len(df) > count:
                df = df.tail(count)
            
            # 保存到本地数据库
            from kline_manager import update_stock_kline_data
            update_stock_kline_data(stock_code, force_update=False)
            
            # 存储到缓存
            kline_cache[cache_key] = df.copy()
            cache_expire_time[cache_key] = current_time + timedelta(hours=1)
            
            return df
        else:
            # 缓存空结果，避免重复请求
            kline_cache[cache_key] = None
            cache_expire_time[cache_key] = current_time + timedelta(minutes=30)
            return None
            
    except Exception as e:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] 获取 {stock_code} K线数据失败: {str(e)}")
        
        # 缓存失败结果，避免短时间内重复请求
        kline_cache[cache_key] = None
        cache_expire_time[cache_key] = current_time + timedelta(minutes=15)
        
        return None

def process_single_stock(stock):
    """
    处理单只股票的监控数据获取
    返回股票监控结果或None（如果失败）
    """
    from db import get_cached_monitor_data, save_monitor_data
    
    stock_code = stock['code']
    stock_name = stock['name']
    timeframe = stock['timeframe']
    
    try:
        # 首先尝试从缓存获取数据（30分钟内的数据）
        cached_data = get_cached_monitor_data(stock_code, timeframe, 30)
        if cached_data:
            # 检查缓存数据的长度，兼容新旧格式
            if len(cached_data) >= 16:
                # 新格式：包含趋势EMA数据和EPS数据
                # 跳过前几个字段（id, code, timeframe），直接获取数据字段
                current_price = cached_data[3]
                ema144 = cached_data[4]
                ema188 = cached_data[5]
                ema5 = cached_data[6]
                ema10 = cached_data[7]
                ema20 = cached_data[8]
                ema30 = cached_data[9]
                ema60 = cached_data[10]
                ema7 = cached_data[11]
                ema21 = cached_data[12]
                ema42 = cached_data[13]
                eps_forecast = cached_data[14]
                created_at = cached_data[15]
                
                result = {
                    'code': stock_code,
                    'name': stock_name,
                    'current_price': current_price,
                    'ema144': ema144,
                    'ema188': ema188,
                    'timeframe': timeframe,
                    'cached': True,
                    'eps_forecast': eps_forecast
                }
                
                # 添加趋势判断所需的EMA数据
                if timeframe == '1d':
                    result['ema5'] = ema5
                    result['ema10'] = ema10
                    result['ema20'] = ema20
                elif timeframe == '2d':
                    result['ema10'] = ema10
                    result['ema30'] = ema30
                    result['ema60'] = ema60
                elif timeframe == '3d':
                    result['ema7'] = ema7
                    result['ema21'] = ema21
                    result['ema42'] = ema42
                
                print(f"使用缓存数据 {stock_code}: 当前价={current_price}, EMA144={ema144}, EMA188={ema188}, EPS={eps_forecast}")
                return result
            else:
                # 旧格式：只有基本EMA数据，需要重新获取
                print(f"缓存数据格式过旧，重新获取 {stock_code} 的数据")
        else:
            print(f"缓存中没有 {stock_code} 的有效数据，重新获取...")
        
        # 缓存中没有有效数据，重新获取
        
        # 获取当前价格
        _, current_price, _, _ = get_real_time_price(stock_code)
        
        if current_price is None:
            print(f"无法获取 {stock_code} 的当前价格，跳过")
            return None
        
        # 获取K线数据
        kline_data = get_stock_kline_data(stock_code, timeframe)
        
        if kline_data is None or len(kline_data) < 188:
            print(f"无法获取 {stock_code} 的足够K线数据，跳过")
            return None
        
        # 计算EMA144和EMA188
        closing_prices = kline_data['收盘']
        ema144 = calculate_ema(closing_prices, 144)
        ema188 = calculate_ema(closing_prices, 188)
        
        # 根据时间维度计算趋势所需的EMA
        ema5 = ema10 = ema20 = None
        ema10_2d = ema30 = ema60 = None
        ema7 = ema21 = ema42 = None
        
        if timeframe == '1d':
            # 日K线：计算EMA5、EMA10、EMA20
            if len(closing_prices) >= 20:  # 确保有足够数据计算最大的EMA周期
                ema5 = calculate_ema(closing_prices, 5)
                ema10 = calculate_ema(closing_prices, 10)
                ema20 = calculate_ema(closing_prices, 20)
        elif timeframe == '2d':
            # 2日K线：计算EMA10、EMA30、EMA60
            if len(closing_prices) >= 60:  # 确保有足够数据计算最大的EMA周期
                ema10_2d = calculate_ema(closing_prices, 10)
                ema30 = calculate_ema(closing_prices, 30)
                ema60 = calculate_ema(closing_prices, 60)
        elif timeframe == '3d':
            # 3日K线：计算EMA7、EMA21、EMA42
            if len(closing_prices) >= 42:  # 确保有足够数据计算最大的EMA周期
                ema7 = calculate_ema(closing_prices, 7)
                ema21 = calculate_ema(closing_prices, 21)
                ema42 = calculate_ema(closing_prices, 42)
        
        if ema144 is not None and ema188 is not None:
            # 创建result对象
            result = {
                'code': stock_code,
                'name': stock_name,
                'current_price': current_price,
                'ema144': ema144,
                'ema188': ema188,
                'timeframe': timeframe,
                'cached': False
            }
            
            # 添加趋势判断所需的EMA数据
            if timeframe == '1d':
                result['ema5'] = ema5
                result['ema10'] = ema10
                result['ema20'] = ema20
            elif timeframe == '2d':
                result['ema10'] = ema10_2d
                result['ema30'] = ema30
                result['ema60'] = ema60
            elif timeframe == '3d':
                result['ema7'] = ema7
                result['ema21'] = ema21
                result['ema42'] = ema42
            
            # 保存到缓存（包含趋势所需的EMA数据和EPS）
            # 对于2日K线，需要使用ema10_2d而不是ema10
            if timeframe == '2d':
                save_monitor_data(stock_code, timeframe, current_price, ema144, ema188,
                                 ema5, ema10_2d, ema20, ema30, ema60, ema7, ema21, ema42, result.get('eps_forecast'))
            else:
                save_monitor_data(stock_code, timeframe, current_price, ema144, ema188,
                                 ema5, ema10, ema20, ema30, ema60, ema7, ema21, ema42, result.get('eps_forecast'))
            
            result = {
                'code': stock_code,
                'name': stock_name,
                'current_price': current_price,
                'ema144': ema144,
                'ema188': ema188,
                'timeframe': timeframe,
                'cached': False
            }
            
            # 添加趋势判断所需的EMA数据
            if timeframe == '1d':
                result['ema5'] = ema5
                result['ema10'] = ema10
                result['ema20'] = ema20
            elif timeframe == '2d':
                result['ema10'] = ema10_2d
                result['ema30'] = ema30
                result['ema60'] = ema60
            elif timeframe == '3d':
                result['ema7'] = ema7
                result['ema21'] = ema21
                result['ema42'] = ema42
            
            print(f"成功获取并缓存 {stock_code} 监控数据: 当前价={current_price}, EMA144={ema144}, EMA188={ema188}")
            return result
        else:
            print(f"无法计算 {stock_code} 的EMA值")
            return None
            
    except Exception as e:
        print(f"处理 {stock_code} 时出错: {str(e)}")
        return None

def get_eps_forecast_sync(stock_code):
    """同步获取EPS预测数据（用于并发调用）"""
    try:
        # 移除市场前缀，只保留6位数字代码
        code = stock_code
        if code.startswith('sh'):
            code = code[2:]
        elif code.startswith('sz'):
            code = code[2:]
        
        from fetch_eps_forecast_akshare import get_current_year_eps_forecast
        eps_forecast = get_current_year_eps_forecast(code)
        return eps_forecast
    except Exception as e:
        print(f"获取 {stock_code} EPS预测失败: {e}")
        return None

def get_monitor_data():
    """
    获取监控数据，包含EMA144和EMA188（并发版本，支持缓存）
    返回股票列表，每只股票包含当前价格、EMA144、EMA188、EPS预测等信息
    """
    from db import get_enabled_monitor_stocks, clean_old_monitor_data
    
    print(f"[{datetime.now().strftime('%H:%M:%S')}] 开始并发获取监控数据...")
    
    # 清理过期的缓存数据
    deleted_count = clean_old_monitor_data()
    if deleted_count > 0:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] 清理了 {deleted_count} 条过期缓存数据")
    
    # 从数据库获取监控股票配置
    monitor_stocks_db = get_enabled_monitor_stocks()
    
    # 转换为需要的格式
    monitor_stocks = []
    for stock in monitor_stocks_db:
        stock_dict = {
            'code': stock[1],  # code
            'name': stock[2],  # name
            'timeframe': stock[3]  # timeframe
        }
        monitor_stocks.append(stock_dict)
    
    print(f"[{datetime.now().strftime('%H:%M:%S')}] 从数据库加载了 {len(monitor_stocks)} 只监控股票")
    
    results = []
    cached_count = 0
    
    # 使用线程池并发处理，最多7个并发
    with ThreadPoolExecutor(max_workers=5) as executor:  # 5个线程，避免过多并发
        # 提交所有任务
        future_to_stock = {executor.submit(process_single_stock, stock): stock for stock in monitor_stocks}
        
        # 收集结果
        for future in concurrent.futures.as_completed(future_to_stock):
            stock = future_to_stock[future]
            try:
                result = future.result()
                if result is not None:
                    if result.get('cached'):
                        cached_count += 1
                    results.append(result)
            except Exception as e:
                print(f"并发处理 {stock['code']} 时出现异常: {str(e)}")
    
    # 检查哪些股票需要获取EPS数据（EPS为空的都需要获取）
    stocks_need_eps = []
    for result in results:
        # 所有EPS为None的股票都需要获取EPS数据
        if result.get('eps_forecast') is None:
            stocks_need_eps.append(result)
    
    # 只对需要EPS数据的股票进行并发获取
    if stocks_need_eps:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] 开始并发获取 {len(stocks_need_eps)} 只股票的EPS预测数据...")
        
        # 使用线程池并发获取EPS预测
        with ThreadPoolExecutor(max_workers=5) as eps_executor:
            # 提交EPS预测任务
            eps_future_to_result = {
                eps_executor.submit(get_eps_forecast_sync, result['code']): result 
                for result in stocks_need_eps
            }
            
            # 收集EPS预测结果
            for eps_future in concurrent.futures.as_completed(eps_future_to_result):
                result = eps_future_to_result[eps_future]
                try:
                    eps_forecast = eps_future.result()
                    result['eps_forecast'] = eps_forecast
                    
                    # 更新缓存中的EPS数据
                    from db import save_monitor_data
                    save_monitor_data(
                        result['code'], result['timeframe'], result['current_price'],
                        result['ema144'], result['ema188'],
                        result.get('ema5'), result.get('ema10'), result.get('ema20'),
                        result.get('ema30'), result.get('ema60'), result.get('ema7'),
                        result.get('ema21'), result.get('ema42'), eps_forecast
                    )
                    
                except Exception as e:
                    print(f"并发获取 {result['code']} EPS预测失败: {e}")
                    result['eps_forecast'] = None
    
    print(f"[{datetime.now().strftime('%H:%M:%S')}] 并发获取监控数据完成，成功获取 {len(results)} 只股票数据，其中 {cached_count} 只使用缓存")
    return results

# 可以直接运行此文件进行测试
if __name__ == '__main__':
    rows, summary = get_portfolio_data()
    import pprint
    if rows:
        print("--- 明细数据 (第一条) ---")
        pprint.pprint(rows[0])
    else:
        print("无明细数据。")
    print("\n--- 总计数据 ---")
    pprint.pprint(summary)
    
    # 测试监控数据
    print("\n--- 监控数据测试 ---")
    monitor_data = get_monitor_data()
    for stock in monitor_data:
        pprint.pprint(stock)

