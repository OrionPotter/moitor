# services/portfolio_service.py
import time
import os
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
import akshare as ak
from models.db import StockRepository

# 清除代理设置
os.environ. pop('http_proxy', None)
os.environ.pop('https_proxy', None)
os.environ.pop('all_proxy', None)


class PortfolioService: 
    """投资组合业务逻辑"""
    
    @staticmethod
    def get_real_time_price(stock_code, max_retries=3):
        """获取单只股票实时价格
        
        Returns:
            tuple: (stock_code, current_price, dividend_ttm, dividend_yield_ttm)
        """
        for i in range(max_retries):
            try:
                # 转换股票代码格式为雪球格式
                if stock_code.startswith('sh'):
                    symbol = 'SH' + stock_code[2:]
                elif stock_code. startswith('sz'):
                    symbol = 'SZ' + stock_code[2:]
                else:
                    symbol = 'SH' + stock_code if stock_code.startswith('6') else 'SZ' + stock_code
                
                token = os.getenv('AKSHARE_TOKEN')
                spot_data = ak.stock_individual_spot_xq(symbol=symbol, token=token, timeout=10)
                
                if spot_data is not None and not spot_data.empty:
                    current_price = None
                    dividend_ttm = None
                    dividend_yield_ttm = None
                    
                    # 提取现价
                    for col in ['现价', '最新价']: 
                        price_row = spot_data[spot_data['item'] == col]
                        if not price_row.empty:
                            val = float(price_row['value'].iloc[0])
                            if val > 0:
                                current_price = val
                                break
                    
                    # 提取股息(TTM)
                    div_row = spot_data[spot_data['item'] == '股息(TTM)']
                    if not div_row.empty:
                        dividend_ttm = float(div_row['value'].iloc[0])
                    
                    # 提取股息率(TTM)
                    yield_row = spot_data[spot_data['item'] == '股息率(TTM)']
                    if not yield_row. empty:
                        dividend_yield_ttm = float(yield_row['value'].iloc[0])
                    
                    if current_price: 
                        print(f"成功获取 {stock_code} 的价格: {current_price}")
                        return stock_code, current_price, dividend_ttm, dividend_yield_ttm
            
            except Exception as e: 
                if i < max_retries - 1:
                    time.sleep(2)
                else:
                    print(f"获取 {stock_code} 实时价格失败: {str(e)[:100]}")
        
        return stock_code, None, None, None
    
    @staticmethod
    def get_portfolio_data():
        """获取完整投资组合数据
        
        Returns:
            tuple: (rows_list, summary_dict)
        """
        print(f"[{datetime.now().strftime('%H:%M:%S')}] 开始获取投资组合数据...")
        
        # 获取所有股票
        stocks = StockRepository.get_all()
        if not stocks:
            return [], {'market_value': 0, 'profit':  0, 'annual_dividend': 0}
        
        stock_codes = [stock[1] for stock in stocks]
        
        # 并发获取实时价格
        print(f"并发获取 {len(stock_codes)} 只股票的数据...")
        start = time.time()
        
        with ThreadPoolExecutor(max_workers=7) as executor:
            results = list(executor.map(PortfolioService.get_real_time_price, stock_codes))
        
        elapsed = time.time() - start
        print(f"并发完成，耗时 {elapsed:.2f} 秒")
        
        # 构建股票数据映射
        stock_data_map = {
            r[0]: {'price': r[1], 'div':  r[2], 'div_yield': r[3]}
            for r in results
        }
        
        # 计算投资组合数据
        rows = []
        total = {'market_value': 0, 'profit': 0, 'annual_dividend': 0}
        
        for stock_id, code, name, cost_price, shares in stocks:
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
        
        print(f"[{datetime.now().strftime('%H:%M:%S')}] 计算完成")
        return rows, total