# services/kline_service.py
import akshare as ak
import pandas as pd
from datetime import datetime, timedelta
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from repositories.kline_repository import KlineRepository
from repositories.monitor_repository import MonitorStockRepository
from utils.logger import get_logger


os.environ.pop('http_proxy', None)
os.environ.pop('https_proxy', None)
os.environ.pop('all_proxy', None)

# 获取日志实例
logger = get_logger('kline_service')


class KlineService:
    """K线管理服务"""
    
    @staticmethod
    def update_single_kline(code, force_update=False):
        """更新单只股票K线数据"""
        try:
            # 转换代码格式
            if code. startswith('sh'):
                symbol = 'sh' + code[2:]
            elif code.startswith('sz'):
                symbol = 'sz' + code[2:]
            else:
                symbol = 'sh' + code if code.startswith('6') else 'sz' + code
            
            logger.info(f"更新 {code} 的K线数据...")
            
            # 确定数据范围
            if force_update:
                start_date = "20200101"
            else:
                latest = KlineRepository.get_latest_date(code)
                if latest:
                    next_day = (datetime.strptime(latest, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y%m%d')
                    start_date = next_day
                else:
                    start_date = "20200101"
            
            end_date = datetime.now().strftime('%Y%m%d')
            
            if start_date >= end_date:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] {code} 数据已最新")
                return True
            
            # 获取K线数据
            print(f"[{datetime.now().strftime('%H:%M:%S')}] 调用API获取 {symbol} 数据 ({start_date} - {end_date})")
            df = ak.stock_zh_a_hist_tx(symbol=symbol, start_date=start_date, end_date=end_date, adjust="qfq")
            
            if df is None or df.empty:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] {code} 没有新数据")
                return True
            
            # 转换列名
            if 'date' in df.columns and 'close' in df.columns:
                df = df.rename(columns={'date': '日期', 'open': '开盘', 'close':  '收盘', 'high': '最高', 'low': '最低'})
            
            # 保存到数据库
            success, count = KlineRepository.save_batch(code, df)
            if success:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] {code} 保存 {count} 条K线数据")
                return True
            else:
                print(f"[{datetime. now().strftime('%H:%M:%S')}] {code} 保存失败")
                return False
        
        except Exception as e:
            print(f"[{datetime. now().strftime('%H:%M:%S')}] 更新 {code} 失败:  {str(e)}")
            return False
    
    @staticmethod
    def batch_update_kline(force_update=False, max_workers=3):
        """批量更新K线数据"""
        if force_update:
            stocks = MonitorStockRepository.get_enabled()
            codes = [s.code for s in stocks]
            print(f"[{datetime.now().strftime('%H:%M:%S')}] 强制更新 {len(codes)} 只股票的K线")
        else:
            codes = KlineRepository.get_need_update(days=1)
            print(f"[{datetime.now().strftime('%H:%M:%S')}] 增量更新 {len(codes)} 只股票的K线")
        
        if not codes:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] 没有股票需要更新")
            return True
        
        success_count = 0
        total = len(codes)
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(KlineService.update_single_kline, code, force_update): code
                for code in codes
            }
            
            for future in as_completed(futures):
                code = futures[future]
                try:
                    if future.result():
                        success_count += 1
                except Exception as e:
                    print(f"[{datetime.now().strftime('%H:%M:%S')}] {code} 异常: {e}")
        
        status = 'success' if success_count == total else 'partial'
        KlineRepository.record_update(success_count, total, status)
        
        print(f"[{datetime.now().strftime('%H:%M:%S')}] K线更新完成:  {success_count}/{total}")
        return success_count == total
    
    @staticmethod
    def get_kline_with_cache(code, period='daily', count=250):
        """从本地数据库获取K线数据"""
        try:
            df = KlineRepository.get_by_code(code, limit=1000)
            
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
    def auto_update_kline_data():
        """自动更新K线数据"""
        try:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] 检查K线更新条件...")
            
            need, reason = KlineService.should_auto_update()
            
            if not need:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] {reason}")
                return
            
            print(f"[{datetime.now().strftime('%H:%M:%S')}] {reason}，开始更新")
            KlineService.batch_update_kline(force_update=False, max_workers=2)
            print(f"[{datetime.now().strftime('%H:%M:%S')}] K线更新完成")
        
        except Exception as e: 
            print(f"[{datetime.now().strftime('%H:%M:%S')}] 自动更新异常: {e}")
    
    @staticmethod
    def should_auto_update():
        """判断是否需要自动更新"""
        try:
            stocks = MonitorStockRepository.get_enabled()
            if not stocks:
                return False, "没有启用的监控股票"

            codes = [s.code for s in stocks]
            latest_dates = []

            for code in codes:
                latest = KlineRepository.get_latest_date(code)
                if latest:
                    latest_dates.append(latest)
            
            if not latest_dates:
                return True, "没有历史K线数据，需初始化"
            
            latest_dt = datetime.strptime(max(latest_dates), "%Y-%m-%d")
            now = datetime.now()
            hours = (now - latest_dt).total_seconds() / 3600
            
            if hours >= 24:
                return True, f"距离上次更新 {hours:.1f} 小时"
            
            if 9 <= now.hour <= 14 and latest_dt.date() < now.date():
                return True, "交易时段需更新今日数据"
            
            return False, f"{hours:.1f} 小时内已更新"
        
        except Exception as e:
            print(f"判断更新条件异常:  {e}")
            return False, "判断失败"