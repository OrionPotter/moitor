import os
import asyncio
import aiohttp
from datetime import datetime
from typing import List, Dict, Optional
import logging

logger = logging.getLogger(__name__)


class XueqiuService:
    """雪球组合监控服务"""
    
    @staticmethod
    def _get_headers() -> Dict[str, str]:
        """获取请求头，包含cookie"""
        token = os.getenv('AKSHARE_TOKEN', '')
        cookie = f"xq_a_token={token};"
        return {
            'Cookie': cookie,
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Referer': 'https://xueqiu.com/'
        }
    
    @staticmethod
    async def _fetch_cube_data(session: aiohttp.ClientSession, cube_symbol: str, count: int = 20, page: int = 1) -> Optional[List[Dict]]:
        """异步获取指定雪球组合的调仓历史
        
        Args:
            session: aiohttp ClientSession对象
            cube_symbol: 组合ID，如 ZH2363479
            count: 每页数量，默认20
            page: 页码，默认1
        
        Returns:
            调仓历史列表，失败返回None
        """
        url = f"https://xueqiu.com/cubes/rebalancing/history.json"
        params = {
            'cube_symbol': cube_symbol,
            'count': count,
            'page': page
        }
        
        try:
            async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as response:
                response.raise_for_status()
                data = await response.json()
                if data and 'list' in data:
                    return data['list']
                return []
        except Exception as e:
            logger.error(f"获取雪球组合 {cube_symbol} 调仓历史失败: {e}")
            return None
    
    @staticmethod
    def _get_cube_name(cube_symbol: str) -> str:
        """从数据库获取组合名称
        
        Args:
            cube_symbol: 组合ID
        
        Returns:
            组合名称
        """
        from models.repositories.xueqiu_cube_repository import XueqiuCubeRepository
        cube = XueqiuCubeRepository.get_by_symbol(cube_symbol)
        return cube.cube_name if cube else cube_symbol
    
    @staticmethod
    def get_all_cubes_data() -> Dict[str, List[Dict]]:
        """获取所有配置的雪球组合数据（异步并发请求）
        
        Returns:
            字典，key为组合ID，value为调仓历史列表
        """
        from models.repositories.xueqiu_cube_repository import XueqiuCubeRepository
        
        # 从数据库获取启用的组合列表
        cube_symbols = XueqiuCubeRepository.get_enabled_symbols()
        
        if not cube_symbols:
            logger.warning("未配置启用的雪球组合")
            return {}
        
        # 使用asyncio运行异步函数
        return asyncio.run(XueqiuService._fetch_all_cubes_async(cube_symbols))
    
    @staticmethod
    async def _fetch_all_cubes_async(cube_symbols: List[str]) -> Dict[str, List[Dict]]:
        """异步获取所有组合数据
        
        Args:
            cube_symbols: 组合ID列表
        
        Returns:
            字典，key为组合ID，value为调仓历史列表
        """
        result = {}
        headers = XueqiuService._get_headers()
        
        # 使用aiohttp的连接器，限制并发连接数
        connector = aiohttp.TCPConnector(limit=10, ttl_dns_cache=300)
        
        async with aiohttp.ClientSession(headers=headers, connector=connector, trust_env=False) as session:
            # 创建所有异步任务
            tasks = [XueqiuService._fetch_cube_data(session, symbol) for symbol in cube_symbols]
            
            # 并发执行所有任务
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # 处理结果
            for cube_symbol, history in zip(cube_symbols, results):
                if isinstance(history, Exception):
                    logger.error(f"获取组合 {cube_symbol} 时发生异常: {history}")
                    result[cube_symbol] = []
                elif history is not None:
                    result[cube_symbol] = history
                else:
                    result[cube_symbol] = []
                    logger.warning(f"获取组合 {cube_symbol} 数据失败")
        
        return result
    
    @staticmethod
    def format_rebalancing_data(cube_symbol: str, history: List[Dict]) -> List[Dict]:
        """格式化调仓数据，便于前端展示
        
        Args:
            cube_symbol: 组合ID
            history: 调仓历史原始数据
        
        Returns:
            格式化后的调仓数据列表
        """
        formatted = []
        
        for item in history:
            try:
                # 解析时间戳
                timestamp = item.get('created_at', 0)
                if timestamp:
                    rebalancing_time = datetime.fromtimestamp(timestamp / 1000).strftime('%Y-%m-%d %H:%M:%S')
                else:
                    rebalancing_time = '-'
                
                # 解析持仓变动 - 使用 rebalancing_histories 字段
                rebalancing_histories = item.get('rebalancing_histories', [])
                
                # 计算持仓变化
                changes = []
                for stock_change in rebalancing_histories:
                    prev_weight = stock_change.get('prev_weight')
                    target_weight = stock_change.get('target_weight', 0)
                    
                    # 判断操作类型
                    if prev_weight is None:
                        # 新买入的股票
                        action = '买入'
                        change = target_weight
                        prev_weight = 0
                    elif prev_weight == 0 and target_weight == 0:
                        # 无变化，跳过
                        continue
                    else:
                        # 有历史仓位，计算变化
                        change = target_weight - prev_weight
                        if change == 0:
                            continue
                        action = '买入' if change > 0 else '卖出'
                    
                    changes.append({
                        'stock_name': stock_change.get('stock_name', ''),
                        'stock_symbol': stock_change.get('stock_symbol', ''),
                        'prev_weight': prev_weight,
                        'target_weight': target_weight,
                        'change': change,
                        'action': action,
                        'price': stock_change.get('price', 0),
                        'prev_price': stock_change.get('prev_price', 0)
                    })
                
                formatted.append({
                    'cube_symbol': cube_symbol,
                    'cube_name': XueqiuService._get_cube_name(cube_symbol),
                    'rebalancing_time': rebalancing_time,
                    'comment': item.get('comment', ''),
                    'changes': changes,
                    'total_change_count': len(changes)
                })
                
            except Exception as e:
                logger.error(f"格式化调仓数据失败: {e}")
                continue
        
        return formatted
    
    @staticmethod
    def get_all_formatted_data() -> Dict[str, List[Dict]]:
        """获取所有雪球组合的格式化数据
        
        Returns:
            字典，key为组合ID，value为格式化后的调仓数据列表
        """
        all_data = XueqiuService.get_all_cubes_data()
        result = {}
        
        for cube_symbol, history in all_data.items():
            result[cube_symbol] = XueqiuService.format_rebalancing_data(cube_symbol, history)
        
        return result