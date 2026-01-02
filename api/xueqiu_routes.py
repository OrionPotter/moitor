from flask import Blueprint, jsonify
from services.xueqiu_service import XueqiuService
from datetime import datetime
import time
import asyncio

xueqiu_routes = Blueprint('xueqiu', __name__)


@xueqiu_routes.route('', methods=['GET'])
def get_xueqiu_data():
    """获取所有雪球组合的调仓数据"""
    start_time = time.time()
    try:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] 开始处理 /api/xueqiu 请求")
        # 调用同步方法，内部使用asyncio.run()运行异步代码
        all_data = XueqiuService.get_all_formatted_data()

        elapsed = time.time() - start_time
        print(f"[{datetime.now().strftime('%H:%M:%S')}] /api/xueqiu 请求完成，耗时 {elapsed:.2f} 秒")

        return jsonify({
            'status': 'success',
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'data': all_data
        })
    except Exception as e:
        elapsed = time.time() - start_time
        print(f"[{datetime.now().strftime('%H:%M:%S')}] /api/xueqiu 请求失败，耗时 {elapsed:.2f} 秒，错误: {str(e)}")
        return jsonify({'status': 'error', 'message': str(e)}), 500


@xueqiu_routes.route('/<cube_symbol>', methods=['GET'])
def get_cube_data(cube_symbol):
    """获取指定雪球组合的调仓数据"""
    try:
        # 在同步函数中运行异步代码
        async def fetch_single():
            headers = XueqiuService._get_headers()
            import aiohttp
            async with aiohttp.ClientSession(headers=headers, trust_env=False) as session:
                history = await XueqiuService._fetch_cube_data(session, cube_symbol)
            return history

        history = asyncio.run(fetch_single())

        if history is None:
            return jsonify({'status': 'error', 'message': '获取数据失败'}), 500

        formatted = XueqiuService.format_rebalancing_data(cube_symbol, history)

        return jsonify({
            'status': 'success',
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'cube_symbol': cube_symbol,
            'data': formatted
        })
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500