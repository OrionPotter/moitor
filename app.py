import os
import datetime
import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from dotenv import load_dotenv
from utils.logger import get_logger
from utils.db import init_db_pool, close_db_pool

load_dotenv()

# 获取日志实例
logger = get_logger('app')


@asynccontextmanager
async def lifespan(app: FastAPI):
    # 启动事件
    # 初始化数据库连接池
    await init_db_pool()
    logger.info("数据库连接池已初始化")
    
    # 启动后台任务
    start_background_tasks()
    
    # 启动定时任务调度器
    from services.scheduler_service import SchedulerService
    SchedulerService.start()
    
    # 添加定时任务：每天15:05执行K线更新
    if os.getenv('AUTO_UPDATE_KLINE', 'true').lower() == 'true':
        from services.kline_service import KlineService
        SchedulerService.add_cron_job(
            KlineService.auto_update_kline_data,
            hour=15,
            minute=5,
            job_id='daily_kline_update'
        )

    # 添加定时任务：每天12:00更新股票列表
    if os.getenv('AUTO_UPDATE_STOCK_LIST', 'true').lower() == 'true':
        from services.stock_list_service import StockListService
        SchedulerService.add_cron_job(
            StockListService.auto_update_stock_list,
            hour=12,
            minute=0,
            job_id='daily_stock_list_update'
        )
    
    yield
    # 关闭事件
    from services.scheduler_service import SchedulerService
    SchedulerService.shutdown()
    
    # 关闭数据库连接池
    await close_db_pool()
    logger.info("数据库连接池已关闭")


app = FastAPI(lifespan=lifespan)

# CORS配置
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 添加请求中间件
@app.middleware("http")
async def log_requests(request: Request, call_next):
    import time
    path = request.url.path
    if not path.startswith('/static'):
        print(f"[{datetime.datetime.now().strftime('%H:%M:%S')}] 请求开始: {request.method} {path}")
    
    start_time = time.time()
    response = await call_next(request)
    process_time = time.time() - start_time
    
    if not path.startswith('/static'):
        print(f"[{datetime.datetime.now().strftime('%H:%M:%S')}] 请求完成: {request.method} {path} - 状态: {response.status_code} - 耗时: {process_time:.2f}s")
    
    return response

# 注册路由
from api.portfolio_routes import portfolio_router
from api.monitor_routes import monitor_router
from api.admin_routes import admin_router
from api.tools_routes import tools_router
from api.xueqiu_routes import xueqiu_router
from api.stock_list_routes import stock_list_router

app.include_router(portfolio_router, prefix='/api/portfolio', tags=['portfolio'])
app.include_router(monitor_router, prefix='/api/monitor', tags=['monitor'])
app.include_router(admin_router, prefix='/api/admin', tags=['admin'])
app.include_router(tools_router, prefix='/api/tools', tags=['tools'])
app.include_router(xueqiu_router, prefix='/api/xueqiu', tags=['xueqiu'])
app.include_router(stock_list_router, prefix='/api/stock-list', tags=['stock-list'])
app.include_router(xueqiu_router, prefix='/api/xueqiu', tags=['xueqiu'])

# 页面路由
@app.get('/', response_class=HTMLResponse)
async def index():
    from fastapi.templating import Jinja2Templates
    templates = Jinja2Templates(directory="templates")
    return templates.TemplateResponse("index.html", {"request": {}})

@app.get('/admin', response_class=HTMLResponse)
async def admin():
    from fastapi.templating import Jinja2Templates
    templates = Jinja2Templates(directory="templates")
    return templates.TemplateResponse("admin.html", {"request": {}})

@app.get('/monitor', response_class=HTMLResponse)
async def monitor():
    from fastapi.templating import Jinja2Templates
    templates = Jinja2Templates(directory="templates")
    return templates.TemplateResponse("monitor.html", {"request": {}})

@app.get('/tools', response_class=HTMLResponse)
async def tools():
    from fastapi.templating import Jinja2Templates
    templates = Jinja2Templates(directory="templates")
    return templates.TemplateResponse("tools.html", {"request": {}})

@app.get('/xueqiu', response_class=HTMLResponse)
async def xueqiu():
    from fastapi.templating import Jinja2Templates
    templates = Jinja2Templates(directory="templates")
    return templates.TemplateResponse("xueqiu.html", {"request": {}})


def start_background_tasks():
    """启动后台任务"""
    if os.getenv('AUTO_UPDATE_KLINE', 'true').lower() != 'true':
        logger.warning("已禁用自动K线更新")
        return
    
    from services.kline_service import KlineService
    
    async def auto_update():
        try:
            await KlineService.batch_update_kline_async(force_update=False)
        except Exception as e:
            logger.error(f"启动时自动更新K线失败: {e}")
    
    # 创建后台任务
    asyncio.create_task(auto_update())
    logger.info("K线更新后台任务已启动")


if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host='0.0.0.0', port=5000)