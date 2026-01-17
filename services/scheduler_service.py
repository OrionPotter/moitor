from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from utils.logger import get_logger

logger = get_logger('scheduler_service')

# 创建全局调度器实例
scheduler = AsyncIOScheduler()


class SchedulerService:
    """定时任务管理服务"""
    
    @staticmethod
    def start():
        """启动调度器"""
        try:
            if not scheduler.running:
                scheduler.start()
                logger.info("定时任务调度器已启动")
        except Exception as e:
            logger.error(f"启动调度器失败: {e}")
    
    @staticmethod
    def shutdown():
        """关闭调度器"""
        try:
            if scheduler.running:
                scheduler.shutdown()
                logger.info("定时任务调度器已关闭")
        except Exception as e:
            logger.error(f"关闭调度器失败: {e}")
    
    @staticmethod
    def add_cron_job(func, hour, minute, job_id=None, args=(), kwargs=None):
        """
        添加定时任务（Cron表达式）
        
        Args:
            func: 要执行的函数
            hour: 小时 (0-23)
            minute: 分钟 (0-59)
            job_id: 任务ID（可选）
            args: 位置参数
            kwargs: 关键字参数
        """
        try:
            if kwargs is None:
                kwargs = {}
            
            trigger = CronTrigger(hour=hour, minute=minute)
            
            if job_id:
                scheduler.add_job(
                    func,
                    trigger=trigger,
                    id=job_id,
                    args=args,
                    kwargs=kwargs,
                    replace_existing=True
                )
                logger.info(f"已添加定时任务: {job_id} - 每天 {hour:02d}:{minute:02d} 执行")
            else:
                scheduler.add_job(
                    func,
                    trigger=trigger,
                    args=args,
                    kwargs=kwargs
                )
                logger.info(f"已添加定时任务: 每天 {hour:02d}:{minute:02d} 执行")
        except Exception as e:
            logger.error(f"添加定时任务失败: {e}")
    
    @staticmethod
    def remove_job(job_id):
        """移除定时任务"""
        try:
            scheduler.remove_job(job_id)
            logger.info(f"已移除定时任务: {job_id}")
        except Exception as e:
            logger.error(f"移除定时任务失败: {e}")
    
    @staticmethod
    def get_jobs():
        """获取所有任务"""
        return scheduler.get_jobs()
    
    @staticmethod
    def pause_job(job_id):
        """暂停任务"""
        try:
            scheduler.pause_job(job_id)
            logger.info(f"已暂停任务: {job_id}")
        except Exception as e:
            logger.error(f"暂停任务失败: {e}")
    
    @staticmethod
    def resume_job(job_id):
        """恢复任务"""
        try:
            scheduler.resume_job(job_id)
            logger.info(f"已恢复任务: {job_id}")
        except Exception as e:
            logger.error(f"恢复任务失败: {e}")