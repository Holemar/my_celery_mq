# -*- coding:utf-8 -*-
import time
import uuid
import logging
from abc import ABC

from celery.schedules import crontab

import settings
from utils.celery_base_task import BaseTask

logger = logging.getLogger(__name__)

# 约定每个定时任务文件都需要定义一个 SCHEDULE 变量，用于定义定时任务(不定义这个变量则不认为是定时任务)
SCHEDULE = {
    # "schedule": 10,  # 每 10 秒执行一次
    'schedule': crontab(minute='*/1'),  # 每分钟执行一次
    "args": (),  # 任务函数参数
    "kwargs": {},  # 任务函数关键字参数
    "options": {'queue': settings.NOTIFY_TASK_QUEUE},  # 任务选项，比如 定义queue
}


# 这里是继承 CeleryTask 类的异步任务写法，需要重写 run 方法。
class NotifyTask(BaseTask, ABC):
    name = f'{settings.APP_NAME}.{__name__}'

    def run(self):
        _id = uuid.uuid4()
        _t = int(time.time())
        retries = int(self.request.retries)  # 重试次数
        logger.info(f'NotifyTask task run id: {_id}, ts:{_t}, 重试次数: {retries}')
        # 观察异常情况、重试情况
        if retries <= 2:
            # a = 1 / 0
            pass
        else:
            return True


''' NotifyTask.run 函数可以代替下面写法
@current_app.task(base=NotifyTask, name='my_celery_mq.tasks.master_notify', queue=settings.NOTIFY_TASK_QUEUE, bind=True)  # , priority=0)
def process(self):
    _id = uuid.uuid4()
    _t = int(time.time())
    retries = int(self.request.retries)  # 重试次数
    logger.info(f'master_notify task id: {_id}, ts:{_t}, 重试次数: {retries}')
    # 观察异常情况、重试情况
    if retries <= 2:
        raise self.retry(exc=RuntimeError('看看异常怎么处理'), countdown=self.default_retry_delay, max_retries=self.max_retries)
    else:
        return True
# '''

