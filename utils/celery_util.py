# -*- coding: utf-8 -*-
import logging
import inspect

from celery import Celery, current_app, Task
from utils.import_util import import_submodules


logger = logging.getLogger(__name__)

'''
def custom_send_task(self, *args, **kwargs):
    """celery 发任务补丁,每个beat及worker子任务执行前都经过它"""
    logger.info(f'celery.Celery.send_task args:{args}, kwargs:{kwargs}')
    return self._old_send_task(*args, **kwargs)

if not hasattr(Celery, '_old_send_task'):
    _old_send_task = getattr(Celery, 'send_task')
    setattr(Celery, '_old_send_task', _old_send_task)
    setattr(Celery, 'send_task', custom_send_task)
# '''


def load_task(path):
    """
    load class tasks
    """
    # 重新赋予基类，必须在task注册之前，才可以使task继承基类
    from .celery_base_task import BaseTask
    current_app.Task = BaseTask

    task_lookup = lambda x: inspect.isclass(x) and x != Task and x != BaseTask and issubclass(x, Task)
    modules = import_submodules(path)
    for k, _cls in modules.items():
        task_name = None
        members = inspect.getmembers(_cls, task_lookup)
        # 使用 process 装饰器的类
        if hasattr(_cls, 'process'):
            logger.debug('Loading Task (PRC): %s', k)
            current_app.register_task(_cls.process)
            task_name = _cls.process.name
        # 继承 Task 的类
        if members:
            _name, _task_cls = members[0]
            logger.debug('Loading Task (CLS): %s', _name)
            _task = _task_cls()
            current_app.register_task(_task)
            task_name = _task.name
        # 加载定时器
        beat_schedule = getattr(_cls, 'SCHEDULE', None)
        if task_name and beat_schedule:
            beat_schedule['task'] = task_name
            current_app.conf.beat_schedule[task_name] = beat_schedule
