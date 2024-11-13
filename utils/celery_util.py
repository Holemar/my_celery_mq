# -*- coding: utf-8 -*-
import logging
import inspect

from celery import Celery, current_app, Task
from kombu.serialization import register

from .import_util import import_submodules
from .str_util import base64_decode
from .json_util import load_json
from .db_util import get_mongo_db, get_redis_client
from .bson_util import bson_dumps, bson_loads
import settings

# 定时任务配置
BEAT_SCHEDULE = {}

# 注册 celery 的 json 序列化
register('json', bson_dumps, bson_loads, content_type='application/json', content_encoding='utf-8')

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


def load_task(path, app=None):
    """
    load class tasks
    """
    global BEAT_SCHEDULE
    # 重新赋予基类，必须在task注册之前，才可以使task继承基类
    from .celery_base_task import BaseTask
    app = app or current_app
    app.Task = BaseTask

    task_classes = lambda x: inspect.isclass(x) and x != Task and x != BaseTask and issubclass(x, Task)
    task_processors = lambda x: isinstance(x, Task)
    modules = import_submodules(path)
    for k, _cls in modules.items():
        # 使用 @celery.task 装饰器的异步任务函数
        for _name, _task_func in inspect.getmembers(_cls, task_processors):
            task_name = _task_func.name
            logger.info('Loading Task (PRC): %s %s', k, task_name)
            app.register_task(_task_func)
            if hasattr(_task_func, 'schedule') and getattr(_task_func, 'schedule', None):
                BEAT_SCHEDULE[task_name] = {
                    'task': task_name,
                    'schedule': getattr(_task_func, 'schedule', None)
                }
                _task_func.schedule = None  # 去掉 schedule 属性，避免重复执行
                # delattr(_task_func, 'schedule')
        # 继承 CeleryTask 类写法的异步任务类
        for _name, _task_cls in inspect.getmembers(_cls, task_classes):
            task_name = _task_cls.name
            logger.info('Loading Task (CLS): %s %s', _name, task_name)
            _task = _task_cls()
            app.register_task(_task)
            if hasattr(_task_cls, 'schedule') and getattr(_task_cls, 'schedule', None):
                BEAT_SCHEDULE[task_name] = {
                    'task': task_name,
                    'schedule': getattr(_task_cls, 'schedule', None)
                }
                delattr(_task_cls, 'schedule')  # 去掉 schedule 属性，避免重复执行
    app.conf.beat_schedule = BEAT_SCHEDULE
    logger.info(f'BEAT_SCHEDULE:{BEAT_SCHEDULE}')


def delete_repeat_task():
    """删除重复的任务(任务可能太久没执行完，从而再次抛出导致重复)"""
    broker_url = settings.CELERY_CONFIG.broker_url
    queues = settings.ALL_QUEUES
    limit_tasks = settings.LIMIT_TASK

    if broker_url.startswith('mongodb://'):
        db = get_mongo_db(broker_url)
        for key in queues:
            size = db.messages.find({"queue": key}).count()
            # 任务数量不多的情况下，认为没有堆积
            if size < limit_tasks:
                continue
            else:
                delete_mongodb_repeat_task(db, key, size)
    elif broker_url.startswith('redis://'):
        conn = get_redis_client(broker_url)
        for key in queues:
            size = conn.llen(key)
            # 任务数量不多的情况下，认为没有堆积
            if size < limit_tasks:
                continue
            else:
                delete_redis_repeat_task(conn, key, size)
    # 使用 RabbitMQ
    elif broker_url.startswith(('amqp://', 'pyamqp://', 'rpc://')):
        pass  # todo: 未实现


def delete_redis_repeat_task(conn, queue, total):
    """删除指定queue的重复任务
    :param conn: 作为celery broker的 redis 数据库连接
    :param queue: queue 名称
    :param total: 积累的任务数量
    """
    param_set = set()
    for index in range(total):
        res = conn.lpop(queue)
        # 没有数据了
        if res is None:
            break
        # conn.lindex(key, index)
        result = load_json(res)
        body = base64_decode(result.get("body"))
        # 参数完全相同的，认为是重复子任务。
        if body in param_set:
            logger.warning('删除重复任务:%s, %s', body, res)
            continue
        # 不重复的任务，放回队列后面
        else:
            param_set.add(body)
            conn.rpush(queue, res)
        del body
        del res
        del result
    del param_set


def delete_mongodb_repeat_task(db, queue, total):
    """删除指定queue的重复任务
    :param db: 作为celery broker的 mongodb 数据库连接
    :param queue: queue 名称
    :param total: 积累的任务数量
    """
    param_set = set()
    limit = 100  # 每次处理的数量
    page = (total + limit - 1) // limit  # 共需分多少批次(分页算法的页数)
    # 分批执行。这里使用倒序页码，是为了避免正序时删除前面的导致后面分页改变
    for page_index in range(page, -1, -1):
        start_index = page_index * limit
        queue_tasks = db.messages.find({"queue": queue}).skip(start_index).limit(limit)
        delete_ids = set()
        for d in queue_tasks:
            payload = d.get('payload')
            result = load_json(payload)
            body = base64_decode(result.get("body"))
            # 参数完全相同的，认为是重复子任务。
            if body in param_set:
                logger.warning('删除重复子任务:%s, %s', body, payload)
                delete_ids.add(d.get("_id"))
        if delete_ids:
            db.messages.remove({"queue": queue, "_id": {'$in': list(delete_ids)}})

