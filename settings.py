# -*- coding: utf-8 -*-
import os

APP_NAME = os.environ.get('APP_NAME', 'my_celery_mq')

CURRENT_DIR, _ = os.path.split(os.path.abspath(__file__))
BASE_DIR = CURRENT_DIR or os.getcwd()  # 当前目录

# celery 监控账号密码
MONITOR_USERNAME = os.environ.get('MONITOR_USERNAME', 'admin')
MONITOR_PASSWORD = os.environ.get('MONITOR_PASSWORD', '123456')
# celery 任务数量限制,超过则认为任务堆积过多
LIMIT_TASK = int(os.environ.get('LIMIT_TASK') or 1000)

#  中间件，使用 RabbitMQ，pyamqp://username:Password@HOST:Port//v_host
# DEFAULT_BROKER = 'amqp://development:password123456@134.175.100.239:5672//development_host'
# DEFAULT_BROKER = 'redis://:@127.0.0.1:6379/1'  # redis://username:password@host:port/db_number
# DEFAULT_BROKER = "mongodb://localhost:27017/my_mq_jobs"  # mongodb://username:password@host:port/database_name
DEFAULT_BROKER = f"sqlalchemy+sqlite:///{BASE_DIR}/db.sqlite"  # sqlite:///path/to/database.db

# 保存运行结果，使用 RabbitMQ, rpc://username:Password@IP:Port//v_host
# DEFAULT_RESULT_BACKEND = 'rpc://development:password123456@134.175.100.239:5672//development_host'
# DEFAULT_RESULT_BACKEND = 'redis://:@127.0.0.1:6379/1'
# DEFAULT_RESULT_BACKEND = "mongodb://localhost:27017/my_mq_jobs"
DEFAULT_RESULT_BACKEND = ''  # 不保存运行结果(使用 sqlite 作为 clery 数据储存时，没法保存结果)


class CELERY_CONFIG:
    broker_url = os.environ.get('BROKER_URL') or DEFAULT_BROKER  # 代理人的地址
    result_backend = os.environ.get('CELERY_RESULT_BACKEND') or DEFAULT_RESULT_BACKEND  # 运行结果存储地址

    '''常见的数据序列化方式
    binary: 二进制序列化方式；python的pickle默认的序列化方法；
    json:json 支持多种语言, 可用于跨语言方案，但好像不支持自定义的类对象；
    XML:类似标签语言；
    msgpack:二进制的类 json 序列化方案, 但比 json 的数据结构更小, 更快；
    yaml:yaml 表达能力更强, 支持的数据类型较 json 多, 但是 python 客户端的性能不如 json
    '''
    task_serializer = os.environ.get('CELERY_TASK_SERIALIZER', 'json')  # 任务序列化
    result_serializer = os.environ.get('CELERY_RESULT_SERIALIZER', 'json')  # 任务执行结果序列化
    accept_content = [os.environ.get('CELERY_ACCEPT_CONTENT', 'json')]  # 指定任务接受的内容序列化类型

    task_default_queue = os.environ.get('CELERY_DEFAULT_QUEUE', APP_NAME)  # 默认队列
    result_expires = int(os.environ.get('CELERY_TASK_RESULT_EXPIRES', 3600))  # 任务结果过期时间，单位秒
    task_time_limit = int(os.environ.get('CELERYD_TASK_TIME_LIMIT', 0)) or None  # 规定完成任务的时间，单位秒。在指定时间内完成任务，否则执行该任务的worker将被杀死，任务移交给父进程
    worker_max_tasks_per_child = int(os.environ.get('CELERYD_MAX_TASKS_PER_CHILD', 100)) or None  # 每个worker执行了多少任务就会死掉，默认是无限的
    task_acks_late = os.environ.get('CELERY_ACKS_LATE', 'false').lower() in ('true', '1')  # 任务发送完成是否需要确认，这一项对性能有一点影响

    timezone = 'Asia/Shanghai'  # 设置时区
    enable_utc = True  # UTC时区换算
    # 设置log格式
    worker_log_format = '[%(asctime)s] [%(module)s.%(funcName)s:%(lineno)s] %(levelname)s: %(message)s'
    worker_task_log_format = '[%(asctime)s] [%(levelname)s/%(task_name)s %(task_id)s]: %(message)s'


FETCH_TASK_QUEUE = os.environ.get('FETCH_TASK_QUEUE') or 'fetch_queue'
NOTIFY_TASK_QUEUE = os.environ.get('NOTIFY_TASK_QUEUE') or 'notify_queue'
# 所有的队列
ALL_QUEUES = (CELERY_CONFIG.task_default_queue, FETCH_TASK_QUEUE, NOTIFY_TASK_QUEUE)

