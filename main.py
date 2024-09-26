# -*- coding: utf-8 -*-
import os
import logging
import argparse

import celery
from celery import Celery

import settings
from utils import celery_util, log_filter

logger = logging.getLogger(__name__)

celery_app = Celery(settings.APP_NAME)
celery_app.config_from_object(settings.CELERY_CONFIG)

celery_util.load_task('tasks')  # 加载 tasks 目录下的任务
# logger.info(f'Celery config: {celery_app.conf}')


def run():
    """
    启动 celery 任务
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('-m', '--mode', choices=['worker', 'beat'])
    parser.add_argument('--pool', choices=['solo', 'gevent', 'prefork', 'eventlet'], default='solo')  # 并发模型，可选：prefork (默认，multiprocessing), eventlet, gevent, threads.
    parser.add_argument('-l', '--loglevel', default='INFO')  # 日志级别，可选：DEBUG, INFO, WARNING, ERROR, CRITICAL, FATAL
    parser.add_argument('-c', '--concurrency', default='')  # 并发数量，prefork 模型下就是子进程数量，默认等于 CPU 核心数
    parser.add_argument('-Q', '--queues', default=','.join(settings.ALL_QUEUES))
    parser.add_argument('--prefetch-multiplier', default='')
    parser.add_argument('-f', '--logfile', default='')
    parser.add_argument('-p', '--port', default='')
    parser.add_argument('--basic-auth', default='{}:{}'.format(settings.MONITOR_USERNAME, settings.MONITOR_PASSWORD))
    args, unknown_args = parser.parse_known_args()
    logfile = args.logfile or f'logs/{args.mode}.log'
    log_filter.add_file_handler(logfile, args.loglevel)
    celery_argv = ['celery'] if celery.__version__ < '5.2.0' else []

    host = os.environ.get('HOST') or '0.0.0.0'
    port = int(os.environ.get('PORT') or '8000')
    if args.port:  # 端口号，优先级： 启动参数 -> 环境变量 -> 默认值
        port = int(args.port)

    if args.mode == 'worker':
        celery_argv += ['worker', '-l', args.loglevel, '--pool', args.pool, '-Q', args.queues]
        if args.concurrency:
            celery_argv += ['-c', args.concurrency]
        if args.prefetch_multiplier:
            celery_argv += ['--prefetch-multiplier', args.prefetch_multiplier]
        celery_app.start(argv=celery_argv + unknown_args)
    elif args.mode == 'beat':
        celery_app.start(argv=celery_argv + ['beat', '-l', args.loglevel] + unknown_args)
    elif args.mode == 'monitor':
        celery_app.start(argv=celery_argv + ['flower', '--basic-auth=' + args.basic_auth,
                                             '--address=' + host, '--port=' + str(port)])


if __name__ == '__main__':
    run()
