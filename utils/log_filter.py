# -*- coding: utf-8 -*-

"""
Log Filter, color and disable head method
"""

import os
import sys
import time
import datetime
import decimal
import uuid
import logging
import logging.config
from logging.handlers import TimedRotatingFileHandler as fileHandler

from celery.signals import after_setup_logger, after_setup_task_logger

DEBUG = os.environ.get('DEBUG', '').lower() in ('true', '1')

LOG_MIN = 50  # 嵌套日志的最短长度，过短会导致无限递归截取。
# 日志里各参数的最大长度限制
LOG_PARAM_LEN = int(os.environ.get('LOG_PARAM_LEN') or 200)
# 数据库日志的日志级别: DEBUG=10, INFO=20, WARNING=30, ERROR=40, CRITICAL=50
DB_LOG_LEVEL = int(os.environ.get('DB_LOG_LEVEL') or 40)

_FORMAT = '[%(asctime)s] [%(module)s.%(funcName)s:%(lineno)s] %(levelname)s: %(message)s'
_formatter = logging.Formatter(_FORMAT)
_LEVEL = logging.DEBUG if DEBUG else logging.INFO
logging.basicConfig(level=_LEVEL, format=_FORMAT)

logger = logging.root
logger.setLevel(_LEVEL)


def short_log(value, length=None):
    """日志截取，避免日志过长"""
    length = length or LOG_PARAM_LEN
    if not length:
        return value
    if value is None:
        return None
    length = max(length, LOG_MIN)  # 长度不能无限小
    if not isinstance(value, str):
        _value = str(value)
        # 只有长度超过的，才转换类型，否则不轻易转换。避免格式化错误。
        if len(_value) > length:
            value = _value
    if isinstance(value, str) and len(value) > length:
        value = value[0:length // 2] + '...' + value[-length // 2:]
    return value


def deep_short_log(value, length=None):
    """
    将 list,tuple,set,dict 等类型里面的字符截短
    :param value: 将要被转码的值,类型可以是:dict,list,tuple,set 等类型
    :param length: 截取长度
    :return: 尽量返回原本的参数类型(list,tuple,set,dict等类型会保持不变)，但参数过长则会变成字符串类型截取
    """
    length = length or LOG_PARAM_LEN
    if not length:
        return value
    if not value:
        return value
    length = max(length, LOG_MIN)  # 长度不能无限小
    # str/bytes 类型的
    if isinstance(value, (str, bytes, bytearray)):
        return short_log(value, length=length)
    # 不会超出长度的类型，直接返回(这里认为设置的长度不小于100)
    elif isinstance(value, (bool, float, complex, uuid.UUID, time.struct_time, datetime.datetime, datetime.date)):
        return value
    # 这些数值也有可能几百位，超出长度限制
    elif isinstance(value, (int, decimal.Decimal)):
        return short_log(value, length=length)
    # list,tuple,set 类型,递归转换
    elif isinstance(value, (list, tuple, set)):
        arr = [deep_short_log(item, length=length // 2) for item in value]
        # 尽量不改变原类型
        if isinstance(value, list): return arr
        if isinstance(value, tuple): return tuple(arr)
        if isinstance(value, set): return set(arr)
    # dict 类型,递归转换(字典里面的 key 也会转成 unicode 编码)
    elif isinstance(value, dict):
        this_value = {}  # 不能改变原参数
        for key1, value1 in value.items():
            # 字典里面的 key 也转成 unicode 编码
            key1 = deep_short_log(key1, length=length // 2)
            this_value[key1] = deep_short_log(value1, length=length // 2)
        return this_value
    # 其它类型
    else:
        return short_log(value, length=length)


class StringFilter(logging.Filter):
    """用于截取日志的字符串，避免日志内容过长
    对于内嵌的 dict、list 等，会嵌套截取长度，每嵌套一层则长度限制变短一倍
    """

    def filter(self, record):
        """当日志的输出字符串超过指定长度，则截取"""
        msg = record.msg
        # 已处理过，不再处理
        if hasattr(record, '_filter_msg'):
            return record._filter_msg
        else:
            # 保存原值
            record.old_msg = record.getMessage()
        if isinstance(msg, (bytes, bytearray)):
            msg = msg.decode()
        elif not isinstance(msg, str):
            try:
                msg = str(msg)
            except Exception as e:
                logging.exception('日志值类型格式化错误:%s, %s', e, msg)
                record._filter_msg = False
                return False  # 报异常就别再打印此日志了
        if isinstance(msg, str) and '%' in msg and record.args:
            try:
                args = record.args
                # list,tuple,set,dict 类型,递归转换
                if isinstance(args, (list, tuple, set, dict)):
                    args = deep_short_log(args, length=LOG_PARAM_LEN)
                # 字符串合并
                msg %= args
                record.args = ()
            # 捕获未知错误，有可能日志里包含二进制、错误编码等
            except Exception as e:
                logging.exception('日志参数传递错误:%s, %s', e, msg)
                record._filter_msg = False
                return False  # 报异常就别再打印此日志了
        # 字符串处理完毕
        record.msg = short_log(msg, length=LOG_PARAM_LEN * 3)
        record._filter_msg = True
        return True


class LevelFilter(object):
    """日志level过滤
    用于同时有两种输出的屏幕日志(sys.stdout和sys.stderr)
    限制sys.stdout的error级别输出，避免两种日志重复输出
    """
    def __init__(self, min_level, max_level=logging.CRITICAL):
        """指定允许输出的日志级别"""
        self.min_level = min_level
        self.max_level = max_level

    def filter(self, record):
        """限制日志的输出级别"""
        return self.min_level <= record.levelno <= self.max_level


string_filter = StringFilter()
logger.addFilter(string_filter)
# 排除屏幕输出(StandardErrorHandler)
logger.handlers[:] = [h for h in logger.handlers if not isinstance(h, logging.StreamHandler)]

# 屏幕普通日志(stdout)
stdout_handler = logging.StreamHandler(sys.stdout)  # stdout
stdout_handler.setFormatter(_formatter)
stdout_handler.setLevel(_LEVEL)
stdout_handler.addFilter(string_filter)
stdout_handler.addFilter(LevelFilter(_LEVEL, logging.WARNING))
logger.addHandler(stdout_handler)

# 屏幕报错日志(stderr)
stderr_handler = logging.StreamHandler(sys.stderr)  # stderr
stderr_handler.setFormatter(_formatter)
stderr_handler.setLevel(logging.ERROR)
logger.addHandler(stderr_handler)

# 文件日志
file_handler = None


@after_setup_task_logger.connect()
def task_logger_setup_handler(*args, **kwargs):
    """
    worker logger
    """
    if file_handler:
        logger.addHandler(file_handler)
    logger.addHandler(stdout_handler)
    logger.addHandler(stderr_handler)

    logging.info("task log handler connected -> Global Logging")


@after_setup_logger.connect()
def global_logger_setup_handler(*args, **kwargs):
    """
    添加 celery 的 logger，它会清除之前的所有 log Handler，需要这里重新添加一次
    """
    # 排除屏幕输出(StandardErrorHandler)
    logger.handlers[:] = [h for h in logger.handlers if not isinstance(h, logging.StreamHandler)]

    if file_handler:
        logger.addHandler(file_handler)
    logger.addHandler(stdout_handler)
    logger.addHandler(stderr_handler)

    logging.info("celery log handler connected -> Global Logging")


def add_file_handler(log_file, logger_level, append=not DEBUG, backup_count=30, formatter=_formatter):
    """
    给 logger 加上 文件 日志处理
    :param {string} log_file: 日志文件的名称
    :param {bool | string} logger_level: 日志级别,默认级别:INFO
    :param {bool} append: 是否追加日志，默认为 True (追加到旧日志文件后面)， 设置为 False 时会先删除旧日志文件
    :param {int} backup_count: 日志文件保留天数
    :param {logging.Formatter} formatter: 日志输出格式
    """
    global file_handler
    if not log_file: return

    logger_level = logger_level.upper() if isinstance(logger_level, str) else logger_level
    logger.setLevel(logger_level)
    log_file = os.path.abspath(log_file)
    log_path = os.path.dirname(log_file)
    # 没有日志文件的目录，则先创建目录，避免因此报错
    if not os.path.isdir(log_path):
        os.makedirs(log_path)
    # 不追加日志，则先清空旧日志文件
    if append is False and os.path.isfile(log_file):
        try:
            open(log_file, mode="w").close()
        except:
            os.popen('echo""> "%s"' % log_file)
    # 添加日志处理器
    file_handler = fileHandler(log_file, when='midnight', backupCount=backup_count)
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logger_level)
    file_handler.addFilter(string_filter)
    logger.addHandler(file_handler)

