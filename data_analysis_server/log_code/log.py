# -*- coding: utf-8 -*-
# @date：2023/12/19 13:55
# @Author：LiuYiJie
# @file： log
"""
设置日志
"""
import os
from os.path import dirname, abspath, join
import logging
from logging import handlers
import data_analysis_server.setting as setting
import datetime


class ColoredFormatter(logging.Formatter):
    COLORS = {
        "black": "\033[40m",  # 黑色
        "red": "\033[91m",  # 红色
        "green": "\033[92m",  # 绿色
        "yellow": "\033[93m",  # 黄色
        "blue": "\033[34m",  # 蓝色
        "purple": "\033[95m",  # 紫色
        "dgreen": "\033[96m",  # 深绿
        "white": "\033[97m",  # 白色
        "reset": '\033[0m',  # 默认
        "bold": "\033[1m",  # 加粗
    }

    DEFAULT_STYLES = {
        "spam": COLORS['green'] + COLORS['bold'],
        "DEBUG": COLORS['blue'] + COLORS['bold'],
        "verbose": COLORS['blue'],
        "INFO": COLORS['white'] + COLORS['bold'],
        "WARNING": COLORS['yellow'] + COLORS['bold'],
        "success": COLORS['green'] + COLORS['bold'],
        "ERROR": COLORS['red'] + COLORS['bold'],
        "CRITICAL": COLORS['red'] + COLORS['bold'],
        "EXCEPTION": COLORS['red'] + COLORS['bold'],

        "asctime": COLORS['green'],
        "message": COLORS['green'] + COLORS['bold'],
        "lineno": COLORS['purple'],
        "threadName": COLORS['red'],
        "module": COLORS['red'],
        "levelname": COLORS['white'] + COLORS['bold'],
        "name": COLORS['blue'],
        "default": COLORS['blue'],
    }

    def __init__(self, styles=None):
        super().__init__()
        self.styles = styles or self.DEFAULT_STYLES

    def set_color(self, levelname: str = None):
        return self.styles.get(levelname, "reset")

    def format(self, record):
        levelname = record.levelname
        asctime = f"{self.styles.get('asctime')}{datetime.datetime.fromtimestamp(record.created).strftime('%Y-%m-%d %H:%M:%S')}{self.COLORS['reset']}"
        threadName = f"{self.styles.get('default')}{record.threadName}{self.COLORS['reset']}"
        pathname = f"{self.styles.get('default')}{record.pathname}{self.COLORS['reset']}"
        lineno = f"{self.styles.get('lineno')}{record.lineno}{self.COLORS['reset']}"
        funcName = f"{self.styles.get('default')}{record.funcName}{self.COLORS['reset']}"
        module = f"{self.styles.get('default')}{record.module}{self.COLORS['reset']}"
        message = super().format(record)

        levelcolor = self.set_color(levelname)
        levelname = f"{levelcolor}{levelname}{self.COLORS['reset']}"
        message = f"{levelcolor}{message}{self.COLORS['reset']}"

        formatted_message = f"{asctime} - {levelname} - {module}:{funcName}:{lineno} - {message}"
        return formatted_message


class ColoredConsoleHandler(logging.StreamHandler):
    def __init__(self, formatter=None):
        super().__init__()
        self.formatter = formatter or ColoredFormatter()


def setup_logger():
    log_obj = logging.getLogger()
    log_obj.setLevel(logging.INFO)
    log_obj.handlers.clear()

    # 自定义输出格式，控制台输出
    console_handler = ColoredConsoleHandler()
    console_handler.setLevel(logging.INFO)
    log_obj.addHandler(console_handler)

    log_to_file = setting.LOG_TO_FILE
    log_name = setting.LOG_NAME or "log_file"
    log_path = setting.LOG_PATH % log_name
    if log_to_file:
        root_dir = dirname(dirname(abspath(__file__)))
        log_file_path = join(root_dir, log_path)
        os.makedirs(dirname(log_file_path), exist_ok=True)

        # 文件输出，每天一个文件
        file_handler = handlers.TimedRotatingFileHandler(
            log_file_path,
            when="midnight",
            interval=1,
            backupCount=10,
            encoding='utf-8')

        file_handler.setLevel(logging.INFO)
        file_handler.suffix = "%Y-%m-%d.log"
        file_handler.setFormatter(logging.Formatter(
            '%(asctime)s - %(module)s - %(funcName)s:%(lineno)d - %(levelname)s - %(message)s'))
        log_obj.addHandler(file_handler)

    return log_obj


logger = setup_logger()