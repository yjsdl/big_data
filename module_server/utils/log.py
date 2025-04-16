# -*- coding: utf-8 -*-
# @date：2023/12/19 13:55
# @Author：LiuYiJie
# @file： log
"""
设置日志
"""
import os
import logging
from logging.handlers import RotatingFileHandler
import module_server.setting as setting
import datetime


class ColoredFormatter(logging.Formatter):
    COLORS = {
        "black": "\033[30m",  # 黑色
        "gray": "\033[90m",  # 深灰色
        "red": "\033[31m",  # 红色
        "green": "\033[32m",  # 绿色
        "yellow": "\033[33m",  # 黄色
        "blue": "\033[34m",  # 蓝色
        "purple": "\033[35m",  # 洋红色
        "dGreen": "\033[38;5;22m",  # 暗绿色
        "bCyan": "\033[38;5;117m",  # 亮青色
        "cyan": "\033[36m",  # 青色
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
        "ascTime": COLORS['green'] + COLORS['bold'],
        "message": COLORS['green'],
        "lineno": COLORS['cyan'] + COLORS['bold'],
        "threadName": COLORS['red'],
        "funcName": COLORS['cyan'] + COLORS['bold'],
        "module": COLORS['cyan'] + COLORS['bold'],
        "levelName": COLORS['white'] + COLORS['bold'],
        "name": COLORS['blue'],
        "default": COLORS['blue'],
    }

    def __init__(self, styles=None):
        super().__init__()
        self.styles = styles or self.DEFAULT_STYLES

    def set_color(self, levelName: str = None):
        return self.styles.get(levelName, "reset")

    def format(self, record):
        levelName = record.levelname
        ascTime = f"{self.styles.get('ascTime')}" \
                  f"{datetime.datetime.fromtimestamp(record.created).strftime('%Y-%m-%d %H:%M:%S')}" \
                  f"{self.COLORS['reset']}"
        threadName = f"{self.styles.get('default')}{record.threadName}{self.COLORS['reset']}"
        pathname = f"{self.styles.get('default')}{record.pathname}{self.COLORS['reset']}"
        lineno = f"{self.styles.get('lineno')}{record.lineno}{self.COLORS['reset']}"
        funcName = f"{self.styles.get('funcName')}{record.funcName}{self.COLORS['reset']}"
        module = f"{self.styles.get('module')}{record.module}{self.COLORS['reset']}"
        message = super().format(record)

        levelColor = self.set_color(levelName)
        levelName = f"{levelColor}{levelName}{self.COLORS['reset']}"
        message = f"{levelColor}{message}{self.COLORS['reset']}"

        formatted_message = f"{ascTime} - {levelName} - {module}:{funcName}:{lineno} - {message}"
        return formatted_message


class ColoredConsoleHandler(logging.StreamHandler):
    def __init__(self, formatter=None):
        super().__init__()
        self.formatter = formatter or ColoredFormatter()


class CustomRotatingFileHandler(RotatingFileHandler):
    def __init__(self, filename, mode='a', maxBytes=0, backupCount=0, encoding=None, delay=False):
        super().__init__(filename, mode=mode, maxBytes=maxBytes, backupCount=backupCount,
                         encoding=encoding, delay=delay)
        self.stream = None

    def doRollover(self):
        if self.stream:
            self.stream.close()

        for i in reversed(range(1, self.backupCount)):
            sfn = f"{self.baseFilename}{i}.log"
            dfn = f"{self.baseFilename}{i + 1}.log"
            if os.path.exists(sfn):
                os.rename(sfn, dfn)

        if os.path.exists(self.baseFilename):
            os.rename(self.baseFilename, f"{self.baseFilename}1.log")

        self.stream = self._open()


def setup_logger():
    log_obj = logging.getLogger()
    log_obj.setLevel(logging.DEBUG)
    log_obj.handlers.clear()

    # 自定义输出格式，控制台输出
    console_handler = ColoredConsoleHandler()
    console_handler.setLevel(setting.LOG_LEVEL_CONSOLE)
    log_obj.addHandler(console_handler)

    log_to_file = setting.LOG_TO_FILE
    log_name = setting.LOG_NAME or "log_file"
    log_path = setting.LOG_PATH % log_name
    log_mode = setting.LOG_MODE
    log_max_bytes = setting.LOG_FILE_SIZE
    log_backup_count = setting.LOG_BACKUP_COUNT
    log_encoding = setting.LOG_ENCODING
    log_level_file = setting.LOG_LEVEL_FILE

    if log_to_file:
        # 文件输出
        file_handler = CustomRotatingFileHandler(
            log_path,
            mode=log_mode,
            maxBytes=log_max_bytes,
            backupCount=log_backup_count,
            encoding=log_encoding
        )

        file_handler.setLevel(log_level_file)
        file_handler.suffix = "%Y-%m-%d.log"
        file_handler.setFormatter(logging.Formatter(
            '%(asctime)s - %(module)s - %(funcName)s:%(lineno)d - %(levelname)s - %(message)s'))
        log_obj.addHandler(file_handler)

    return log_obj


logger = setup_logger()
