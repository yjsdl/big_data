# -*- coding: utf-8 -*-
# @date：2023/12/19 13:55
# @Author：LiuYiJie
# @file： log
"""
设置日志
"""
import os
import json
import logging
from logging.handlers import RotatingFileHandler
import datetime
import module_server.setting as setting


class ColoredFormatter(logging.Formatter):
    COLORS = {
        "black": "\033[30m",
        "gray": "\033[90m",
        "red": "\033[31m",
        "green": "\033[32m",
        "yellow": "\033[33m",
        "blue": "\033[34m",
        "purple": "\033[35m",
        "dGreen": "\033[38;5;22m",
        "bCyan": "\033[38;5;117m",
        "cyan": "\033[36m",
        "white": "\033[38;5;251m",
        "reset": '\033[0m',
        "bold": "\033[1m",
    }

    DEFAULT_STYLES = {
        "DEBUG": COLORS['blue'] + COLORS['bold'],
        "INFO": COLORS['white'] + COLORS['bold'],
        "WARNING": COLORS['yellow'] + COLORS['bold'],
        "ERROR": COLORS['red'] + COLORS['bold'],
        "CRITICAL": COLORS['red'] + COLORS['bold'],
        "ascTime": COLORS['green'] + COLORS['bold'],
        "lineno": COLORS['cyan'] + COLORS['bold'],
        "funcName": COLORS['cyan'] + COLORS['bold'],
        "module": COLORS['cyan'] + COLORS['bold'],
        "levelName": COLORS['white'] + COLORS['bold'],
        "default": COLORS['blue'],
    }

    def __init__(self, styles=None):
        super().__init__()
        self.styles = styles or self.DEFAULT_STYLES

    def format(self, record):
        levelName = record.levelname
        ascTime = f"{self.styles.get('ascTime')}" \
                  f"{datetime.datetime.fromtimestamp(record.created).strftime('%Y-%m-%d %H:%M:%S')}" \
                  f"{self.COLORS['reset']}"
        lineno = f"{self.styles.get('lineno')}{record.lineno}{self.COLORS['reset']}"
        funcName = f"{self.styles.get('funcName')}{record.funcName}{self.COLORS['reset']}"
        module = f"{self.styles.get('module')}{record.module}{self.COLORS['reset']}"
        message = super().format(record)

        levelColor = self.styles.get(levelName, "")
        levelName = f"{levelColor}{levelName}{self.COLORS['reset']}"
        message = f"{levelColor}{message}{self.COLORS['reset']}"

        return f"{ascTime} - {levelName} - {module}:{funcName}:{lineno} - {message}"


class JSONFormatter(logging.Formatter):
    def format(self, record):
        log_record = {
            "time": datetime.datetime.fromtimestamp(record.created).strftime('%Y-%m-%d %H:%M:%S'),
            "level": record.levelname,
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
            "message": record.getMessage(),
        }
        return json.dumps(log_record, ensure_ascii=False)


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


class KeywordFilter(logging.Filter):
    def __init__(self, keywords):
        super().__init__()
        self.keywords = keywords

    def filter(self, record):
        return not any(keyword in record.getMessage() for keyword in self.keywords)


class ErrorOnlyFilter(logging.Filter):
    def filter(self, record):
        return record.levelno >= logging.ERROR


class LoggerManager:
    def __init__(self):
        self.logger = logging.getLogger()
        self.logger.setLevel(logging.DEBUG)
        self.logger.handlers.clear()

        self.formatter = self.get_formatter()
        self.setup_handlers()

    def setup_handlers(self):
        self.setup_console_handler()

        if setting.LOG_TO_FILE:
            self.setup_main_file_handler()
            if getattr(setting, 'LOG_SPLIT_ERROR', True):
                self.setup_error_file_handler()

    def setup_console_handler(self):
        console_handler = ColoredConsoleHandler()
        console_handler.setLevel(setting.LOG_LEVEL_CONSOLE)

        if setting.LOG_FILTER_ENABLED:
            for module in setting.LOG_FILTER_PYPI:
                logging.getLogger(module).setLevel(logging.WARNING)
            keyword_filter = KeywordFilter(setting.LOG_FILTER_KEYWORDS)
            console_handler.addFilter(keyword_filter)

        self.logger.addHandler(console_handler)

    def setup_main_file_handler(self):
        log_name = setting.LOG_NAME or self.get_script_name()
        log_path = setting.LOG_PATH % log_name

        file_handler = CustomRotatingFileHandler(
            log_path,
            mode=setting.LOG_MODE,
            maxBytes=setting.LOG_FILE_SIZE,
            backupCount=setting.LOG_BACKUP_COUNT,
            encoding=setting.LOG_ENCODING
        )
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(self.formatter)
        if getattr(setting, 'LOG_SPLIT_ERROR', True):
            # 如果启用单独错误日志，则主日志只记录低于ERROR级别
            file_handler.addFilter(self.exclude_errors_filter())

        self.logger.addHandler(file_handler)

    def setup_error_file_handler(self):
        log_name = setting.LOG_NAME or self.get_script_name()
        error_path = setting.LOG_PATH % f"{log_name}_error"

        error_handler = CustomRotatingFileHandler(
            error_path,
            mode=setting.LOG_MODE,
            maxBytes=setting.LOG_FILE_SIZE,
            backupCount=setting.LOG_BACKUP_COUNT,
            encoding=setting.LOG_ENCODING
        )
        error_handler.setLevel(logging.ERROR)
        error_handler.setFormatter(self.formatter)

        self.logger.addHandler(error_handler)

    @staticmethod
    def get_formatter():
        if setting.LOG_JSON_OUTPUT:
            return JSONFormatter()
        else:
            return logging.Formatter(
                '%(asctime)s - %(levelname)s - %(module)s - %(funcName)s:%(lineno)d - %(message)s'
            )

    @staticmethod
    def exclude_errors_filter():
        class ExcludeErrorsFilter(logging.Filter):
            def filter(self, record):
                return record.levelno < logging.ERROR
        return ExcludeErrorsFilter()

    @staticmethod
    def get_script_name():
        try:
            return os.path.splitext(os.path.basename(__import__("__main__").__file__))[0]
        except AttributeError:
            return "log_file"

    def get_logger(self):
        return self.logger


logger = LoggerManager().get_logger()
