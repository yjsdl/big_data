# -*- coding: utf-8 -*-
# @date：2025/1/7 14:01
# @Author：LiuYiJie
# @file： setting


# 日志配置
LOG_NAME = None
LOG_PATH = 'logs/%s.log'
LOG_TO_FILE = True  # 是否启动日志输出到文件
LOG_MODE = 'a'  # 日志输出到文件的方式
LOG_ENCODING = 'utf-8'  # 日志输出到文件的编码
LOG_LEVEL_CONSOLE = 'DEBUG'  # 日志输出到控制台的等级
LOG_LEVEL_FILE = 'INFO'  # 日志输出到文件的等级
LOG_FILE_SIZE = 10 * 1024 * 1024  # 每个日志文件大小
LOG_BACKUP_COUNT = 10  # 保留日志文件数
LOG_FILTER_ENABLED = True  # 是否启动日志过滤
LOG_SPLIT_ERROR = False  # 设置为 True 则开启错误日志分离
LOG_JSON_OUTPUT = False  # 设置为 True 则支持 JSON 格式输出，日志文件以json显示
LOG_FILTER_PYPI = [
    'pymysql',
    'pymongo',
    'redis',
    'kafka'
]
# 要过滤掉日志中包含这些关键词的日志项
LOG_FILTER_KEYWORDS = [
    'Connecting to database',
    'sqlalchemy',
    'pymysql',
    'SELECT',
    'pymongo',
]
