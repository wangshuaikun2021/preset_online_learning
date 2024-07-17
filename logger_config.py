# -*- coding:utf-8 -*-
# @FileName  :logger_config.py
# @Time      :2024/7/16 22:07
# @Author    :wsk

# logger_config.py
from loguru import logger

# 配置loguru
logger.add("logs/log_{time}.log",
           rotation="100 MB",  # 每当日志文件达到 100 MB 时，新建一个日志文件
           retention=100,      # 保留最多 100 个日志文件
           compression="zip")  # 压缩旧的日志文件为zip

# 导出 logger
# __all__ = ["logger"]
