# -*- coding:utf-8 -*-
# @FileName  :logger_config.py
# @Time      :2024/7/16 22:07
# @Author    :wsk

from loguru import logger

def setup_logger():
    logger.remove()  # 移除之前的所有配置，避免重复配置
    logger.add("logs/all_logs.log",
               rotation="100 MB",  # 每当日志文件达到 100 MB 时，新建一个日志文件
               retention=100,      # 保留最多 100 个日志文件
               compression="zip",  # 压缩旧的日志文件为 zip
               enqueue=True,       # 确保所有日志写入同一个文件
            #    backtrace=True,     # 捕捉回溯信息
            #    diagnose=True
               )      # 捕捉诊断信息
    return logger

# 导出 setup_logger 函数
__all__ = ["setup_logger"]
