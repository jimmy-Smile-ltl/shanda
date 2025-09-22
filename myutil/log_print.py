# -*- coding: utf-8 -*-
# @Time    : 2025/7/8 10:30
# @Author  : Jimmy Smile
# @Project : 北大信研院
# @File    : log_print.py
# @Software: PyCharm
import logging
import logging.handlers
import os
import sys


class LogPrint:
    """
    一个融合了“即用性”与“灵活性”的日志类。
    - 开箱即用：默认配置控制台和文件日志。
    - 配置简单：通过参数控制默认行为。
    - 功能强大：支持链式调用添加更多自定义处理器。
    - 使用便捷：实例本身即可调用 .log(), .info() 等方法。
    """
    _instance = None

    def __new__(cls, *args, **kwargs):
        """
        单例模式，确保整个应用中只有一个日志配置实例。
        """
        if cls._instance is None:
            cls._instance = super(LogPrint, cls).__new__(cls)
        return cls._instance

    def __init__(self,
                 name: str = "logger",
                 log_dir: str = 'logs',
                 console_level: int = logging.INFO,
                 file_level: int = logging.DEBUG,  # 控制台和文件的日志级别，忽略小于的
                 save_to_file: bool = True,  # 是否保存到文件
                 backup_count: int = 7  # 文件日志的备份数量，默认保留7个旧日志文件 多的将会被删除
                 ):
        """
        智能的构造函数：完成默认配置，同时为高级定制做准备。
        """
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.DEBUG)  # 总开关设置为最低，由 Handler 过滤

        # 初始化时清除已有 handlers，防止重复配置
        if self.logger.hasHandlers():
            self.logger.handlers.clear()

        self.formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )

        # 1. 添加默认的控制台 Handler
        self.add_console_handler(console_level)

        # 2. 根据参数，添加默认的文件 Handler
        if save_to_file:
            log_file_name = f"{name.lower()}.log"
            log_path = os.path.join(log_dir, log_file_name)
            self.add_timed_rotating_file_handler(
                file_name=log_path,
                level=file_level,
                backup_count=backup_count
            )

    def add_console_handler(self, level: int = logging.INFO):
        """
        【可链式调用】添加一个输出到控制台的 Handler。
        """
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(level)
        console_handler.setFormatter(self.formatter)
        self.logger.addHandler(console_handler)
        return self

    def add_timed_rotating_file_handler(
            self,
            file_name: str,
            level: int = logging.DEBUG,
            backup_count: int = 7,
            when: str = 'D',
            interval: int = 1
    ):
        """
        【可链式调用】添加一个输出到文件、并能定时轮换的 Handler。
        """
        log_dir = os.path.dirname(file_name)
        if log_dir and not os.path.exists(log_dir):
            os.makedirs(log_dir)

        file_handler = logging.handlers.TimedRotatingFileHandler(
            filename=file_name,
            when=when,
            interval=interval,
            backupCount=backup_count,
            encoding='utf-8'
        )
        file_handler.setLevel(level)
        file_handler.setFormatter(self.formatter)
        self.logger.addHandler(file_handler)
        return self

    # --- 便捷调用方法 ---
    def log(self, message: str, *args, **kwargs):
        """便捷方法，等同于 info 级别。"""
        self.logger.info(message, *args, **kwargs)

    def debug(self, message: str, *args, **kwargs):
        self.logger.debug(message, *args, **kwargs)

    def info(self, message: str, *args, **kwargs):
        self.logger.info(message, *args, **kwargs)

    def warning(self, message: str, *args, **kwargs):
        self.logger.warning(message, *args, **kwargs)

    def error(self, message: str, *args, **kwargs):
        self.logger.error(message, *args, **kwargs)

    def critical(self, message: str, *args, **kwargs):
        self.logger.critical(message, *args, **kwargs)

    def print(self, message: str, level: int = logging.INFO, *args, **kwargs):
        """
        【新增】打印并保存日志，可以动态指定级别，默认为 INFO。
        这是最灵活的日志记录方法。
        """
        self.logger.log(level, message, *args, **kwargs)


# logging.NOTSET	0	当在日志记录器上设置时，表示将查询上级日志记录器以确定生效的级别。如果仍被解析为 NOTSET，则会记录所有事件。在处理器上设置时，所有事件都将被处理。
# logging.DEBUG	10	详细的信息，通常只有试图诊断问题的开发人员才会感兴趣。
# logging.INFO	20	确认程序按预期运行。
# logging.WARNING	30	表明发生了意外情况，或近期有可能发生问题（例如‘磁盘空间不足’）。软件仍会按预期工作。
# logging.ERROR	40	由于严重的问题，程序的某些功能已经不能正常执行。
# logging.CRITICAL	50	严重的错误，表明程序已不能继续执行
# -*- coding: utf-8 -*-

# 将对齐后的 Markdown 表格赋值给一个多行字符串变量
markdown_table_string = """
| 调用方法            | 消息级别 (数值)   | 是否打印到控制台？<br>(判断: >= `INFO` / 20) | 是否写入文件？<br>(判断: >= `DEBUG` / 10) |
| :------------------ | :---------------- | :------------------------------------------: | :---------------------------------------: |
| `logger.debug(...)` | **DEBUG** (10)    | **否** (10 < 20)                             | **是** (10 >= 10)                           |
| `logger.info(...)`  | **INFO** (20)     | **是** (20 >= 20)                            | **是** (20 >= 10)                           |
| `logger.warning(...)` | **WARNING** (30)  | **是** (30 >= 20)                            | **是** (30 >= 10)                           |
| `logger.error(...)`   | **ERROR** (40)    | **是** (40 >= 20)                            | **是** (40 >= 10)                           |
| `logger.critical(...)`| **CRITICAL** (50) | **是** (50 >= 20)                            | **是** (50 >= 10)                           |
"""
