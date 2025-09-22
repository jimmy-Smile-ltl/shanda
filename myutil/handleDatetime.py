# -*- coding: utf-8 -*-
# @Time    : 2025/6/24 15:35
# @Author  : Jimmy Smile
# @Project : 北大信研院
# @File    : handleDatetime.py
# @Software: PyCharm
from datetime import datetime, timedelta

import dateparser
from dateutil import parser  # 导入 dateutil 的解析器


def convert_date_robust(date_str: str) -> str | None:
    """
    Robustly convert various date string formats to a 'YYYY-MM-DD' string.
    Handles formats like:
    - 'November 15, 2024'
    - 'Mar 1, 2022'
    - Timezone-aware strings like 'Jun 24, 2025 15:30:00 +0800'
    """
    # 默认的最不可能的时间
    # default_date = datetime(1900, 1, 1, 0, 0, 0)
    # default_date_str = default_date.strftime("%Y-%m-%d %H:%M:%S")
    # 设置一个假时间，影响后续数据清洗，空字符，插入报错，None,插入正常
    default_date_str = None
    if not isinstance(date_str, str):
        print(f"Error: Input must be a string, but got {type(date_str)} value {date_str}")
        return default_date_str
    try:
        # parser.parse() 能智能解析大多数日期格式
        dt_object = parser.parse(date_str)
        # .strftime() 将 datetime 对象格式化为所需的字符串格式
        return dt_object.strftime("%Y-%m-%d %H:%M:%S")
    except (parser.ParserError, ValueError) as e:
        # 如果 dateutil 也无法解析，则捕获异常
        # print(f"Error converting date string '{date_str}': {e}")
        try:
            # 处理昨天 几分钟前 这种
            now = datetime.now()
            dt_object = dateparser.parse(date_str, settings={'RELATIVE_BASE': now})
            return dt_object.strftime("%Y-%m-%d %H:%M:%S")
        except Exception as e:
            return default_date_str


class HandleDatetime:
    @staticmethod
    def get_current_datetime():
        """获取当前时间"""
        return datetime.now()

    @staticmethod
    def format_datetime(dt, fmt="%Y-%m-%d %H:%M:%S"):
        """格式化时间"""
        return dt.strftime(fmt)

    @staticmethod
    def parse_datetime(date_str, fmt="%Y-%m-%d %H:%M:%S"):
        """解析字符串为时间"""
        return datetime.strptime(date_str, fmt)

    @staticmethod
    def add_days(dt, days):
        """在时间上加上指定天数"""
        return dt + timedelta(days=days)

    @staticmethod
    def convert_date_robust(date_str: str) -> str | None:
        """
        Robustly convert various date string formats to a 'YYYY-MM-DD' string.
        Handles formats like:
        - 'November 15, 2024'
        - 'Mar 1, 2022'
        - Timezone-aware strings like 'Jun 24, 2025 15:30:00 +0800'
        """
        # 默认的最不可能的时间
        # default_date = datetime(1900, 1, 1, 0, 0, 0)
        # default_date_str = default_date.strftime("%Y-%m-%d %H:%M:%S")
        # 设置一个假时间，影响后续数据清洗，空字符，插入报错，None,插入正常
        default_date_str = None
        if not isinstance(date_str, str):
            print(f"handleDatetime  Error: Input must be a string, but got {type(date_str)} value {date_str}")
            return default_date_str
        try:
            # parser.parse() 能智能解析大多数日期格式
            dt_object = parser.parse(date_str)
            # .strftime() 将 datetime 对象格式化为所需的字符串格式
            return dt_object.strftime("%Y-%m-%d %H:%M:%S")
        except (parser.ParserError, ValueError) as e:
            # 如果 dateutil 也无法解析，则捕获异常
            # print(f"Error converting date string '{date_str}': {e}")
            try:
                # 处理昨天 几分钟前 这种
                now = datetime.now()
                dt_object = dateparser.parse(date_str, settings={'RELATIVE_BASE': now})
                return dt_object.strftime("%Y-%m-%d %H:%M:%S")
            except Exception as e:
                return default_date_str
    # def convert_date_robust(date_str: str) -> str | None:
    #     """
    #     Robustly convert various date string formats to a 'YYYY-MM-DD' string.
    #     Handles formats like:
    #     - 'November 15, 2024'
    #     - 'Mar 1, 2022'
    #     - Timezone-aware strings like 'Jun 24, 2025 15:30:00 +0800'
    #     """
    #     # 默认的最不可能的时间
    #     # default_date = datetime(1900, 1, 1, 0, 0, 0)
    #     # default_date_str = default_date.strftime("%Y-%m-%d %H:%M:%S")
    #     # 设置一个假时间，影响后续数据清洗，空字符，插入报错，None,插入正常
    #     default_date_str = None
    #     if not isinstance(date_str, str):
    #         print(f"Error: Input must be a string, but got {type(date_str)}")
    #         return default_date_str
    #     try:
    #         # parser.parse() 能智能解析大多数日期格式
    #         dt_object = parser.parse(date_str)
    #         # .strftime() 将 datetime 对象格式化为所需的字符串格式
    #         return dt_object.strftime("%Y-%m-%d %H:%M:%S")
    #     except (parser.ParserError, ValueError) as e:
    #         # 如果 dateutil 也无法解析，则捕获异常
    #         # print(f"Error converting date string '{date_str}': {e}")
    #         return default_date_str

# if __name__ == '__main__':
#     handle = HandleDatetime()
#     print(handle.convert_date_robust("07 January 2010;"))
#     print(handle.convert_date_robust("07 January 2010."))