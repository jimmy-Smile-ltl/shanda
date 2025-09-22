# -*- coding: utf-8 -*-
# @Time    : 2025/7/9 15:18
# @Author  : Jimmy Smile
# @Project : 北大信研院
# @File    : handleJson.py
# @Software: PyCharm
class HandleJson:
    @staticmethod
    def safe_extract(data, path, default=""):
        """
        安全地从嵌套的字典和列表中提取数据。

        :param data: 原始的字典或列表。
        :param path: 一个包含键名和索引的访问路径列表，例如 ['key1', 0, 'key2']。
        :param default: 如果路径中任何一步失败，返回的默认值。
        :return: 提取到的值或默认值。
        """
        current_data = data
        for key in path:
            # 检查当前数据是否可以进行下一步提取
            if isinstance(current_data, dict):
                current_data = current_data.get(key)
            elif isinstance(current_data, list):
                # 只有在索引是整数且列表不为空的情况下才尝试访问
                if isinstance(key, int) and -len(current_data) <= key < len(current_data):
                    current_data = current_data[key]
                else:
                    return default  # 索引无效或类型不匹配
            else:
                return default  # 无法继续深入

            # 如果任何一步返回None，则提前中止并返回默认值
            if current_data is None:
                return default

        return current_data if current_data is not None else default
