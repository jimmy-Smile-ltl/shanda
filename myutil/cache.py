# -*- coding: utf-8 -*-
# @Time    : 2025/5/29 13:29
# @Author  : Jimmy Smile
# @Project : 北大信研院
# @File    : store_page.py
# @Software: PyCharm
import os
import warnings

import redis

warnings.filterwarnings("ignore")

class Cache():
    def __init__(self, key: str):
        # remote_ip = "192.168.141.7"  # 这个ip可能是变化的，但是电脑一直没有关机，所以可以直接写死 wifi server的ip
        # remote_ip = "10.0.30.54"  # 这个ip可能是变化的，但是电脑一直没有关机，所以可以直接写死 有线 server的ip
        # local_ip = "localhost"
        # # 判断是当前是哪台电脑， 根据绝对路径下面的user来判断
        # absolutePath = os.path.abspath(__file__)
        # # 获取当前文件的绝对路径
        # user = absolutePath.split(os.sep)[2]
        # if user == "JimmySmile" or user.find("immy") != -1:
        #     # print("当前电脑是JimmySmile,使用远程Redis服务器")
        #     user_ip = remote_ip
        # elif user == "唐凯":
        #     # print("当前电脑是Jimmmy的工作台，使用本地Redis服务器")
        #     user_ip = local_ip
        # else:
        #     print(
        #         "当前电脑不是JimmySmile,  也不是Jimmy的工作台（唐凯），使用远程Redis服务器，注意在同一局域网内 wifi guest 不行")
        #     user_ip = remote_ip
        # ... 其他初始化 ...
        #
        #
        # self.redis_client = redis.StrictRedis(host=user_ip, port=6379, db=0, decode_responses=True,
        #                                       password="jimmysmile")
        # # TARGET_REDIS_CONFIG = {
        #     'host': '192.168.130.53',
        #     'port': 6379,  # 假设目标 Redis 在不同端口
        #     'db': 1,
        #     'password': "123456"
        # }
        self.redis_client = redis.StrictRedis(host="192.168.130.53", port=6379, db=1, decode_responses=True,
                                              password="123456")

        self.key = key

    def get_redis_client(self):
        """获取Redis客户端"""
        return self.redis_client

    def shutdown(self):
        """关闭Redis连接"""
        try:
            self.redis_client.close()
        except Exception as e:
            print(f"关闭Redis连接失败: {e}")

    def record_int(self, value: int) -> None:
        try:
            self.redis_client.set(self.key, value)
        except Exception as e:
            print(f"记录进度失败: {e}")

    def get_int(self, default=1) -> int:
        try:
            value = self.redis_client.get(self.key)
            return int(value) if value else default
        except Exception as e:
            print(f"获取进度失败: {e}")
            return default

    def record_string(self, value: str) -> None:
        try:
            self.redis_client.set(self.key, value)
        except Exception as e:
            print(f"记录进度失败: {e}")

    def get_string(self, default="") -> str:
        try:
            value = self.redis_client.get(self.key)
            return value if value else default
        except Exception as e:
            print(f"获取进度失败: {e}")
            return default

    def clear_value(self) -> None:

        try:
            self.redis_client.delete(self.key)
        except Exception as e:
            print(f"清除进度失败: {e}")

    def record_list(self, value: list) -> None:
        try:
            if not isinstance(value, list):
                raise ValueError("value must be a list")
            if len(value) == 0:
                raise ValueError("value list cannot be empty")
            self.redis_client.rpush(self.key, *value)
        except Exception as e:
            print(f"记录列表失败: {e}")

    def get_list(self, default=[]) -> list:
        try:
            value = self.redis_client.lrange(self.key, 0, -1)
            return value if value else default
        except Exception as e:
            print(f"获取列表失败: {e}")
            return default

    def append_to_list(self, value: str) -> None:
        """不会报错。 在 Redis 中，rpush 命令如果 key 不存在，会自动创建一个新列表并插入元素。因此，即使该 key 不存在，也能正常追加，不会抛出异常"""
        try:
            self.redis_client.rpush(self.key, value)
        except Exception as e:
            print(f"追加到列表失败: {e}")

    def remove_from_list(self, value: str) -> None:
        try:
            self.redis_client.lrem(self.key, 0, value)
        except Exception as e:
            print(f"从列表中移除失败: {e}")

    def get_list_length(self) -> int:
        try:
            return self.redis_client.llen(self.key)
        except Exception as e:
            print(f"获取列表长度失败: {e}")
            return 0

    def clear_list(self, method: str = 'trim') -> None:
        """
        清空一个Redis列表 (List)。

        提供了两种策略:
        1. 'trim' (默认): 保留键，但将其中的所有元素移除。这是最高效的清空方式。
        2. 'delete': 直接删除整个键。

        :param method: 清空方法，可选值为 'trim' 或 'delete'。
        """
        try:
            if method == 'trim':
                # --- 方案一 (推荐): 保留键，清空内容 ---
                # 使用 LTRIM key 1 0 命令。
                # 这是一个Redis的技巧，通过将列表裁剪为一个不存在的范围，
                # 来达到瞬间清空所有元素的效果，同时保留了键本身。
                # 这对于“重置”一个列表非常有用，类似于将进度设为0。
                self.redis_client.ltrim(self.key, 1, 0)
                print(f"列表键 '{self.key}' 已成功清空 (使用LTRIM)。")
            elif method == 'delete':
                # --- 方案二: 直接删除键 ---
                # 这种方法更简单直接。当应用逻辑可以处理键不存在的情况
                # (例如，读取一个不存在的列表返回空) 时，这是个不错的选择。
                self.redis_client.delete(self.key)
                print(f"列表键 '{self.key}' 已成功删除。")
            else:
                print(f"错误：无效的清空方法 '{method}'。请选择 'trim' 或 'delete'。")

        except Exception as e:
            print(f"清空列表失败: {e}")
    def add_to_set(self, value: str) -> None:
        """向集合添加元素"""
        try:
            self.redis_client.sadd(self.key, value)
        except Exception as e:
            print(f"向集合添加元素失败: {e}")
    def remove_from_set(self, value: str) -> None:
        """从集合中移除元素"""
        try:
            self.redis_client.srem(self.key, value)
        except Exception as e:
            print(f"从集合中移除元素失败: {e}")
    def is_member_of_set(self, value: str) -> bool:
        """检查元素是否是集合的成员"""
        try:
            return self.redis_client.sismember(self.key, value)
        except Exception as e:
            print(f"检查集合成员失败: {e}")
            return False
    def get_set_members(self) -> set:
        """获取集合的所有成员"""
        try:
            return self.redis_client.smembers(self.key)
        except Exception as e:
            print(f"获取集合成员失败: {e}")
            return set()
# #

# # 使用示例
# cache = Cache("test_key2")
# cache.record_string("Hello, Redis!")
# print(cache.get_string())  # 输出: Hello, Redis!
# cache.clear_value()  # 清除之前的值
#
# cache = Cache("test_key3")
# cache.record_list(["item1", "item2", "item3"])
# print(cache.get_list())  # 输出: ['item1', 'item2', 'item3']
#
# cache.append_to_list("item4")
# print(cache.get_list())  # 输出: ['item1', 'item2', 'item3', 'item4']
#
# print(cache.get_list_length())  # 输出: 4
#
# cache.remove_from_list("item2")
# print(cache.get_list())  # 输出: ['item1', 'item3', 'item4']
# cache.clear_value()  # 清除之前的值
# cache.shutdown()  # 关闭Redis连接


# 虽然能正常运行，但是有以下报错，没有解决，可能是网络链接问题
# Exception ignored in: <function Redis.__del__ at 0x000002323F191300>
# Traceback (most recent call last):
#   File "C:\Users\JimmySmile\anaconda3\Lib\site-packages\redis\client.py", line 520, in __del__
#   File "C:\Users\JimmySmile\anaconda3\Lib\site-packages\redis\client.py", line 535, in close
#   File "C:\Users\JimmySmile\anaconda3\Lib\site-packages\redis\connection.py", line 1497, in disconnect
#   File "C:\Users\JimmySmile\anaconda3\Lib\site-packages\redis\connection.py", line 1398, in _checkpid
# TypeError: 'NoneType' object is not callable
