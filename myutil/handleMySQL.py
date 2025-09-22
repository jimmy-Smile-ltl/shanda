# -*- coding: utf-8 -*-
# @Time    : 2025/6/25 09:57
# @Author  : Jimmy Smile
# @Project : 北大信研院
# @File    : handkeMySQL.py
# @Software: PyCharm

import json
import time
from collections import Counter

import pymysql
import pymysql.cursors


class MySQLHandler:
    def __init__(self, db_name, table_name,return_type="tuple"):
        """
        初始化MySQL处理器。
        """
        self.db_name = db_name
        self.table_name = table_name
        self.return_type = return_type
        self.connection = self.get_db_connection(self.return_type)

    def get_db_connection(self,return_type="tuple"):
        for i in range(20):
            try:
                if return_type.lower() == "dict":
                    return pymysql.connect(
                        host='10.0.102.52',
                        user='root',
                        password="123456",
                        database=self.db_name,
                        connect_timeout=30,
                        autocommit=False,
                        cursorclass=pymysql.cursors.SSDictCursor
                    )
                else:
                    return pymysql.connect(
                        host='10.0.102.52',
                        user='root',
                        password="123456",
                        database=self.db_name,
                        connect_timeout=30,
                        autocommit=False,
                        cursorclass = pymysql.cursors.SSCursor
                    )
            except Exception as e:
                print(f"连接数据库失败: {e}, 重试 {i + 1}/10")
                sleep_time = 30 * 4 * (i + 1)  # 每次重试等待时间递增
                time.sleep(sleep_time)
                if i == 9:
                    raise Exception("无法连接到数据库，请检查配置或网络连接。")
                continue
        return None

    def execute_query(self, query, params=None):
        """
        执行查询语句。
        """
        for i in range(5):
            try:
                with self.connection.cursor() as cursor:
                    cursor.execute(query, params)
                    return cursor.fetchall()
            except pymysql.Error as e:
                print(f"查询执行失败: {e}")
                self.connection = self.get_db_connection(self.return_type)
        else:
            return None

    def insert_data(self, data, unique_col = "article_url") -> None | str:
        """
        插入单条数据。如果值的类型是 list 或 dict，则转换为 JSON 字符串。
        """
        if not isinstance(data, dict):
            print("数据必须是字典格式")
            return

        processed_data = data.copy()
        for key, value in processed_data.items():
            if isinstance(value, (list, dict)):
                processed_data[key] = json.dumps(value, ensure_ascii=False)

        columns = ', '.join([f'`{k}`' for k in processed_data.keys()])
        placeholders = ', '.join(['%s'] * len(processed_data))
        query = f"INSERT INTO `{self.table_name}` ({columns}) VALUES ({placeholders}) "
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query, tuple(processed_data.values()))
                self.connection.commit()
                cursor.close()
                return "insert"
        except Exception as e:
            try:
                self.connection.rollback()
            except Exception as e:
                self.connection = self.get_db_connection(self.return_type)
                self.connection.rollback()
            try:
                # 构建 ON DUPLICATE KEY UPDATE 子句
                # 我们将更新除了主键和创建时间之外的所有字段
                update_clause_parts = []
                for key in processed_data.keys():
                    if key != unique_col:
                        # 在更新时，我们通常不修改这两个字段
                        update_clause_parts.append(f"`{key}` = VALUES(`{key}`)")
                update_clause = ', '.join(update_clause_parts)
                columns = ', '.join([f'`{k}`' for k in processed_data.keys()])
                placeholders = ', '.join(['%s'] * len(processed_data))
                query = f"INSERT INTO `{self.table_name}` ({columns}) VALUES ({placeholders}) " + f"ON DUPLICATE KEY UPDATE {update_clause}"
                with self.connection.cursor() as cursor:
                    cursor.execute(query, tuple(processed_data.values()))
                    self.connection.commit()
                    cursor.close()
                    return  "update"
            except Exception as e:
                print(f"数据插入失败  AND  数据更新失败: {e} data:{json.dumps(data, ensure_ascii=False,indent=4)}")
                try:
                    self.connection.rollback()
                except Exception as e:
                    self.connection = self.get_db_connection(self.return_type)
                    self.connection.rollback()



    def insert_data_list(self, data_list,unique_col = "article_url") -> None | str:
        """
        批量插入数据。如果值的类型是 list 或 dict，则转换为 JSON 字符串。
        """
        if not data_list or not isinstance(data_list, list) or not all(isinstance(d, dict) for d in data_list):
            print("数据必须是字典列表格式且不能为空")
            return

        processed_list = []
        for data in data_list:
            processed_data = data.copy()
            for key, value in processed_data.items():
                if isinstance(value, (list, dict)):
                    processed_data[key] = json.dumps(value, ensure_ascii=False)
            processed_list.append(processed_data)
        if not processed_list:
            return
        # 使用 sorted() 按 key 的字母顺序排序 默认是按添加顺序来的，其实还应该比较key 是不是都是一样的
        sorted_order_keys = tuple(sorted( processed_list[0].keys()))
        columns = ', '.join([f'`{k}`' for k in sorted_order_keys])
        placeholders = ', '.join(['%s'] * len(sorted_order_keys))
        query = f"INSERT INTO `{self.table_name}` ({columns}) VALUES ({placeholders})"

        try:
            values_to_insert = []
            for d in processed_list:
                values_to_insert.append(tuple(d.get(key) for key in sorted_order_keys))
            with self.connection.cursor() as cursor:
                cursor.executemany(query, values_to_insert)
                print(f"批量数据插入成功,插入条数: {cursor.rowcount}")
                self.connection.commit()
                cursor.close()
            return "insert"
        except Exception as e:
            print(f"批量数据插入失败: {e}  改为一个一个插入")
            try:
                self.connection.rollback()
            except Exception as e:
                self.connection = self.get_db_connection(self.return_type)
                self.connection.rollback()
            # 记录操作 记录 集合
            operations = []
            if "processed_list" in locals() and processed_list:
                # 那就一个一个插入
                for item in processed_list:
                    operation = self.insert_data(item, unique_col=unique_col)
                    if operation:
                        operations.append(operation)
                counts = Counter(operations)

                print(f"总计插入 {len(processed_list)}个 具体操作计算 {counts} ")
            if len(operations) == 1:
                return operations.pop()
            else: # 又插入有更新，返回插入
                return "insert"

    def delete_condition_data(self, condition, max_num: int = 1000):
        """
        删除指定条件的数据。
        """
        if not isinstance(condition, dict):
            print("条件必须是字典格式")
            return False
        # 必须有条件
        if not condition:
            print("删除条件不能为空")
            return False
        # 先计算相关符合条件的数量，小于1000 直接删，大于1000 提示用户确认
        if self.isMoreOneKiloRows(condition=condition, max_num=max_num):
            print(f"表 {self.table_name} 中数据超过 {max_num} 条，请再次确认是否删除，输入 y 确认删除，其他键取消")
            user_input = input("确认删除数据 (y/n): ")
            if user_input.lower() != 'y':
                print("取消删除数据操作")
                return False
            condition_str = ' AND '.join([f"`{key}` = '{condition[key]}'" for key in condition.keys()])
            delete_query = f"DELETE FROM `{self.table_name}` WHERE " + condition_str
            try:
                with self.connection.cursor() as cursor:
                    cursor.execute(delete_query)
                    self.connection.commit()
                    print(f"符合条件的数据已删除: {cursor.rowcount} 条")
                    cursor.close()
                return True
            except pymysql.Error as e:
                print(f"查询数据条数失败: {e}")
                return False
        else:
            print(f"<UNK> 小于{max_num}<UNK> 自动删除历史数据")
            condition_str = ' AND '.join([f"`{key}` = '{condition[key]}'" for key in condition.keys()])
            delete_query = f"DELETE FROM `{self.table_name}` WHERE " + condition_str
            try:
                with self.connection.cursor() as cursor:
                    cursor.execute(delete_query)
                    self.connection.commit()
                    print(f"符合条件的数据已删除: {cursor.rowcount} 条")
                    cursor.close()
                return True
            except pymysql.Error as e:
                print(f"查询数据条数失败: {e}")
                return False

    def close(self):
        """关闭数据库连接。"""
        if self.connection:
            self.connection.close()
        print("数据库连接已关闭。")

    # 添加，建表，删除表的命令
    def create_table(self, create_sql):
        """
        创建表。
        """
        try:
            if create_sql.find('CREATE TABLE') == -1:
                print("SQL语句不包含CREATE TABLE")
                return
            if self.table_name not in create_sql:
                print(f"SQL语句中不包含表名: {self.table_name}")
                return
            if self.is_has_table(self.table_name):
                print(f"表 {self.table_name} 已存在，无需创建")
                return
            with self.connection.cursor() as cursor:
                cursor.execute(create_sql)
                self.connection.commit()
                cursor.close()
            # 提取表名
            table_name = create_sql.split('`')[1]
            print(f"表 {table_name} 创建成功")
        except pymysql.Error as e:
            table_name = create_sql.split('`')[1]
            print(f"表 {table_name} 创建失败: {e}")
            try:
                self.connection.rollback()
            except Exception as e:
                self.connection = self.get_db_connection(self.return_type)
                self.connection.rollback()

    def is_has_table(self, table_name):
        """
        检查指定的数据库中是否存在某张表。

        :param cursor: 数据库游标对象
        :param table_name: 要检查的表名 (字符串)
        :return: 如果表存在，返回 True；否则返回 False。
        """
        try:
            # 使用 INFORMATION_SCHEMA.TABLES 来查询
            # %s 是参数占位符，可以防止SQL注入
            # DATABASE() 函数会自动获取当前连接的数据库名
            sql = """
                SELECT COUNT(*)
                FROM information_schema.tables
                WHERE table_schema = DATABASE() AND table_name = %s;
            """
            for i in range(3):
                with self.connection.cursor() as cursor:
                    cursor.execute(sql, (table_name,))
                    # fetchone() 会返回一个元组，例如 (1,) 或 (0,)
                    # 如果结果中的第一个值（计数）大于0，说明表存在
                    fetchone = cursor.fetchone()
                    cursor.close()
                    if isinstance(fetchone,tuple):
                        if  fetchone[0] == 1:
                            return True
                        else:
                            return False
                    elif isinstance(fetchone,dict) and fetchone.get("COUNT(*)"):
                        return True
                    else:
                        print(f"is_has_table方法 检查表是否存在时返回值异常: {fetchone}")
                        continue
            else:
                return False
        except Exception as e:
            print(f"检查表是否存在时出错: {e}")
            return False
    def drop_table(self, max_num: int = 100):
        """
        删除表。
        """

        query = f"DROP TABLE IF EXISTS `{self.table_name}`"
        try:
            if not self.is_has_table(self.table_name):
                print(f"\r表 {self.table_name} 不存在，无法删除")
                return False
            if self.isMoreOneKiloRows(max_num=max_num):
                print(f"表 {self.table_name} 中数据超过 {max_num} 条，默认不删除")
                return False
                # print(f"表 {self.table_name} 中数据超  {max_num} 条，请再次确认是否删除，输入 y 确认删除，其他键取消")
                # user_input = input("确认删除表 (y/n): ")
                # if user_input.lower() != 'y':
                #     print("取消删除表操作")
                #     return
            if max_num > 1000:
                print(f"<UNK>max_num参数{max_num}过大<UNK> 上限1000，最多可以删除含1000条数据的表")
                return False
            with self.connection.cursor() as cursor:
                cursor.execute(query)
                self.connection.commit()
                print(f"表删除成功 ,行数小于{max_num}条,默认删除")
                cursor.close()
                return True
        except pymysql.Error as e:
            print(f"表删除失败: {e}")
            try:
                self.connection.rollback()
            except Exception as e:
                self.connection = self.get_db_connection(self.return_type)
                self.connection.rollback()
            return False

    def clear_table(self, max_num=100):
        """
        清空表数据。
        """
        query = f"TRUNCATE TABLE `{self.table_name}`"
        try:
            if not self.is_has_table(self.table_name):
                print(f"\r表 {self.table_name} 不存在，无法清空")
                return False
            if self.isMoreOneKiloRows(max_num=max_num) :
                print(f"表 {self.table_name} 中数据超过 {max_num}条,默认不可清空")
                return False
            if max_num > 1000:
                print(f"<UNK>max_num参数{max_num}过大<UNK> 上限1000，最多可以清空含1000条数据的表")
                return False
            with self.connection.cursor() as cursor:
                cursor.execute(query)
                self.connection.commit()
                cursor.close()
                print(f"表 {self.table_name} 数据清空成功")
                return True
        except pymysql.Error as e:
            print(f"表 {self.table_name} 数据清空失败: {e}")
            try:
                self.connection.rollback()
            except Exception as e:
                self.connection = self.get_db_connection(self.return_type)
                self.connection.rollback()
            return False

    def isMoreOneKiloRows(self, condition: dict = None, max_num: int = 1000):
        """
        检查表中是否有超过1000条数据。
        """
        if condition:
            condition_str = ' AND '.join([f"`{key}` = '{condition[key]}'" for key in condition.keys()])
            query = f"SELECT COUNT(*) FROM `{self.table_name}` WHERE {condition_str}"
        else:
            query = f"SELECT COUNT(*) FROM `{self.table_name}`"
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query)
                count = cursor.fetchone()[0]
                print(f" 表 {self.table_name} 查询到数据 {count} <UNK> ",end ="\t")
                return count > max_num
        except pymysql.Error as e:
            print(f"查询数据条数失败: {e}")
            # 避免误删，报错返回True
            return True

    # 获取最大最小的id
    def getMinMaxId(self,table_name=None):
        """
        获取表中的最小和最大 ID。
        """
        if not table_name:
            table_name = self.table_name
        query = f"SELECT MIN(id) AS min_id, MAX(id) AS max_id FROM `{self.db_name}`.`{table_name}`"
        try:
            with self.connection.cursor(pymysql.cursors.DictCursor) as cursor:
                cursor.execute(query)
                result = cursor.fetchone()
                if result:
                    return result['min_id'], result['max_id']
                else:
                    return None, None
        except pymysql.Error as e:
            print(f"查询最小和最大 ID 失败: {e}")
            return None, None

