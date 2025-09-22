# -*- coding: utf-8 -*-
# @Time    : 2025/6/25 10:30
# @Author  : Jimmy Smile (Refactored by Gemini)
# @Project : 北大信研院
# @File    : handleMySQL_sqlalchemy.py
# @Software: PyCharm

import json
import time
from collections import Counter
from typing import Tuple
import pymysql
from sqlalchemy import create_engine, text, exc
from sqlalchemy.engine import Engine


class MySQLHandler:
    """
    一个使用 SQLAlchemy 连接池进行数据库操作的工具类。
    它提供了高性能、稳定且线程安全的数据库交互方式。
    """

    def __init__(self, db_name: str, table_name: str,pool_size=5, max_overflow=10):
        """
        初始化MySQL处理器，创建数据库引擎和连接池。
        :param db_name: 数据库名称
        :param table_name: 默认操作的表名
        """
        self.has_flush = False
        self.db_name = db_name
        self.table_name = table_name
        self.pool_size = pool_size
        self.max_overflow = max_overflow
        # 在初始化时创建一次引擎，该引擎管理着一个连接池
        self.engine: Engine = self._create_db_engine()
        print(f"SQLAlchemy engine for database '{self.db_name}' created successfully.")

    def _create_db_engine(self) -> Engine:
        """
        私有方法，用于创建和配置 SQLAlchemy 引擎。
        """
        # SQLAlchemy 连接字符串格式: "dialect+driver://username:password@host:port/database"
        db_url = f"mysql+pymysql://root:123456@10.0.102.52/{self.db_name}?charset=utf8mb4"
        for i in range(10):
            try:
                engine =  create_engine(
                    db_url,
                    pool_size=self.pool_size,  # 连接池中保持的最小连接数
                    max_overflow=self.max_overflow,  # 连接池中允许超出 pool_size 的最大连接数
                    pool_recycle=3600,  # 连接回收时间(秒),防止因超时被数据库断开(MySQL默认wait_timeout是8小时)
                    pool_pre_ping=True,  # 在每次从池中获取连接时，检查其有效性
                    connect_args={  # 其他 pymysql.connect 的参数
                        'connect_timeout': 30,
                        "cursorclass" : pymysql.cursors.SSCursor
                    }
                )
                self.has_flush = True
                return engine
            except Exception as e:
                print(f"连接数据库失败: {e}, 重试 {i + 1}/10")
                sleep_time = 30 * 2 ** (i + 1)  # 每次重试等待时间递增
                time.sleep(sleep_time)
                if i == 9:
                    raise Exception("无法连接到数据库，请检查配置或网络连接。")
                continue

    def execute_query(self, query: str, params: tuple|dict = (),type: str = "dict") -> list | None:
        """
        执行只读的查询语句 (SELECT)。
        :param query: SQL 查询字符串
        :param params: 查询参数字典
        :return: 查询结果列表，失败则返回 None
        """
        for i in range(3):
            result_list = []
            try:
                # 'with' 语句从连接池获取连接，并在结束后自动归还
                with self.engine.connect() as connection:
                    # 使用 text() 包装SQL语句，并传入参数，防止SQL注入
                    stmt = text(query)
                    result = connection.execution_options(
                                stream_results=True
                            ).execute(stmt, params or {})
                    if type == "tuple":
                        # fetchall() 返回一个Row对象的列表
                        # return result.fetchall()
                        for row in result:  # 直接迭代返回 Row 对象 (类元组)
                            result_list.append(tuple(row))
                    else:
                        for row in result.mappings():  # .mappings() 返回字典迭代器
                            result_list.append(row)
                        return result_list
                        # # 使用 .mappings().all() 将所有结果行转换为字典列表
                        # results = result.mappings().all()
                        # # fetchall() 返回一个Row对象的列表
                        # return results

            except exc.SQLAlchemyError as e:
                print(f"Query execution failed: {e}")
                self.engine: Engine = self._create_db_engine()
        else:
            return None

    def execute_update(self, query: str, params: dict = None) -> int:
        """
        执行数据修改语句 (INSERT, UPDATE, DELETE)。
        :param query: SQL DML 字符串
        :param params: 参数字典
        :return: 受影响的行数, 失败则返回 -1
        """
        for i in range(3):
            try:
                with self.engine.connect() as connection:
                    stmt = text(query)
                    result = connection.execute(stmt, params or {})
                    # 对于DML语句，需要提交事务
                    connection.commit()
                    # .rowcount 可以获取受影响的行数
                    return result.rowcount
            except exc.SQLAlchemyError as e:
                print(f"Update execution failed: {e}")
                self.engine: Engine = self._create_db_engine()
        else:
            return -1

    def _process_data(self, data: dict) -> dict:
        """
        处理数据，将 list 或 dict 转换为 JSON 字符串。
        """
        if type(data) is not  dict:
            data = dict(data)
        processed_data = data.copy()
        for key, value in processed_data.items():
            if isinstance(value, (list, dict)):
                processed_data[key] = json.dumps(value, ensure_ascii=False)
        return processed_data

    def insert_data(self, data: dict, unique_col: str = "article_url") -> str | None:
        """
        插入或更新单条数据 (UPSERT)。
        使用 ON DUPLICATE KEY UPDATE 实现原子操作。
        """
        if not isinstance(data, dict):
            print("Error: Data must be a dictionary.")
            return None

        processed_data = self._process_data(data)

        columns = ', '.join([f'`{k}`' for k in processed_data.keys()])
        placeholders = ', '.join([f':{k}' for k in processed_data.keys()])

        update_clause_parts = [
            f"`{key}` = VALUES(`{key}`)"
            for key in processed_data.keys() if key != unique_col
        ]
        update_clause = ', '.join(update_clause_parts)

        query = (
            f"INSERT INTO `{self.table_name}` ({columns}) VALUES ({placeholders}) "
            f"ON DUPLICATE KEY UPDATE {update_clause}"
        )
        max_retries = 3
        for i in range(max_retries):
            attempt = i + 1
            try:
                with self.engine.connect() as connection:
                    stmt = text(query)
                    result = connection.execute(stmt, processed_data)
                    connection.commit()
                    # MySQL 在 ON DUPLICATE KEY UPDATE 语句后：
                    # - 如果执行了 INSERT，rowcount 是 1
                    # - 如果执行了 UPDATE，rowcount 是 2 (如果数据未变，则是0)
                    # - 如果数据未变，rowcount 是 0
                    if result.rowcount == 1:
                        return "insert"
                    elif result.rowcount > 1:
                        return "update"
                    else:
                        return "no_change"
            except Exception as e:
                connection.rollback()
                wait_time = 0.5 * (2 ** attempt)
                if "1213" in str(e) and "Deadlock" in str(e):
                    print(f"线程发生死锁，第 {attempt}/{max_retries} 次重试... (等待 {wait_time:.2f}s)")
                else:
                    self.has_flush = False
                    first_id = processed_data.get('id', 'N/A')
                    print(f"线程发生不可重试的错误 (批次起始ID: {first_id}): {e}\n")

                time.sleep(wait_time)
        print(f"Insert or update failed: {e} data:{data}")
        return None

    def insert_data_list(self, data_list: list, unique_col: str = "article_url") -> str | None:
        """
        批量插入数据。如果失败，则回退到逐条插入。
        """
        if not data_list or not isinstance(data_list, list):
            print("Error: Data must be a non-empty list of dictionaries.")
            return None
        if not all(isinstance(item, dict) for item in data_list):
            data_list = [dict(item) for item in data_list]
        processed_list = [self._process_data(data) for data in data_list]
        first_item_keys = processed_list[0].keys()
        columns = ', '.join([f'`{k}`' for k in first_item_keys])
        placeholders = ', '.join([f':{k}' for k in first_item_keys])
        query = f"INSERT INTO `{self.table_name}` ({columns}) VALUES ({placeholders})"

        try:
            with self.engine.connect() as connection:
                # SQLAlchemy 的 execute 支持对 executemany 的自动处理
                result = connection.execute(text(query), processed_list)
                connection.commit()
                print(f"Batch insert successful. Rows affected: {result.rowcount}")
                return "insert"
        except exc.SQLAlchemyError as e:
            # print(f"Batch insert failed: {e}. Falling back to individual inserts.")
            operations = [self.insert_data(item, unique_col) for item in data_list]
            counts = Counter(filter(None, operations))
            print(f"Individual insert summary for {len(data_list)} items: {counts}")
            # 如果有任何插入或更新，可以认为操作是成功的
            return "insert" if "insert" in counts or "update" in counts else None

    def delete_condition_data(self, condition: dict, max_num: int = 1000) -> bool:
        """
        根据条件删除数据，并有安全检查。
        """
        if not isinstance(condition, dict) or not condition:
            print("Error: Condition must be a non-empty dictionary.")
            return False

        count = self.count_rows(condition)
        if count is None:  # 查询失败
            return False

        if count > max_num:
            print(f"Data count ({count}) exceeds the limit ({max_num}). Deletion requires confirmation.")
            user_input = input(f"Are you sure you want to delete {count} rows from '{self.table_name}'? (y/n): ")
            if user_input.lower() != 'y':
                print("Deletion cancelled by user.")
                return False
        else:
            print(f"Data count ({count}) is within the limit ({max_num}). Proceeding with deletion.")

        condition_str = ' AND '.join([f"`{key}` = :{key}" for key in condition.keys()])
        delete_query = f"DELETE FROM `{self.table_name}` WHERE {condition_str}"

        rows_deleted = self.execute_update(delete_query, condition)
        if rows_deleted != -1:
            print(f"Successfully deleted {rows_deleted} rows.")
            return True
        return False

    def close(self):
        """
        关闭引擎持有的所有连接池中的连接。
        通常在应用关闭时调用。
        """
        if self.engine:
            self.engine.dispose()
            print("Database connection pool has been disposed.")

    def create_table(self, create_sql: str):
        """
        如果表不存在，则创建表。
        """
        if self.is_has_table(self.table_name):
            print(f"Table `{self.table_name}` already exists.")
            return
        print(f"Table `{self.table_name}` does not exist. Attempting to create...")
        self.execute_update(create_sql)
        # 再次检查以确认
        if self.is_has_table(self.table_name):
            print(f"Table `{self.table_name}` created successfully.")
        else:
            print(f"Failed to create table `{self.table_name}`.")

    def is_has_table(self, table_name: str) -> bool:
        """
        检查表是否存在。
        """
        query = """
                SELECT COUNT(*)
                FROM information_schema.tables
                WHERE table_schema = :db_name \
                  AND table_name = :table_name \
                """
        params = {"db_name": self.db_name, "table_name": table_name}
        result = self.execute_query(query, params)
        return result[0][0] == 1 if result is not None else False

    def drop_table(self, max_num: int = 100) -> bool:
        """
        如果表中数据量在安全范围内，则删除表。
        """
        if not self.is_has_table(self.table_name):
            print(f"Table `{self.table_name}` does not exist. Nothing to drop.")
            return False

        if max_num > 1000:
            print(f"Error: max_num safety limit is 1000. Provided: {max_num}")
            return False

        count = self.count_rows()
        if count > max_num:
            print(f"Table contains {count} rows, which exceeds the safety limit of {max_num}. Drop aborted.")
            return False

        query = f"DROP TABLE IF EXISTS `{self.table_name}`"
        self.execute_update(query)
        print(f"Table `{self.table_name}` dropped successfully (had {count} rows).")
        return not self.is_has_table(self.table_name)

    def clear_table(self, max_num: int = 100) -> bool:
        """
        如果表中数据量在安全范围内，则清空表。
        """
        if not self.is_has_table(self.table_name):
            print(f"Table `{self.table_name}` does not exist. Nothing to clear.")
            return False

        if max_num > 1000:
            print(f"Error: max_num safety limit is 1000. Provided: {max_num}")
            return False

        count = self.count_rows()
        if count > max_num:
            print(f"Table contains {count} rows, which exceeds the safety limit of {max_num}. Truncate aborted.")
            return False

        query = f"TRUNCATE TABLE `{self.table_name}`"
        self.execute_update(query)
        print(f"Table `{self.table_name}` truncated successfully (had {count} rows).")
        return self.count_rows() == 0

    def count_rows(self, condition: dict = None) -> int | None:
        """
        计算表中的行数，可选择带条件。
        """
        if condition:
            condition_str = ' AND '.join([f"`{key}` = :{key}" for key in condition.keys()])
            query = f"SELECT COUNT(*) FROM `{self.table_name}` WHERE {condition_str}"
            params = condition
        else:
            query = f"SELECT COUNT(*) FROM `{self.table_name}`"
            params = None

        result = self.execute_query(query, params)
        if result is not None:
            count = result[0][0]
            print(f"Table `{self.table_name}` (with condition: {condition}) has {count} rows.")
            return count
        return None
    def close_engine(self):
        """
        关闭引擎持有的所有连接池中的连接。
        通常在应用关闭时调用。
        """
        if self.engine:
            self.engine.dispose()
            print("Database connection pool has been disposed.")

    # 获取最大最小的id
    def getMinMaxId(self, table_name=None):
        """
        获取表中的最小和最大 ID。
        """
        if not table_name:
            table_name = self.table_name
        query = f"SELECT MIN(id) AS min_id, MAX(id) AS max_id FROM `{self.db_name}`.`{table_name}`"
        try:
            result = self.execute_query(query,type="dict")
            result = result[0]
            if result:
                return result['min_id'], result['max_id']
            else:
                return None, None
        except pymysql.Error as e:
            print(f"查询最小和最大 ID 失败: {e}")
            return None, None

# # --- 使用示例 ---
# if __name__ == '__main__':
#     # 1. 初始化处理器，此时会创建连接池
#     db_handler = MySQLHandler(db_name='collection', table_name='article_ri_cmu')
#     min_id ,max_id = db_handler.getMinMaxId()
#     print(f"Min ID: {min_id}, Max ID: {max_id}")

    # # 2. 准备建表语句
    # create_sql_statement = """
    #                        CREATE TABLE IF NOT EXISTS `test_articles` \
    #                        ( \
    #                            `id`          INT AUTO_INCREMENT PRIMARY KEY, \
    #                            `title`       VARCHAR(255) NOT NULL, \
    #                            `content`     TEXT, \
    #                            `article_url` VARCHAR(512) NOT NULL UNIQUE, \
    #                            `tags`        JSON, \
    #                            `created_at`  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    #                        ) ENGINE = InnoDB \
    #                          DEFAULT CHARSET = utf8mb4; \
    #                        """
#
#     # 3. 创建表 (如果不存在)
#     db_handler.create_table(create_sql_statement)
#
#     # 4. 插入单条数据 (会触发 INSERT)
#     print("\n--- Inserting a new article ---")
#     article_1 = {
#         'title': 'SQLAlchemy is Awesome',
#         'content': 'This is a post about using SQLAlchemy.',
#         'article_url': 'http://example.com/sqlalchemy',
#         'tags': ['python', 'database', 'sqlalchemy']
#     }
#     status = db_handler.insert_data(article_1)
#     print(f"Operation status: {status}")
#
#     # 5. 再次插入相同数据 (会触发 UPDATE)
#     print("\n--- Inserting the same article again (should update) ---")
#     article_1_updated = {
#         'title': 'SQLAlchemy is Really Awesome!',
#         'content': 'An updated post about using SQLAlchemy.',
#         'article_url': 'http://example.com/sqlalchemy',
#         'tags': ['python', 'orm']
#     }
#     status = db_handler.insert_data(article_1_updated)
#     print(f"Operation status: {status}")
#
#     # 6. 批量插入数据
#     print("\n--- Batch inserting new articles ---")
#     articles_list = [
#         {
#             'title': 'Understanding Connection Pools',
#             'content': 'A deep dive into database connection pooling.',
#             'article_url': 'http://example.com/pools',
#             'tags': ['performance', 'database']
#         },
#         {
#             'title': 'Getting Started with PyMySQL',
#             'content': 'A beginner guide.',
#             'article_url': 'http://example.com/pymysql',
#             'tags': ['python', 'mysql']
#         }
#     ]
#     db_handler.insert_data_list(articles_list)
#
#     # 7. 查询数据
#     print("\n--- Querying all articles ---")
#     all_articles = db_handler.execute_query("SELECT * FROM test_articles")
#     if all_articles:
#         for article in all_articles:
#             # 通过列名访问，更清晰
#             print(f"ID: {article.id}, Title: {article.title}, URL: {article.article_url}")
#
#     # 8. 带条件删除 (假设数据量小，直接删除)
#     print("\n--- Deleting an article by condition ---")
#     db_handler.delete_condition_data({'article_url': 'http://example.com/pymysql'}, max_num=10)
#
#     # 9. 清理和关闭
#     print("\n--- Cleaning up ---")
#     # db_handler.clear_table(max_num=100) # 清空表（如果行数小于100）
#     # db_handler.drop_table(max_num=100)   # 删除表（如果行数小于100）
#
#     # 10. 在程序结束时，释放连接池中的所有连接
#     db_handler.close()

