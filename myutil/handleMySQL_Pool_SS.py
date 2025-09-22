# -*- coding: utf-8 -*-
# @Time    : 2025/9/5 10:00
# @Author  : Gemini
# @Project : Python-DB-Handler
# @File    : handleMySQL_Pool.py
# @Software: PyCharm

import json
import time
from collections import Counter
from typing import List, Dict, Any, Iterator, Union, Tuple
import pymysql
import pymysql.cursors
from sqlalchemy import create_engine, exc, text
from sqlalchemy.engine import Engine
from contextlib import contextmanager


class MySQLHandler:
    """
    一个使用 SQLAlchemy 连接池进行数据库操作的健壮工具类。
    通过获取底层 PyMySQL 游标提供灵活的查询模式。
    """

    def __init__(self, db_name: str, table_name: str, pool_size: int = 5, max_overflow: int = 10,
                 stream_mode: bool = True, default_result_type: str = "dict"):
        """
        初始化MySQL处理器。
        :param db_name: 数据库名称
        :param table_name: 默认操作的表名
        :param pool_size: 连接池中保持的最小连接数
        :param max_overflow: 连接池中允许超出 pool_size 的最大连接数
        :param stream_mode: 是否默认使用流式查询 (服务器端游标)。
        :param default_result_type: 查询结果的默认返回类型, 'dict' 或 'tuple'。
        """
        if default_result_type not in ["dict", "tuple"]:
            raise ValueError("default_result_type must be either 'dict' or 'tuple'")

        self.db_name = db_name
        self.table_name = table_name
        self._pool_size = pool_size
        self._max_overflow = max_overflow
        self.stream_mode = stream_mode
        self.default_result_type = default_result_type

        self.engine: Engine = self._create_db_engine()
        print(
            f"SQLAlchemy engine for '{self.db_name}' created. Default mode: {'Streaming' if stream_mode else 'Buffered'}, Result type: {default_result_type}.")

    def _create_db_engine(self) -> Engine:
        db_url = f"mysql+pymysql://root:123456@10.0.102.52/{self.db_name}?charset=utf8mb4"
        try:
            return create_engine(
                db_url,
                pool_size=self._pool_size,
                max_overflow=self._max_overflow,
                pool_recycle=3600,
                pool_pre_ping=True,
                connect_args={'connect_timeout': 30}
            )
        except Exception as e:
            print(f"Fatal error: Could not create database engine: {e}")
            raise

    def reconnect(self):
        print("Attempting to reconnect by recreating the engine...")
        if self.engine:
            self.engine.dispose()
        self.engine = self._create_db_engine()
        print("Engine re-created successfully.")

    @contextmanager
    def _get_cursor(self, use_stream: bool = False, result_type: str = "dict") -> Iterator[
        Tuple[pymysql.cursors.Cursor, pymysql.connections.Connection]]:
        """
        上下文管理器，用于获取底层的 PyMySQL 连接和指定类型的游标。
        """
        raw_conn = self.engine.raw_connection()
        cursor = None
        try:
            cursor_class = pymysql.cursors.Cursor  # Default is tuple, buffered
            if use_stream:
                if result_type == 'dict':
                    cursor_class = pymysql.cursors.SSDictCursor
                else:
                    cursor_class = pymysql.cursors.SSCursor
            else:
                if result_type == 'dict':
                    cursor_class = pymysql.cursors.DictCursor

            cursor = raw_conn.cursor(cursor_class)
            yield cursor, raw_conn
        finally:
            if cursor:
                cursor.close()
            if raw_conn:
                raw_conn.close()  # This returns the underlying connection to SQLAlchemy's pool

    def _execute_with_retry(self, func):
        retries = 3
        for attempt in range(retries):
            try:
                return func()
            except exc.OperationalError as e:
                print(f"Connection error (Attempt {attempt + 1}/{retries}): {e}.")
                if attempt < retries - 1:
                    self.reconnect()
                    time.sleep(1 * (attempt + 1))
                else:
                    print("Failed to execute after multiple retries.")
            except Exception as e:
                print(f"A non-retriable error occurred: {e}")
                break
        return None

    def execute_query(self, query: str, params: dict = None, use_stream: bool = None, result_type: str = None) -> Union[
        List, None]:
        """
        执行只读查询 (SELECT)。
        """
        is_streaming = self.stream_mode if use_stream is None else use_stream
        res_type = self.default_result_type if result_type is None else result_type

        def query_logic():
            with self._get_cursor(use_stream=is_streaming, result_type=res_type) as (cursor, _):
                cursor.execute(query, params or {})
                if is_streaming:
                    # 即使是流式，也按要求构建一个列表返回
                    return list(cursor)
                else:
                    return cursor.fetchall()

        return self._execute_with_retry(query_logic)

    def execute_update(self, query: str, params: Union[Dict, List[Dict]] = None) -> int | None:
        """
        执行数据修改语句 (INSERT, UPDATE, DELETE)，支持单条和批量。
        """

        def update_logic():
            with self._get_cursor() as (cursor, raw_conn):
                if isinstance(params, list):
                    rowcount = cursor.executemany(query, params)
                else:
                    rowcount = cursor.execute(query, params or {})
                raw_conn.commit()
                return rowcount

        return self._execute_with_retry(update_logic)

    def close_engine(self):
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
            result = self.execute_query(query)
            result = result[0]
            if result:
                return result['min_id'], result['max_id']
            else:
                return None, None
        except pymysql.Error as e:
            print(f"查询最小和最大 ID 失败: {e}")
            return None, None
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
        placeholders = ', '.join([f'%({k})s' for k in processed_data.keys()])

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
                with self._get_cursor() as (cursor, connection):
                    result = cursor.execute(query, processed_data)
                    connection.commit()
                    # MySQL 在 ON DUPLICATE KEY UPDATE 语句后：
                    # - 如果执行了 INSERT，rowcount 是 1
                    # - 如果执行了 UPDATE，rowcount 是 2 (如果数据未变，则是0)
                    # - 如果数据未变，rowcount 是 0
                    if result == 1:
                        return "insert"
                    elif result > 1:
                        return "update"
                    else:
                        return "no_change"
            except Exception as e:
                wait_time = 0.5 * (2 ** attempt)
                if "1213" in str(e) and "Deadlock" in str(e):
                    print(f"线程发生死锁，第 {attempt}/{max_retries} 次重试... (等待 {wait_time:.2f}s)")
                else:
                    self.has_flush = False
                    first_id = processed_data.get('id', 'N/A')
                    print(f"线程发生不可重试的错误 (批次起始ID: {first_id}): {e}\n")

                time.sleep(wait_time)
        print(f"Insert or update failed: data:{data}")
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
        placeholders = ', '.join([f'%({k})s' for k in first_item_keys])
        query = f"INSERT INTO `{self.table_name}` ({columns}) VALUES ({placeholders})"

        try:
            with self._get_cursor() as (cursor, connection):
                # SQLAlchemy 的 execute 支持对 executemany 的自动处理
                result = cursor.executemany(query, processed_list)
                connection.commit()
                print(f"Batch insert successful. Rows affected: {result.rowcount}")
                return "insert"
        except Exception as e:
            # print(f"Batch insert failed: {e}. Falling back to individual inserts.")
            operations = [self.insert_data(item, unique_col) for item in data_list]
            counts = Counter(filter(None, operations))
            print(f"Individual insert summary for {len(data_list)} items: {counts}")
            # 如果有任何插入或更新，可以认为操作是成功的
            return "insert" if "insert" in counts or "update" in counts else None

# if __name__ == "__main__":
#     handler = MySQLHandler(db_name="collection", table_name="article_ri_cmu", pool_size=5, max_overflow=10,
#                            stream_mode=True, default_result_type="dict")
#     try:
#         # 执行一个查询
#         results = handler.execute_query("SELECT * FROM article_ri_cmu LIMIT 10")
#         print("Query Results:", len(results))
#
#         # 获取最小和最大 ID
#         min_id, max_id = handler.getMinMaxId()
#         print(f"Min ID: {min_id}, Max ID: {max_id}")
#
#     finally:
#         handler.close_engine()

