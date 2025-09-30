# -*- coding: utf-8 -*-
import json
import time
from collections import Counter
import psycopg2
import psycopg2.extras
from psycopg2 import sql

import re
import psycopg2.errors


def parse_unique_violation(e: psycopg2.errors.UniqueViolation) -> str:
    """
    解析 psycopg2 的 UniqueViolation 异常，并返回一个带有针对性建议的、
    人性化的错误信息字符串。
    """
    error_msg = str(e)

    # 尝试解析约束名称
    constraint_match = re.search(r'violates unique constraint "([^"]+)"', error_msg)
    constraint_name = constraint_match.group(1) if constraint_match else "未知约束"

    # 尝试解析冲突的键值对
    detail_match = re.search(r'Key \(([^)]+)\)=\(([^)]+)\)', error_msg)
    key_column = detail_match.group(1) if detail_match else "未知列"
    key_value = detail_match.group(2) if detail_match else "未知值"

    # 根据约束名称的后缀判断冲突类型
    if constraint_name.endswith("_pkey"):
        # --- 场景一：主键冲突 (Primary Key Violation) ---
        table_name_guess = constraint_name.replace("_pkey", "")

        suggestion = (
            f"这通常发生在ID序列计数器与表中现有数据不同步时。\n"
            f"      如果您最近手动导入过数据(例如使用COPY命令)，ID序列的值可能没有自动更新。\n\n"
            f"      ▶︎ 解决方案：请执行以下SQL命令来重置与该表关联的ID序列：\n"
            f"      SELECT setval(pg_get_serial_sequence('\"your_schema\".\"{table_name_guess}\"', '{key_column}'), "
            f"(SELECT MAX({key_column}) FROM \"your_schema\".\"{table_name_guess}\"));\n\n"
            f"      (注意：请将 your_schema 和 {table_name_guess} 替换为真实的schema和表名)"
        )

        return (
            f"【主键冲突提醒 (Primary Key Violation)】\n"
            f"  - 表与约束: {constraint_name}\n"
            f"  - 冲突详情: 主键列 '{key_column}' 的值 '{key_value}' 已经存在。\n"
            f"  - 可能原因与解决方案:\n      {suggestion}"
        )
    else:
        # --- 场景二：唯一约束冲突 (Unique Constraint Violation) ---
        suggestion = (
            f"这意味着您尝试插入或更新的数据中，'{key_column}' 字段的值必须是唯一的，但数据库中已有记录使用了相同的值。\n\n"
            f"      ▶︎ 解决方案：\n"
            f"        1. 检查您的输入数据源，去除或修改重复的数据。\n"
            f"        2. 如果您期望的是更新现有记录，请确保您的代码逻辑（如 ON CONFLICT DO UPDATE）是针对 '{key_column}' 这个唯一键来设计的。"
        )

        return (
            f"【唯一约束冲突提醒 (Unique Constraint Violation)】\n"
            f"  - 冲突约束: {constraint_name}\n"
            f"  - 冲突详情: 唯一键列 '{key_column}' 的值 '{key_value}' 已经存在。\n"
            f"  - 可能原因与解决方案:\n      {suggestion}"
        )
"""
PostgreSQL 数据库处理类
"""
class PostgreSQLHandler:
    def __init__(self, db_name, table_name, return_type="tuple",is_local:bool = True):
        self.db_name = db_name # 数据库名称 无效了
        self.table_name = table_name
        self.schema = 'spider'
        self.return_type = return_type
        self.connection = self.get_db_connection(self.return_type, is_local)
        self.create_schema_if_not_exists(self.schema)

    def get_db_connection(self, return_type="tuple",is_local:bool =True):
        # 主机: 10.241.132.70
        #   端口: 35432
        #   数据库: talents
        #   用户名: postgres
        #   密码: 'BIJ$IkNkDH5{V4b_V3@T'

        if is_local:
            ip = '127.0.0.1'
            user =  'postgres'
            password = 'postgres'
            port = 5432
            db_name = 'postgres'
        else:
            ip = '10.241.132.70'
            user = 'postgres'
            password = 'BIJ$IkNkDH5{V4b_V3@T'
            port = 35432
            db_name = 'talents'
        db_name = self.db_name or db_name # 优先使用传入的数据库名称

        for i in range(20):
            try:
                if return_type.lower() == "dict":
                    return psycopg2.connect(
                        host= ip,
                        user= user,
                        password= password,
                        dbname=db_name,
                        connect_timeout=30,
                        port=port,
                        cursor_factory=psycopg2.extras.RealDictCursor
                    )
                else:
                    return psycopg2.connect(
                        host=ip,
                        user=user,
                        password=password,
                        dbname=db_name,
                        connect_timeout=30,
                        port=port,
                    )
            except Exception as e:
                print(f"连接数据库失败: {e}, 重试 {i + 1}/10")
                sleep_time = 30 * 4 * (i + 1)
                time.sleep(sleep_time)
                if i == 9:
                    raise Exception("无法连接到数据库，请检查配置或网络连接。")
        return None


    # 添加到 PostgreSQLHandler 类中
    def schema_exists(self, schema_name: str | None = None) -> bool:
        """
        检测 schema 是否存在
        """
        name = schema_name or self.schema
        query = "SELECT 1 FROM information_schema.schemata WHERE schema_name = %s"
        try:
            with self.connection.cursor() as cur:
                cur.execute(query, (name,))
                return cur.fetchone() is not None
        except psycopg2.Error as e:
            print(f"检查 schema 失败: {e}")
            try:
                self.connection.rollback()
            except Exception:
                self.connection = self.get_db_connection(self.return_type)
            return False

    def create_schema_if_not_exists(self, schema_name: str | None = None, owner: str | None = None) -> bool:
        """
        若 schema 不存在则创建；可选指定 owner
        """
        name = schema_name or self.schema
        if self.schema_exists(name):
            return True
        try:
            with self.connection.cursor() as cur:
                if owner:
                    stmt = sql.SQL("CREATE SCHEMA IF NOT EXISTS {} AUTHORIZATION {}") \
                        .format(sql.Identifier(name), sql.Identifier(owner))
                else:
                    stmt = sql.SQL("CREATE SCHEMA IF NOT EXISTS {}") \
                        .format(sql.Identifier(name))
                cur.execute(stmt)
            self.connection.commit()
            return True
        except psycopg2.Error as e:
            print(f"创建 schema 失败: {e}")
            try:
                self.connection.rollback()
            except Exception:
                self.connection = self.get_db_connection(self.return_type)
            return False

    def execute(self,sql:str):
        for i in range(3):
            try:
                with self.connection.cursor() as cursor:
                    cursor.execute(sql)
                    self.connection.commit()
                    return True
            except psycopg2.Error as e:
                print(f"执行失败: {e}")
                if "already exists" in  str(e):
                    self.connection.rollback()
                    return True
                self.connection = self.get_db_connection(self.return_type)
        else:
            return False
    def execute_query(self, query, params=None):
        for i in range(5):
            try:
                with self.connection.cursor() as cursor:
                    cursor.execute(query, params)
                    return cursor.fetchall()
            except psycopg2.Error as e:
                print(f"查询执行失败: {e}")
                self.connection = self.get_db_connection(self.return_type)
        else:
            return None

    def insert_data(self, data, unique_col = "article_url" , table_name = None) -> None | str:
        if not isinstance(data, dict):
            print("数据必须是字典格式")
            return
        if not table_name:
            table_name = self.table_name
        processed_data = data.copy()
        for key, value in processed_data.items():
            if isinstance(value, (list, dict)):
                processed_data[key] = json.dumps(value, ensure_ascii=False)

        columns = ', '.join([f'"{k}"' for k in processed_data.keys()])
        placeholders = ', '.join([f'%({k})s' for k in processed_data.keys()])

        # 第一步：尝试插入
        insert_query = f'INSERT INTO "{self.schema}"."{table_name}" ({columns}) VALUES ({placeholders})'

        try:
            with self.connection.cursor() as cursor:
                cursor.execute(insert_query, processed_data)
                self.connection.commit()
                # 移除 cursor.close()
                return "insert"
        except psycopg2.IntegrityError as e:
            if "duplicate key value violates unique constraint" in str(e):
                try:
                    self.connection.rollback()
                    if unique_col and self.table_name != "source_collection_info":
                        # 关键修改：排除 id 字段和 unique_col 字段
                        excluded_fields = {'id', unique_col}
                        update_clause = ', '.join([
                            f'"{k}" = %({k})s'
                            for k in processed_data.keys()
                            if k not in excluded_fields
                        ])

                        if update_clause:  # 确保有字段需要更新
                            update_query = f'UPDATE "{self.schema}"."{table_name}" SET {update_clause} WHERE "{unique_col}" = %({unique_col})s'
                            with self.connection.cursor() as cursor:
                                cursor.execute(update_query, processed_data)
                                self.connection.commit()
                                # 移除 cursor.close()
                        else:
                            print("没有需要更新的字段")
                    return "update"
                except Exception as update_e:
                    print(f"单行操作 插入失败,而且更新失败: {update_e}")
                    try:
                        self.connection.rollback()
                        friendly_message = parse_unique_violation(e)
                        print(f"\n--- 数据库操作提示 ---\n{friendly_message}\n-----------------------\n")
                    except Exception:
                        self.connection = self.get_db_connection(self.return_type)
                    return None
            else:
                print(f"插入失败（非重复键错误）: {e}")
                try:
                    self.connection.rollback()
                except Exception:
                    self.connection = self.get_db_connection(self.return_type)
                return None
        except Exception as e:
            print(f"数据插入失败: {e}")
            try:
                self.connection.rollback()
            except Exception:
                self.connection = self.get_db_connection(self.return_type)
            return None
    # 批量插入数据，data_list 是字典列表
    def insert_data_list(self, data_list, unique_col="article_url", table_name=None) -> None | str | dict:
        if not data_list or not isinstance(data_list, list) or not all(isinstance(d, dict) for d in data_list):
            print("数据必须是字典列表格式且不能为空")
            return

        tbl_name = table_name or self.table_name
        processed_list = []
        for data in data_list:
            processed_data = data.copy()
            for key, value in processed_data.items():
                if isinstance(value, (list, dict)):
                    processed_data[key] = json.dumps(value, ensure_ascii=False)
            processed_list.append(processed_data)

        if not processed_list:
            return

        keys = processed_list[0].keys()

        # 使用 sql.Identifier 来安全地引用表、schema 和列名
        table_identifier = sql.Identifier(tbl_name)
        schema_identifier = sql.Identifier(self.schema)
        columns_identifiers = [sql.Identifier(k) for k in keys]
        unique_col_identifier = sql.Identifier(unique_col)

        excluded_fields = {'id', unique_col}
        update_clause_list = []
        for k in keys:
            if k not in excluded_fields:
                col_identifier = sql.Identifier(k)
                # 确保 EXCLUDED 也被正确引用
                update_clause_list.append(sql.SQL("{} = EXCLUDED.{}").format(col_identifier, col_identifier))

        update_clause = sql.SQL(', ').join(update_clause_list)

        # 构造安全的 SQL 查询
        query = sql.SQL("""
                        INSERT INTO {schema}.{table} ({columns})
                        VALUES %s
                        ON CONFLICT ({unique_col}) DO
                        UPDATE SET {update_clause}
                            RETURNING (xmax = 0) AS inserted;
                        """).format(
            schema=schema_identifier,
            table=table_identifier,
            columns=sql.SQL(', ').join(columns_identifiers),
            unique_col=unique_col_identifier,
            update_clause=update_clause
        )

        # 将字典列表转换为元组列表
        data_tuples = [tuple(d.get(k) for k in keys) for d in processed_list]

        try:
            # 确保在独立的事务中执行
            with self.connection.cursor() as cursor:
                results = psycopg2.extras.execute_values(
                    cursor,
                    query.as_string(cursor),  # 将 sql 对象转换为字符串
                    data_tuples,
                    template=None,
                    fetch=True
                )

                # 检查返回类型，因为游标可能不是 RealDictCursor
                if results and isinstance(results[0], (dict, psycopg2.extras.RealDictRow)):
                    insert_count = sum(1 for result in results if result.get('inserted', False))
                else:
                    insert_count = sum(1 for row in results if row[0])

                update_count = len(results) - insert_count

                result_stats = {
                    'inserted': insert_count,
                    'updated': update_count,
                    'total': len(results),
                    'input_count': len(processed_list)
                }

                self.connection.commit()
                print(f"批量数据插入/更新成功 table_name {tbl_name}: "
                      f"输入 {result_stats['input_count']} 条, "
                      f"插入 {result_stats['inserted']} 条, "
                      f"更新 {result_stats['updated']} 条, "
                      f"总计 {result_stats['total']} 条")
            return "success"
        except psycopg2.errors.UniqueViolation as e:
            # 当捕获到 UniqueViolation 时，调用我们的解析函数
            friendly_message = parse_unique_violation(e)
            print(f"\n--- 数据库操作提示 ---\n{friendly_message}\n-----------------------\n")
            # 您仍然可以保留后续的更新逻辑，或者根据需要进行其他处理
            self.connection.rollback()
            result_list = [self.insert_data(data, unique_col=unique_col, table_name=tbl_name) for data in
                           processed_list]
            count = Counter(result_list)
            print(f"逐个插入结果统计: {dict(count)}")
            return "unique_violation_detected"  # 返回一个明确的状态
        except Exception as e:
            print(f"批量数据插入/更新失败,改为逐个插入: {e}")
            self.connection.rollback()  # 回滚失败的批量操作
            result_list = [self.insert_data(data, unique_col=unique_col, table_name=tbl_name) for data in
                           processed_list]
            count = Counter(result_list)
            print(f"逐个插入结果统计: {dict(count)}")
            return  dict(count)

    def write_to_json_line(self, data: dict, table_name: str = None):
        """
        将单个字典对象以 JSON 格式追加到文件的末尾，占一行。
        文件名将基于 table_name 生成。
        :param data: 要写入的字典数据。
        :param table_name: 表名，用于生成文件名。如果为 None，则使用 self.table_name。
        """
        if not isinstance(data, dict):
            print("错误：提供的数据必须是字典。")
            return

        tbl_name = table_name or self.table_name
        file_path = f"{tbl_name}.json"

        try:
            with open(file_path, 'a', encoding='utf-8') as f:
                json_string = json.dumps(data, ensure_ascii=False)
                f.write(json_string + '\n')
            print(f"成功将 1 条记录写入到 '{file_path}'")
        except Exception as e:
            print(f"写入文件 '{file_path}' 失败: {e}")

    def write_to_json_lines(self, data_list: list, table_name: str = None):
        """
        将字典列表中的每个字典对象以 JSON 格式追加到文件的末尾，每个占一行。
        文件名将基于 table_name 生成。
        :param data_list: 包含字典的列表。
        :param table_name: 表名，用于生成文件名。如果为 None，则使用 self.table_name。
        """
        if not isinstance(data_list, list) or not all(isinstance(d, dict) for d in data_list):
            print("错误：提供的数据必须是字典列表。")
            return

        tbl_name = table_name or self.table_name
        file_path = f"{tbl_name}.json"
        count = 0
        try:
            with open(file_path, 'a', encoding='utf-8') as f:
                for data in data_list:
                    json_string = json.dumps(data, ensure_ascii=False)
                    f.write(json_string + '\n')
                    count += 1
            print(f"成功将 {count} 条记录写入到 '{file_path}'")
        except Exception as e:
            print(f"写入文件 '{file_path}' 失败: {e}")


    def delete_condition_data(self, condition, max_num: int = 1000):
        if not isinstance(condition, dict):
            print("条件必须是字典格式")
            return False
        if not condition:
            print("删除条件不能为空")
            return False
        if self.isMoreOneKiloRows(condition=condition, max_num=max_num):
            print(f"表 {self.table_name} 中数据超过 {max_num} 条，请再次确认是否删除，输入 y 确认删除，其他键取消")
            user_input = input("确认删除数据 (y/n): ")
            if user_input.lower() != 'y':
                print("取消删除数据操作")
                return False
        condition_str = ' AND '.join([f'"{key}" = %s' for key in condition.keys()])
        delete_query = f'DELETE FROM "{self.schema}"."{self.table_name}" WHERE {condition_str}'
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(delete_query, tuple(condition.values()))
                self.connection.commit()
                print(f"符合条件的数据已删除: {cursor.rowcount} 条")
                cursor.close()
            return True
        except psycopg2.Error as e:
            print(f"查询数据条数失败: {e}")
            return False

    def close(self):
        if self.connection:
            self.connection.close()
        print("数据库连接已关闭。")

    def create_table(self, create_sql):
        try:
            if "CREATE TABLE" not in create_sql:
                print("SQL语句不包含CREATE TABLE")
                return
            if self.table_name not in create_sql:
                print(f"SQL语句中不包含表名: {self.table_name}")
                return
            if "spider" not in create_sql:
                print("SQL语句中不包含 schema 'spider'")
                return
            if self.is_has_table(self.table_name):
                print(f"表 {self.table_name} 已存在，无需创建")
                return
            with self.connection.cursor() as cursor:
                cursor.execute(create_sql)
                self.connection.commit()
                cursor.close()
            print(f"表 {self.table_name} 创建成功")
        except psycopg2.Error as e:
            print(f"表 {self.table_name} 创建失败: {e}")
            try:
                self.connection.rollback()
            except Exception as e:
                self.connection = self.get_db_connection(self.return_type)
                self.connection.rollback()

    def drop_table(self, max_num: int = 100):
        query = f'DROP TABLE IF EXISTS "{self.schema}"."{self.table_name}"'
        try:
            if not self.is_has_table(self.table_name):
                print(f"\r表 {self.table_name} 不存在，无法删除")
                return False
            if self.isMoreOneKiloRows(max_num=max_num):
                print(f"表 {self.table_name} 中数据超过 {max_num} 条，默认不删除")
                return False
            if max_num > 1000:
                print(f"<UNK>max_num参数{max_num}过大<UNK> 上限1000，最多可以删除含1000条数据的表")
                return False
            with self.connection.cursor() as cursor:
                cursor.execute(query)
                self.connection.commit()
                print(f"表删除成功 ,行数小于{max_num}条,默认删除")
                cursor.close()
                return True
        except psycopg2.Error as e:
            print(f"表删除失败: {e}")
            try:
                self.connection.rollback()
            except Exception as e:
                self.connection = self.get_db_connection(self.return_type)
                self.connection.rollback()
            return False

    def clear_table(self, max_num=100):
        query = f'TRUNCATE TABLE "{self.schema}"."{self.table_name}"'
        try:
            if not self.is_has_table(self.table_name):
                print(f"\r表 {self.table_name} 不存在，无法清空")
                return False
            if self.isMoreOneKiloRows(max_num=max_num):
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
        except psycopg2.Error as e:
            print(f"表 {self.table_name} 数据清空失败: {e}")
            try:
                self.connection.rollback()
            except Exception as e:
                self.connection = self.get_db_connection(self.return_type)
                self.connection.rollback()
            return False

    def is_has_table(self, table_name):
        sql = """
              SELECT COUNT(*)
              FROM information_schema.tables
              WHERE table_schema = %s \
                AND table_name = %s; \
              """
        for i in range(3):
            try:
                with self.connection.cursor() as cursor:
                    cursor.execute(sql, (self.schema, table_name))
                    fetchone = cursor.fetchone()
                    if fetchone is not None:
                        if isinstance(fetchone,dict):
                            return fetchone.get('count', 0) == 1
                        else:
                            return fetchone[0] == 1
                    else:
                        # 如果 fetchone 为 None，说明查询异常，重试
                        print(f"is_has_table: 检查表是否存在时返回值异常，重试 {i + 1}/3")
                        continue
            except psycopg2.Error as e:
                print(f"检查表是否存在时出错: {e}")
                # 如果是事务中止错误，必须回滚
                if "current transaction is aborted" in str(e):
                    try:
                        self.connection.rollback()
                    except psycopg2.Error as rb_e:
                        print(f"回滚失败，重新连接: {rb_e}")
                        self.connection = self.get_db_connection(self.return_type)
                else:  # 其他错误，也尝试重连
                    self.connection = self.get_db_connection(self.return_type)

                time.sleep(1)  # 等待一秒再重试

        print(f"is_has_table: 经过多次重试后仍无法确定表 '{table_name}' 是否存在。")
        return False

    def isMoreOneKiloRows(self, condition: dict = None, max_num: int = 1000):
        if condition:
            condition_str = ' AND '.join([f'"{key}" = %s' for key in condition.keys()])
            query = f'SELECT COUNT(*) FROM "{self.schema}"."{self.table_name}" WHERE {condition_str}'
            params = tuple(condition.values())
        else:
            query = f'SELECT COUNT(*) FROM "{self.schema}"."{self.table_name}"'
            params = None
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query, params)
                count = cursor.fetchone()[0]
                print(f" 表 {self.table_name} 查询到数据 {count} <UNK> ", end="\t")
                return count > max_num
        except psycopg2.Error as e:
            print(f"查询数据条数失败: {e}")
            return True

    def getMinMaxId(self, table_name=None):
        if not table_name:
            table_name = self.table_name
        query = f'SELECT MIN(id) AS min_id, MAX(id) AS max_id FROM "{self.schema}"."{table_name}"'
        try:
            with self.connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                cursor.execute(query)
                result = cursor.fetchone()
                if result:
                    return result['min_id'], result['max_id']
                else:
                    return None, None
        except psycopg2.Error as e:
            print(f"查询最小和最大 ID 失败: {e}")
            return None, None

# # --- main 测试 ---
# if __name__ == '__main__':
#     db_handler = PostgreSQLHandler(db_name='postgres', table_name='test_articles')
#     create_sql = """
#         CREATE TABLE IF NOT EXISTS "{self.schema}"."test_articles" (
#             id SERIAL PRIMARY KEY,
#             title VARCHAR(255) NOT NULL,
#             content TEXT,
#             article_url VARCHAR(512) NOT NULL UNIQUE,
#             tags JSON,
#             created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
#         );
#     """
#     db_handler.create_table(create_sql)
#     article_1 = {
#         'title': 'PostgreSQL is Awesome',
#         'content': 'This is a post about using PostgreSQL.',
#         'article_url': 'http://example.com/pg',
#         'tags': ['python', 'database', 'postgresql']
#     }
#     status = db_handler.insert_data(article_1)
#     print(f"Operation status: {status}")
#
#     article_1_updated = {
#         'title': 'PostgreSQL is Really Awesome!',
#         'content': 'An updated post about using PostgreSQL.',
#         'article_url': 'http://example.com/pg',
#         'tags': ['python', 'orm']
#     }
#     status = db_handler.insert_data(article_1_updated)
#     print(f"Operation status: {status}")
#
#     articles_list = [
#         {
#             'title': 'Understanding Connection Pools',
#             'content': 'A deep dive into database connection pooling.',
#             'article_url': 'http://example.com/pools',
#             'tags': ['performance', 'database']
#         },
#         {
#             'title': 'Getting Started with Psycopg2',
#             'content': 'A beginner guide.',
#             'article_url': 'http://example.com/psycopg2',
#             'tags': ['python', 'postgresql']
#         }
#     ]
#     db_handler.insert_data_list(articles_list)
#
#     all_articles = db_handler.execute_query('SELECT * FROM  "{self.schema}"."test_articles"')
#     if all_articles:
#         for article in all_articles:
#             print(article)
#
#     db_handler.delete_condition_data({'article_url': 'http://example.com/psycopg2'}, max_num=10)
#     min_id, max_id = db_handler.getMinMaxId()
#     print(f"Min ID: {min_id}, Max ID: {max_id}")
#
#     db_handler.clear_table(max_num=100)
#     db_handler.drop_table(max_num=100)
#     db_handler.close()
