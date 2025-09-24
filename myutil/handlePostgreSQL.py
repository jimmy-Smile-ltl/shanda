# -*- coding: utf-8 -*-
import json
import time
from collections import Counter
import psycopg2
import psycopg2.extras

"""
PostgreSQL 数据库处理类


"""
class PostgreSQLHandler:
    def __init__(self, db_name, table_name, return_type="tuple"):
        self.db_name = db_name
        self.table_name = table_name
        self.schema = 'spider'
        self.return_type = return_type
        self.connection = self.get_db_connection(self.return_type)

    def get_db_connection(self, return_type="tuple"):
        for i in range(20):
            try:
                if return_type.lower() == "dict":
                    return psycopg2.connect(
                        host='127.0.0.1',
                        user='postgres',
                        password="",
                        dbname=self.db_name,
                        connect_timeout=30,
                        cursor_factory=psycopg2.extras.RealDictCursor
                    )
                else:
                    return psycopg2.connect(
                        host='127.0.0.1',
                        user='postgres',
                        password="",
                        dbname=self.db_name,
                        connect_timeout=30
                    )
            except Exception as e:
                print(f"连接数据库失败: {e}, 重试 {i + 1}/10")
                sleep_time = 30 * 4 * (i + 1)
                time.sleep(sleep_time)
                if i == 9:
                    raise Exception("无法连接到数据库，请检查配置或网络连接。")
        return None

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

    def insert_data(self, data, unique_col="article_url") -> None | str:
        if not isinstance(data, dict):
            print("数据必须是字典格式")
            return

        processed_data = data.copy()
        for key, value in processed_data.items():
            if isinstance(value, (list, dict)):
                processed_data[key] = json.dumps(value, ensure_ascii=False)

        columns = ', '.join([f'"{k}"' for k in processed_data.keys()])
        placeholders = ', '.join([f'%({k})s' for k in processed_data.keys()])

        # 第一步：尝试插入
        insert_query = f'INSERT INTO "{self.schema}"."{self.table_name}" ({columns}) VALUES ({placeholders})'

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
                            update_query = f'UPDATE "{self.schema}"."{self.table_name}" SET {update_clause} WHERE "{unique_col}" = %({unique_col})s'
                            with self.connection.cursor() as cursor:
                                cursor.execute(update_query, processed_data)
                                self.connection.commit()
                                # 移除 cursor.close()
                        else:
                            print("没有需要更新的字段")
                    return "update"
                except Exception as update_e:
                    print(f"更新失败: {update_e}")
                    try:
                        self.connection.rollback()
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

    def insert_data_list(self, data_list, unique_col="article_url") -> None | str:
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

        keys = processed_list[0].keys()
        columns = ', '.join([f'"{k}"' for k in keys])
        placeholders = ', '.join([f'%({k})s' for k in keys])

        # 关键修改：排除 id 字段
        excluded_fields = {'id', unique_col}
        update_clause = ', '.join([
            f'"{k}" = EXCLUDED."{k}"'
            for k in keys
            if k not in excluded_fields
        ])
        # 这种直接就更新了,问题是更新了多少行呢,插入多少行呢? 这个其实最好做个统计的
        query = f'INSERT INTO "{self.schema}"."{self.table_name}" ({columns}) VALUES ({placeholders}) ON CONFLICT ("{unique_col}") DO UPDATE SET {update_clause}'
        # query = f'INSERT INTO "{self.schema}"."{self.table_name}" ({columns}) VALUES ({placeholders})
        try:
            with self.connection.cursor() as cursor:
                psycopg2.extras.execute_batch(cursor, query, processed_list)
                affected_rows = cursor.rowcount
                # 获取统计结果
                stats = cursor.fetchone()
                if stats:
                    insert_count, update_count, total_count = stats
                    result = {
                        'inserted': insert_count,
                        'updated': update_count,
                        'total': total_count,
                        'input_count': len(processed_list)
                    }
                else:
                    # 如果CTE方式不支持，fallback到简单统计
                    affected_rows = cursor.rowcount
                    result = {
                        'inserted': 0,
                        'updated': 0,
                        'total': affected_rows,
                        'input_count': len(processed_list)
                    }
                self.connection.commit()
                print(f"批量数据插入/更新成功 table_name {self.table_name}: "
                      f"输入 {result['input_count']} 条, "
                      f"插入 {result.get('inserted', '未知')} 条, "
                      f"更新 {result.get('updated', '未知')} 条, "
                      f"总计 {result['total']} 条")
            return "insert"
        except Exception as e:
            # 这个不会触发了,不知道更新了多少啊 怎么办呢
            print(f"批量数据插入/更新失败,改为一个个插入:{e}")
            try:
                self.connection.rollback()
                # insert one by one
                result_list = []
                for data in processed_list:
                    result = self.insert_data(data, unique_col=unique_col)
                    result_list.append(result)
                count = Counter(result_list)
                print(f"批量插入结果统计: {dict(count)}")
            except Exception as e:
                self.connection = self.get_db_connection(self.return_type)
                self.connection.rollback()

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

    def is_has_table(self, table_name):
        try:
            sql = """
                SELECT COUNT(*)
                FROM information_schema.tables
                WHERE table_schema = %s AND table_name = %s;
            """
            for i in range(3):
                with self.connection.cursor() as cursor:
                    cursor.execute(sql, ("spider", table_name))
                    fetchone = cursor.fetchone()
                    cursor.close()
                    if isinstance(fetchone, tuple):
                        if fetchone[0] == 1:
                            return True
                        else:
                            return False
                    else:
                        print(f"is_has_table方法 检查表是否存在时返回值异常: {fetchone}")
                        continue
            else:
                return False
        except Exception as e:
            print(f"检查表是否存在时出错: {e}")
            return False

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
