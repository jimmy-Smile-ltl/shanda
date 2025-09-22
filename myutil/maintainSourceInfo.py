import json
import traceback

import pymysql


class MaintainSourceInfo:
    '''
    维护采集源信息的类，记录采集网站及相关存储信息，mysql表名，hadoop路径等
    '''

    def __init__(self):
        self.db_config = {
            "host": "10.0.102.52",
            "user": "root",
            "password": "123456",
            "database": "collection",
            "port": 3306,
        }
        self.table_name = 'source_collection_info'
        self.connection = None
        self.connect()

    def connect(self):
        try:
            connection = pymysql.connect(
                host=self.db_config['host'],
                user=self.db_config['user'],
                password=self.db_config['password'],
                database=self.db_config['database'],
                port=self.db_config.get('port', 3306),
                charset='utf8mb4',
                connect_timeout=5
            )
            self.connection = connection
            return connection
        except pymysql.MySQLError as e:
            print("数据库连接失败:", e)
            raise

    def close(self):
        if self.connection:
            self.connection.close()

    def insert_source_info(self, data, debug=False):
        """
        参数：
        - db_config: dict，数据库连接配置（host, user, password, database, port）
        - data_dict: dict，要插入的字段和值（字段必须存在于表结构中）
        """
        if not isinstance(data, dict):
            print("数据必须是字典格式")
            return
        category = data.get('category', None)
        mysql_table = data.get('mysql_table', None)
        where_condition = data.get('where_condition', None)
        if not where_condition or not mysql_table or not category:
            print(
                "category   mysql_table 和 where_condition 必须提供,不能是空字符串（可替换为无）,不能为None（索引将失效）,是联合唯一索引")
            return
        processed_data = data.copy()
        for key, value in processed_data.items():
            if isinstance(value, (list, dict)):
                processed_data[key] = json.dumps(value, ensure_ascii=False)

        columns = ', '.join([f'`{k}`' for k in processed_data.keys()])
        placeholders = ', '.join(['%s'] * len(processed_data))
        query = f"INSERT INTO `{self.table_name}` ({columns}) VALUES ({placeholders}) "
        with self.connection.cursor() as cursor:
            try:
                cursor.execute(query, tuple(processed_data.values()))
                self.connection.commit()
                print(f"采集源{data['source_name']} 信息插入成功")
            except pymysql.err.ProgrammingError as pe:
                print("SQL语法错误:", pe)
            except pymysql.err.OperationalError as oe:
                print("数据库连接失败:", oe)
            except Exception as e:
                try:
                    # 构建 ON DUPLICATE KEY UPDATE 子句
                    # 我们将更新除了主键和创建时间之外的所有字段
                    update_clause_parts = []
                    processed_data = data.copy()
                    for key in processed_data.keys():
                        update_clause_parts.append(f"`{key}` = VALUES(`{key}`)")
                    update_clause = ', '.join(update_clause_parts)
                    columns = ', '.join([f'`{k}`' for k in processed_data.keys()])
                    placeholders = ', '.join(['%s'] * len(processed_data))
                    query = f"INSERT INTO `{self.table_name}` ({columns}) VALUES ({placeholders}) " + f"ON DUPLICATE KEY UPDATE {update_clause}"
                    cursor.execute(query, tuple(processed_data.values()))
                    self.connection.commit()
                    print(f"采集源{data['source_name']} 信息更新成功")
                except Exception as e:
                    print("插入或更新数据时发生错误:", e)
                    traceback.print_exc()
                    self.connection.rollback()

# if __name__ == "__main__":
#     maintain_table = MaintainSourceInfo()
#     # data = {
#     #     'source_name':'Frontiers（英文）',
#     #     'source_url': "https://www.frontiersin.org",
#     #     'category': '科技文献',
#     #     'database_name': 'collection',
#     #     'mysql_table': 'science_frontiers',
#     #     'hdfs_path': '/science_frontiers',
#     #     'is_mixed_data': 0,
#     #     'where_condition': "无",
#     # }
#     data = {
#         'source_name':'pubscholar',
#         'source_url': "https://pubscholar.cn/",
#         'category': '发明专利',
#         'database_name': 'collection',
#         'mysql_table': 'pubscholar_zhuanli',
#         'hdfs_path': "无",
#         'is_mixed_data': 0,
#         'where_condition': "无",
#     }
#     maintain_table.insert_source_info(data, debug=True)
#     # insert_source_info(db_config, data, debug=True)
#     db_config = {
#         "host": "10.0.102.52",
#         "user": "root",
#         "password": "123456",
#         "database": "collection",
#         "port": 3306,
#     }

# data = {
#     'source_name':'F1000Research（英文）',
#     'source_url': 'https://f1000research.com/',
#     'category': '科技文献',
#     'database_name': 'collection',
#     'mysql_table': 'science_f1000research',
#     'hdfs_path': '/science_f1000research',
#     'is_mixed_data': 0,
#     'where_condition': None,
# }
# data = {
#     'source_name':'OSF（英文）',
#     'source_url': "https://osf.io/",
#     'category': '科技文献',
#     'database_name': 'collection',
#     'mysql_table': 'science_osf',
#     'hdfs_path': '/science_osf',
#     'is_mixed_data': 0,
#     'where_condition': None,
# }
