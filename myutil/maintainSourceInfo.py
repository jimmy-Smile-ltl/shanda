import json
import traceback
import psycopg2
from  myutil.handlePostgreSQL import PostgreSQLHandler


class MaintainSourceInfoPG:
    '''
    维护采集源信息的类，记录采集网站及相关存储信息，PostgreSQL表名，hdfs路径等
    '''

    def __init__(self):
        self.db_config = {
            "host": "10.0.102.52",
            "user": "root",
            "password": "123456",
            "database": "collection",
            "port": 5432,
        }
        self.table_name = 'source_collection_info'
        self.schema = 'spider'
        self.db_handler = PostgreSQLHandler(db_name='postgres', table_name=self.table_name)


    def create_source_info_table(self):
        """创建采集源信息表"""
        create_sql = f'''
        CREATE TABLE IF NOT EXISTS "{self.schema}"."{self.table_name}" (
            id SERIAL PRIMARY KEY,
            source_name VARCHAR(256) NOT NULL,
            source_url VARCHAR(512),
            category VARCHAR(128) NOT NULL,
            database_name VARCHAR(128),
            mysql_table VARCHAR(128) NOT NULL,
            schema VARCHAR(64) DEFAULT 'spider',
            count INT DEFAULT 0,
            create_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            update_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(category, mysql_table, source_url)
        );

        CREATE OR REPLACE FUNCTION update_modified_column()
        RETURNS TRIGGER AS $$
        BEGIN
            NEW.update_time = CURRENT_TIMESTAMP;
            RETURN NEW;
        END;
        $$ language 'plpgsql';

        DROP TRIGGER IF EXISTS update_{self.table_name}_modtime ON "{self.schema}"."{self.table_name}";
        CREATE TRIGGER update_{self.table_name}_modtime
            BEFORE UPDATE ON "{self.schema}"."{self.table_name}"
            FOR EACH ROW
            EXECUTE FUNCTION update_modified_column();
        '''

        try:
            self.db_handler.create_table(create_sql)
        except Exception as e:
            print(f"创建表 {self.schema}.{self.table_name} 失败: {e}")
            traceback.print_exc()
            self.db_handler.connect()

    def insert_source_info(self, data, debug=False):
        """
        参数：
        - data: dict，要插入的字段和值（字段必须存在于表结构中）
        """
        if not isinstance(data, dict):
            print("数据必须是字典格式")
            return
        if not self.db_handler.is_has_table(self.table_name):
            self.create_source_info_table()

        category = data.get('category', None)
        mysql_table = data.get('mysql_table', None)
        schema = data.get('schema', 'spider')

        if not mysql_table or not category:
            print("category 和 mysql_table 必须提供，不能是空字符串，不能为None")
            return
        try:
            result = self.db_handler.insert_data(data=data)
            if not result:
                print("异常结果，维护表信息失败")
            elif isinstance(result,str)  and result.lower().strip()=="insert":
                if debug:
                    print(f"表信息插入数据成功: {data}")
            elif isinstance(result,str)  and result.lower().strip()=="update":
                if debug:
                    print(f"表信息更新数据成功: {data}")
            else:
                print(f"未知结果: {result}")
        except Exception as e:
            print(f"插入数据失败: {data}, 错误: {e}")
            traceback.print_exc()
            self.db_handler.connect()

if __name__ == "__main__":
    maintain = MaintainSourceInfoPG()
    maintain.create_source_info_table()
    test_data = {
        'source_name': 'arxiv',
        'source_url': "https://arxiv.org/",
        'category': '预印本',
        'database_name': 'postgres',
        'mysql_table': 'article_arxiv_org',
        'schema': 'spider'
    }
    maintain.insert_source_info(test_data, debug=True)


