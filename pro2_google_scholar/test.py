import datetime
import json
import os.path
import urllib.parse
from urllib.parse import urljoin
import requests
from bs4 import BeautifulSoup
import re
from urllib.parse import urlparse, parse_qs
import time
from myutil.cache import Cache
from myutil.handleDatetime import convert_date_robust
from myutil.handlePostgreSQL import  PostgreSQLHandler
from myutil.handleRequest import SingleRequestHandler, AsyncRequestHandler, ThreadRequestHandler,CurlRequestHandler
from myutil.handleSoup import extractSoup
from myutil.log_print import LogPrint
from myutil.maintainSourceInfo import MaintainSourceInfoPG
from get_author_info_by_id import GetAuthorInfoById
from get_artilce_by_title import GetArticleByTitle
class TestGoogleScholar:
    def __init__(self):
         self.site = "https://scholar.google.com/"
         self.headers = {
             "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
             "accept-language": "en",
             "cache-control": "max-age=0",
             "priority": "u=0, i",
             "referer": "https://scholar.google.com/scholar?hl=zh-CN&as_sdt=0%2C5&q=Why+and+How+Auxiliary+Tasks+Improve+JEPA+Representations&btnG=",
             "sec-ch-ua": "\"Chromium\";v=\"140\", \"Not=A?Brand\";v=\"24\", \"Google Chrome\";v=\"140\"",
             "sec-ch-ua-arch": "\"arm\"",
             "sec-ch-ua-bitness": "\"64\"",
             "sec-ch-ua-full-version-list": "\"Chromium\";v=\"140.0.7339.186\", \"Not=A?Brand\";v=\"24.0.0.0\", \"Google Chrome\";v=\"140.0.7339.186\"",
             "sec-ch-ua-mobile": "?0",
             "sec-ch-ua-model": "\"\"",
             "sec-ch-ua-platform": "\"macOS\"",
             "sec-ch-ua-platform-version": "\"15.6.0\"",
             "sec-ch-ua-wow64": "?0",
             "sec-fetch-dest": "document",
             "sec-fetch-mode": "navigate",
             "sec-fetch-site": "same-origin",
             "sec-fetch-user": "?1",
             "upgrade-insecure-requests": "1",
             "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36",
             "x-browser-channel": "stable",
             "x-browser-copyright": "Copyright 2025 Google LLC. All rights reserved.",
             "x-browser-validation": "jFliu1AvGMEE7cpr93SSytkZ8D4=",
             "x-browser-year": "2025"
         }
         self.cookies = {
             "GSP": "LM=1758679529:S=ZOze-rRwzGbwyAih",
             "NID": "525=lowb-5kxZdNRIhhp83qUi9wfXxM-SHfjVpZI8YPYHpp4gBxx8I1QhjZllbvKHg94uActoOavEPKKtk_FD1ocsTRshad8wJXGuayPb0yo6WzSBKwB4gPw-XgWxe1mTZUHzTol1uT2xir46SkCZ3104I3ILBKdZ12LnFLv1aFRHw-kTp6IWHac9YcIU0KRchKe8MURjePJTdbG"
         }
         test_url = self.site
         test_url = None
         self.single_handler = SingleRequestHandler(
             test_url=test_url,  # 测试链接，避免请求过多导致IP被封
         )
         self.log_print = LogPrint()
         self.db_name = "postgres"
         self.table_name = "article_arxiv_org"
         self.log_offset = Cache("google_scholar_log_offset")
         self.postgreSQL_handler = PostgreSQLHandler(db_name=self.db_name, table_name=self.table_name, return_type="dict")
         max_id, min_id = self.postgreSQL_handler.getMinMaxId()
         self.max_id = max_id
         self.min_id = min_id
         self.log_print.print(f"table:{ self.table_name } max_id: {self.max_id}, min_id: {self.min_id}")

    def create_table_article_search(self,table_name =None ):
        if not table_name:
            table_name = "spider.article_search_by_google_scholar"
        sql =f"""
        CREATE TABLE  if not exists { table_name }(
                    id BIGSERIAL PRIMARY KEY,
                    article_title TEXT,
                    article_url TEXT UNIQUE,
                    author_dict_list JSONB,
                    publish_info TEXT,
                    origin_id BIGINT,
                    cited_num INTEGER,
                    html TEXT,
                    origin_table VARCHAR(255),
                    origin_title TEXT,
                    article_idx INTEGER,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                
                    -- 要求的联合唯一索引
                    CONSTRAINT idx_articles_origin_article_unique UNIQUE (origin_id, origin_table, origin_title, article_idx)
                );
                -- 创建一个触发器函数，用于在更新行时自动更新 updated_at 字段
                CREATE OR REPLACE FUNCTION trigger_set_timestamp()
                RETURNS TRIGGER AS $$
                BEGIN
                  NEW.updated_at = NOW();
                  RETURN NEW;
                END;
                $$ LANGUAGE plpgsql;
                
                -- 将触发器绑定到表上
                CREATE TRIGGER set_timestamp
                BEFORE UPDATE ON { table_name }
                FOR EACH ROW
                EXECUTE FUNCTION trigger_set_timestamp();
            """
        is_success = self.postgreSQL_handler.execute(sql)
        is_has_table = self.postgreSQL_handler.is_has_table(table_name.split(".")[-1])
        result = f"create table {table_name} is_success: {is_success}, is_has_table: {is_has_table}"
        self.log_print.print(result)

    def create_table_author_info(self , table_name =None):
        if not table_name:
            table_name = "spider.scholar_author"
        sql = f"""
            CREATE TABLE  if not exists {table_name} (
                id BIGSERIAL PRIMARY KEY,
                scholar_id VARCHAR(255) UNIQUE NOT NULL,
                name TEXT,
                avatar_url TEXT,
                profile_url TEXT,
                scholar_index JSONB,
                affiliation TEXT,
                category JSONB,
                cite_per_year JSONB,
                open_access_num INTEGER,
                non_open_access_num INTEGER,
                collaborator_list JSONB,
                article_list JSONB,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            
            -- 为常用的查询字段创建索引
            CREATE INDEX if not exists  idx_scholar_authors_name ON { table_name }(name);
            
            -- 创建一个触发器函数，用于在更新行时自动更新 updated_at 字段
            -- (如果之前的表中已创建，则无需重复创建此函数)
            CREATE OR REPLACE FUNCTION  trigger_set_timestamp()
            RETURNS TRIGGER AS $$
            BEGIN
              NEW.updated_at = NOW();
              RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;
            
            -- 将触发器绑定到新表上
            CREATE TRIGGER set_timestamp
            BEFORE UPDATE ON {table_name}
            FOR EACH ROW
            EXECUTE FUNCTION trigger_set_timestamp();
       """
        is_success = self.postgreSQL_handler.execute(sql)
        is_has_table = self.postgreSQL_handler.is_has_table(table_name.split(".")[-1])
        result = f"create table {table_name} is_success: {is_success}, is_has_table: {is_has_table}"
        self.log_print.print(result)


    def get_data_list_by_id(self, start_id, end_id):
        sql = f'select id, article_title from "spider"."{self.table_name}" where id>={start_id} and id<={end_id} order by id asc;'
        data_list = self.postgreSQL_handler.execute_query(sql)
        return data_list

if __name__ == '__main__':
    test = TestGoogleScholar()
    table_name_aricle = "spider.article_search_by_google_scholar"
    test.create_table_article_search(table_name = table_name_aricle)
    table_name_author = "spider.scholar_author"
    test.create_table_author_info(table_name = table_name_author)
    data_list = test.get_data_list_by_id(1, 20)
    for data in data_list:
        print(data)
        title = data['article_title']
        article_id = data['id']
        get_article = GetArticleByTitle(title=title)
        article_list = get_article.run()
        if not article_list:
            print(f"未找到结果: title={title}, article_id={article_id}")
            continue
        print(f"article_id: {article_id}, title: {title}, article_info: {len(article_list)}")
        for idx, article in enumerate(article_list):
            article.update(
                {
                    "origin_id": article_id,
                    "origin_table": test.table_name,
                    "origin_title": title,
                    "article_idx": idx + 1
                }
            )
            insert_result = test.postgreSQL_handler.insert_data(table_name=table_name_aricle.split(".")[-1], data=article,unique_col=None)
            print("insert_result: ", insert_result)
            author_dict_list = article.get("author_dict_list", [])
            for author in author_dict_list:
                if not author.get("url"):
                    continue
                # 1. 解析 URL
                url = author.get("url")
                parsed_url = urlparse(url)
                # 2. 将查询字符串解析为字典
                # parse_qs 的值是一个列表，因为同一个参数可能在 URL 中出现多次
                query_params = parse_qs(parsed_url.query)

                # 3. 从字典中获取 'user' 参数的值
                # .get('user', [None]) 提供一个默认值以避免 KeyErorr
                # [0] 用于从列表中取出第一个（也是唯一的）值
                author_id = query_params.get('user', [None])[0]
                if not author_id:
                    print(f"url: {author.get('url')} not author_id ,check please")
                    continue
                get_author = GetAuthorInfoById(scholar_id=author_id)
                author_info = get_author.run()
                if not author_info:
                    print(f"author_id: {author_id}, name: {author.get('name')} get_author_info None, check please")
                    continue
                insert_result = test.postgreSQL_handler.insert_data(table_name=table_name_author.split(".")[-1], data=author_info,
                                                                    unique_col=None)
                print("insert_result  author: ", insert_result)
        time.sleep(5)

