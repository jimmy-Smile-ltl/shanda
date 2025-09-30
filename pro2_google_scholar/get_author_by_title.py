
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
from pro2_google_scholar.get_artilce_by_title import GetArticleByTitle
from pro2_google_scholar.get_author_info_by_id import GetAuthorInfoById
# 在文件顶部添加导入
from concurrent.futures import ThreadPoolExecutor
import concurrent.futures
import concurrent

class GetAuthorByTitle:
    def __init__(self,table_name_read = "article_arxiv_org"):
         self.site = "https://scholar.google.com/"
         self.headers = {
             "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
             "accept-language": "en",
             "cache-control": "max-age=0",
             "priority": "u=0, i",
             "referer": "https://scholar.google.com/scholar?hl=zh-CN&as_sdt=0%2C5&q=Why+and+How+Auxiliary+Tasks+Improve+JEPA+Representations&btnG=",
             # "sec-ch-ua": "\"Chromium\";v=\"140\", \"Not=A?Brand\";v=\"24\", \"Google Chrome\";v=\"140\"",
             # "sec-ch-ua-arch": "\"arm\"",
             # "sec-ch-ua-bitness": "\"64\"",
             # "sec-ch-ua-full-version-list": "\"Chromium\";v=\"140.0.7339.186\", \"Not=A?Brand\";v=\"24.0.0.0\", \"Google Chrome\";v=\"140.0.7339.186\"",
             # "sec-ch-ua-mobile": "?0",
             # "sec-ch-ua-model": "\"\"",
             # "sec-ch-ua-platform": "\"macOS\"",
             # "sec-ch-ua-platform-version": "\"15.6.0\"",
             # "sec-ch-ua-wow64": "?0",
             # "sec-fetch-dest": "document",
             # "sec-fetch-mode": "navigate",
             # "sec-fetch-site": "same-origin",
             # "sec-fetch-user": "?1",
             "upgrade-insecure-requests": "1",
             "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36",
             # "x-browser-channel": "stable",
             # "x-browser-copyright": "Copyright 2025 Google LLC. All rights reserved.",
             # "x-browser-validation": "jFliu1AvGMEE7cpr93SSytkZ8D4=",
             # "x-browser-year": "2025"
         }
         self.cookies = {
             # "GSP": "LM=1758679529:S=ZOze-rRwzGbwyAih",
             # "NID": "525=lowb-5kxZdNRIhhp83qUi9wfXxM-SHfjVpZI8YPYHpp4gBxx8I1QhjZllbvKHg94uActoOavEPKKtk_FD1ocsTRshad8wJXGuayPb0yo6WzSBKwB4gPw-XgWxe1mTZUHzTol1uT2xir46SkCZ3104I3ILBKdZ12LnFLv1aFRHw-kTp6IWHac9YcIU0KRchKe8MURjePJTdbG"
         }
         test_url = self.site
         # test_url = None
         self.single_handler = SingleRequestHandler(
             test_url=test_url,  # 测试链接，避免请求过多导致IP被封
         )
         self.log_print = LogPrint(name =f"GetAuthorByTitle_{table_name_read}")
         self.db_name = "postgres"
         self.table_name_read = table_name_read
         self.log_offset = Cache(f"log_offset_get_author_{table_name_read}")
         self.postgreSQL_handler = PostgreSQLHandler(db_name=self.db_name, table_name=self.table_name_read, return_type="dict")
         self.table_name_article = "spider.article_search_by_google_scholar"
         self.create_table_article_search(table_name=self.table_name_article)
         self.table_name_author = "spider.scholar_author"
         self.create_table_author_info(table_name=self.table_name_author)
         min_id ,max_id  = self.postgreSQL_handler.getMinMaxId()
         self.max_id = max_id
         self.min_id = min_id
         self.log_print.print(f"table:{ self.table_name_read } max_id: {self.max_id}, min_id: {self.min_id}")

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
        sql = f'select id, article_title from "spider"."{self.table_name_read}" where id>={start_id} and id<={end_id} order by id asc;'
        data_list = self.postgreSQL_handler.execute_query(sql)
        return data_list

    def handle_one_title(self,title:str,article_id:int):
        self.log_offset.record_int(article_id)
        start_time = time.time()
        if not title or not article_id:
            self.log_print.print(f"两者都不能为空 title: {title}, id: {id}")

        get_article = GetArticleByTitle(title=title)
        article_list, info = get_article.run()
        if not article_list:
            self.log_print.print(f"未找到结果: title={title}, article_id={article_id} info={info}")
            return
        self.log_print.print(f"article_id: {article_id}, title: {title}, article_len: {len(article_list)}")
        for idx, article in enumerate(article_list):

            article.update(
                {
                    "origin_id": article_id,
                    "origin_table": self.table_name_read,
                    "origin_title": title,
                    "article_idx": idx + 1
                }
            )
            insert_result = self.postgreSQL_handler.insert_data(table_name=self.table_name_article.split(".")[-1],
                                                                      data=article, unique_col=None)
            print("insert_result    article: ", insert_result)
            if insert_result == "update":
                print(f"结果为 update , 说明已经存在，跳过 author 处理")
                continue
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
                author_info ,info  = get_author.run()
                if not author_info :
                    self.log_print.print(f"author_id: {author_id}, name: {author.get('name')} get_author_info {info}, check please")
                    continue
                if author_info is True:
                    # print(f"author_id: {author_id}, name: {author.get('name')} 已存在")
                    continue
                insert_result = self.postgreSQL_handler.insert_data(table_name=self.table_name_author.split(".")[-1],
                                                                    data=author_info,
                                                                    unique_col=None)
                self.log_print.print(f"insert_result    author: {insert_result} , info:{info}")
        end_time = time.time()
        take_time = end_time - start_time
        self.log_print.print(f"标题与作者信息处理完毕 耗时{take_time:.4f}s  article_id: {article_id}, title: {title}")
        time.sleep(2)

    def handle_one_title_thread(self, title: str, article_id: int, max_workers=5):
        """
        handle_one_title 的优化版本。
        1. 使用多线程并发获取作者信息。
        2. 在所有请求完成后，批量插入数据到数据库。
        """
        self.log_offset.record_int(article_id)
        start_time = time.time()
        if not title or not article_id:
            self.log_print.print(f"标题或ID不能为空 title: {title}, id: {article_id}")
            return

        # 1. 获取文章列表
        get_article = GetArticleByTitle(title=title)
        article_list, info = get_article.run()
        if not article_list:
            self.log_print.print(f"未找到文章结果: title={title}, article_id={article_id} info={info}")
            return

        self.log_print.print(f"article_id: {article_id}, title: {title}, 找到 {len(article_list)} 篇文章")

        articles_to_insert = []
        authors_to_fetch = {}  # 使用字典去重：{author_id: author_name}

        # 2. 收集需要插入的文章和需要获取的作者
        for idx, article in enumerate(article_list):
            article.update({
                "origin_id": article_id,
                "origin_table": self.table_name_read,
                "origin_title": title,
                "article_idx": idx + 1
            })
            articles_to_insert.append(article)

            for author in article.get("author_dict_list", []):
                author_url = author.get("url")
                if not author_url:
                    continue

                query_params = parse_qs(urlparse(author_url).query)
                author_id = query_params.get('user', [None])[0]
                if author_id and author_id not in authors_to_fetch:
                    authors_to_fetch[author_id] = author.get('name')

        # 3. 并发获取作者信息
        authors_to_insert = []
        if authors_to_fetch:
            # 定义一个内部函数用于线程池调用
            def _fetch_author_info(author_id, author_name):
                get_author = GetAuthorInfoById(scholar_id=author_id)
                author_info, info = get_author.run()
                if author_info and author_info is not True:
                    self.log_print.print(f"成功获取作者信息: id={author_id}, name={author_name}")
                    return author_info
                elif author_info is True:
                    # self.log_print.print(f"作者信息已存在: id={author_id}, name={author_name}")
                    pass
                else:
                    self.log_print.print(f"获取作者信息失败: id={author_id}, name={author_name}, info={info}")
                return None

            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # 提交所有任务
                future_to_author = {executor.submit(_fetch_author_info, author_id, name): author_id for author_id, name
                                    in authors_to_fetch.items()}
                for future in concurrent.futures.as_completed(future_to_author):
                    try:
                        result = future.result()
                        if result:
                            authors_to_insert.append(result)
                    except Exception as exc:
                        author_id = future_to_author[future]
                        self.log_print.print(f"获取 author_id: {author_id} 的信息时产生异常: {exc}")

        # 4. 批量插入数据
        if articles_to_insert:
            # 假设您的 postgreSQL_handler 有一个 insert_many_data 方法
            # 注意：批量插入通常无法像单条插入一样方便地返回 "update" 或 "insert"。
            # 这里我们只执行插入操作，忽略冲突。您需要为表设置好唯一约束。
            try:
                article_rows = self.postgreSQL_handler.insert_data_list(
                    table_name=self.table_name_article.split(".")[-1],
                    data_list=articles_to_insert,
                    unique_col='article_url'  # 使用唯一键来避免重复
                )
                self.log_print.print(f"批量插入 {article_rows} 条文章数据成功。")
            except Exception as e:
                self.log_print.print(f"批量插入文章数据时出错: {e}")

        if authors_to_insert:
            try:
                author_rows = self.postgreSQL_handler.insert_data_list(
                    table_name=self.table_name_author.split(".")[-1],
                    data_list=authors_to_insert,
                    unique_col='scholar_id'  # 使用唯一键来避免重复
                )
                self.log_print.print(f"批量插入 {author_rows} 条作者数据成功。")
            except Exception as e:
                self.log_print.print(f"批量插入作者数据时出错: {e}")

        end_time = time.time()
        take_time = end_time - start_time
        self.log_print.print(f"标题与作者信息处理完毕 耗时{take_time:.4f}s  article_id: {article_id}, title: {title}")
        time.sleep(2)  # 保留延时，避免对目标网站造成过大压力

    def run(self):
        current = self.log_offset.get_int(default=0)
        if not current or current < self.min_id:
            current = self.min_id
        while current <= self.max_id:
            self.log_offset.record_int(current)
            data_list  = self.get_data_list_by_id(current, current + 20)
            if not data_list:
                self.log_print.print(f"未找到数据 id>={current} and id<={current + 20}  但是不会 break 继续下一个")
                current += 20
                continue
            for data in data_list:
                title = data['article_title']
                article_id = data['id']
                self.handle_one_title(title=title, article_id=article_id)
            current +=  20
            time.sleep(5)
        self.log_print.print(f"处理完成，当前ID: {current}, 最大ID: {self.max_id}")


    def run_thread(self, max_workers=10):
        """
        使用线程池并发处理任务。
        :param max_workers: 并发线程数，建议从 5 开始，根据网络和服务器情况调整。
        """
        current = self.log_offset.get_int(default=0)
        if not current or current < self.min_id:
            current = self.min_id

        self.log_print.print(f"开始运行，起始ID: {current}, 最大并发数: {max_workers}")

        # 使用 with 语句管理线程池
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            while current <= self.max_id:
                self.log_offset.record_int(current)

                # 每次获取一批数据
                batch_size = 20
                data_list = self.get_data_list_by_id(current, current + batch_size - 1)

                if not data_list:
                    self.log_print.print(f"在 ID 范围 [{current}, {current + batch_size - 1}] 未找到数据，继续...")
                    current += batch_size
                    # 即使没有数据，也稍微暂停一下，避免空轮询过快
                    time.sleep(5)
                    continue

                # 将批处理任务提交到线程池
                # future_to_data = {executor.submit(self.handle_one_title, data['article_title'], data['id']): data for data in data_list}
                # for future in concurrent.futures.as_completed(future_to_data):
                #     data = future_to_data[future]
                #     try:
                #         future.result()  # 获取任务结果，如果任务中出现异常，这里会抛出
                #     except Exception as exc:
                #         self.log_print.print(f"处理 article_id: {data['id']} 时产生异常: {exc}")
                for data in data_list:
                    title = data['article_title']
                    article_id = data['id']
                    # 将 handle_one_title 方法作为任务提交给线程池
                    executor.submit(self.handle_one_title_thread, title=title, article_id=article_id)

                self.log_print.print(
                    f"已提交 ID 范围 [{current}, {current + batch_size - 1}] 的 {len(data_list)} 个任务到线程池。")

                # 更新 current 到下一批次的起始ID
                current += batch_size

                # 批次间的延时可以保留，以控制整体请求速率
                time.sleep(5)

        self.log_print.print(f"所有任务已提交处理完成，最终ID: {current}, 最大ID: {self.max_id}")




