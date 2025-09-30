import datetime
import json
import os.path
import urllib.parse
from urllib.parse import urljoin
import requests
from bs4 import BeautifulSoup
import re
import time
from myutil.cache import Cache
from myutil.handleDatetime import convert_date_robust
from myutil.handlePostgreSQL import  PostgreSQLHandler
from myutil.handleRequest import SingleRequestHandler, AsyncRequestHandler, ThreadRequestHandler,CurlRequestHandler
from myutil.handleSoup import extractSoup
from myutil.log_print import LogPrint
from myutil.maintainSourceInfo import MaintainSourceInfoPG
from pro2_google_scholar.get_author_by_title import GetAuthorByTitle

class spider_arxiv_org_ai_new:
    def __init__(self):
        """
        其实 含有 html 页面  ，但是呢都是根据作者的latex渲染的，结果不太一样，页面千奇百怪，邮件等需要信息提取困难，只访问摘要页 这个是规范的内容
        """
        self.db_name = "postgres"
        self.table_name = "article_arxiv_org"
        self.site = "https://arxiv.org/"  #
        self.source = "arxiv"
        self.category = "预印本"
        self.language = "en"
        self.log_print = LogPrint()
        self.delete_table_if_less = 20  # 删除表的条件，少于1000条数据就删除
        self.log_page = Cache(f"log_page_{self.table_name}_new")
        self.lod_date = Cache(f"log_date_{self.table_name}_new")
        # 日志
        self.postgreSQL_handler = PostgreSQLHandler(db_name=self.db_name, table_name=self.table_name)
        test_url = self.site  #
        # test_url = None
        self.headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-language": "en,en-CN;q=0.9,zh-CN;q=0.8,zh;q=0.7",
            "cache-control": "max-age=0",
            "priority": "u=0, i",
            "referer": "https://arxiv.org/list/cs.AI/new",
            "sec-ch-ua": "\"Chromium\";v=\"140\", \"Not=A?Brand\";v=\"24\", \"Google Chrome\";v=\"140\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"macOS\"",
            "sec-fetch-dest": "document",
            "sec-fetch-mode": "navigate",
            "sec-fetch-site": "same-origin",
            "sec-fetch-user": "?1",
            "upgrade-insecure-requests": "1",
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36"
        }
        self.cookies = {
            # "_ga": "GA1.1.1070728378.1758572327",
            # "_ga_B1RR0QKWGQ": "GS2.1.s1758572327$o1$g1$t1758572386$j1$l0$h0",
            # "arxiv-search-parameters": "\"{\\\"order\\\": \\\"-announced_date_first\\\"\\054 \\\"size\\\": \\\"50\\\"\\054 \\\"abstracts\\\": \\\"show\\\"\\054 \\\"date-date_type\\\": \\\"submitted_date\\\"}\"",
            # "arxiv_bibex": "{%22active%22:false%2C%22ds_cs%22:%22S2%22}",
            # "arxiv_labs": "{%22sameSite%22:%22strict%22%2C%22expires%22:365%2C%22last_tab%22:%22tabtwo%22%2C%22bibex-toggle%22:%22disabled%22%2C%22connectedpapers-toggle%22:%22disabled%22%2C%22alphaxiv-toggle%22:%22enabled%22%2C%22gotitpub-toggle%22:%22enabled%22}"
         }
        # self.single_handler = SingleRequestHandler(
        #     test_url=test_url,  # 测试链接，避免请求过多导致IP被封
        # )
        self.single_handler = CurlRequestHandler(
            test_url=test_url,  # 测试链接，避免请求过多导致IP被封
        )
        self.thread_handler = ThreadRequestHandler(
            max_workers = 3,
            test_url=test_url,  # 测试链接，避免请求过多导致IP被封
            headers=self.headers,
            cookies=self.cookies
        )
        self.create_table()
        self.page_size =50 # 每页50条数据

    def create_table(self):
        """
        创建Frontiers报告数据表
        """
        # 建表麻烦呀
        create_sql = f'''
          CREATE TABLE IF NOT EXISTS "spider"."{self.table_name}" (
              id SERIAL PRIMARY KEY,
              article_title VARCHAR(512),
              article_url VARCHAR(512) UNIQUE,
              article_doi VARCHAR(512),
              date_published TIMESTAMP,
              abstract TEXT,
              content TEXT DEFAULT '',
              author_list JSONB,
              category_list JSONB,
              file_info JSONB,
              site VARCHAR(128) DEFAULT '{self.site}',
              source VARCHAR(128) DEFAULT '{self.source}',
              language VARCHAR(16) DEFAULT '{self.language}',
              html TEXT,
              create_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
              update_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
          );

          CREATE OR REPLACE FUNCTION update_modified_column()
          RETURNS TRIGGER AS $$
          BEGIN
              NEW.update_time = CURRENT_TIMESTAMP;
              RETURN NEW;
          END;
          $$ language 'plpgsql';

          DROP TRIGGER IF EXISTS update_{self.table_name}_modtime ON "spider"."{self.table_name}";
          CREATE TRIGGER update_{self.table_name}_modtime
              BEFORE UPDATE ON "spider"."{self.table_name}"
              FOR EACH ROW
              EXECUTE FUNCTION update_modified_column();
          '''
        is_delete = self.postgreSQL_handler.drop_table(max_num=self.delete_table_if_less)
        if is_delete:
            self.log_page.clear_value()
        self.postgreSQL_handler.create_table(create_sql)
        source_info_data = {
            'source_name': self.source,
            'source_url': self.site,
            'category': self.category,
            'database_name': 'postgres',
            'mysql_table': self.table_name,
            'schema': 'spider'
        }
        maintain_table = MaintainSourceInfoPG()
        maintain_table.insert_source_info(source_info_data, debug=True)
        
        
    def get_page(self, page_num):
        page_url = "https://arxiv.org/list/cs.AI/new"
        # params = {
        #     "skip": "800",
        #     "show": "50"
        # }
        params = {
            "skip": f"{(page_num - 1)  * self.page_size}",
            "show": f"{self.page_size}"
        }
        response = self.single_handler.fetch(url=page_url, headers=self.headers, cookies=self.cookies,params=params)
        # headers = {
        #     "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        #     "accept-language": "en,en-CN;q=0.9,zh-CN;q=0.8,zh;q=0.7",
        #     "priority": "u=0, i",
        #     "referer": "https://arxiv.org/list/cs.AI/new?skip=0&show=50",
        #     "sec-ch-ua": "\"Chromium\";v=\"140\", \"Not=A?Brand\";v=\"24\", \"Google Chrome\";v=\"140\"",
        #     "sec-ch-ua-mobile": "?0",
        #     "sec-ch-ua-platform": "\"macOS\"",
        #     "sec-fetch-dest": "document",
        #     "sec-fetch-mode": "navigate",
        #     "sec-fetch-site": "same-origin",
        #     "sec-fetch-user": "?1",
        #     "upgrade-insecure-requests": "1",
        #     "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36"
        # }
        # cookies = {
        #     "_ga": "GA1.1.1070728378.1758572327",
        #     "_ga_B1RR0QKWGQ": "GS2.1.s1758572327$o1$g1$t1758572386$j1$l0$h0",
        #     "arxiv-search-parameters": "\"{\\\"order\\\": \\\"-announced_date_first\\\"\\054 \\\"size\\\": \\\"50\\\"\\054 \\\"abstracts\\\": \\\"show\\\"\\054 \\\"date-date_type\\\": \\\"submitted_date\\\"}\"",
        #     "arxiv_bibex": "{%22active%22:false%2C%22ds_cs%22:%22S2%22}",
        #     "arxiv_labs": "{%22sameSite%22:%22strict%22%2C%22expires%22:365%2C%22last_tab%22:%22tabtwo%22%2C%22bibex-toggle%22:%22disabled%22%2C%22connectedpapers-toggle%22:%22disabled%22%2C%22alphaxiv-toggle%22:%22enabled%22%2C%22gotitpub-toggle%22:%22enabled%22}"
        # }
        # url = "https://arxiv.org/list/cs.AI/new"
        # params = {
        #     "skip": f" {(page_num - 1)  * self.page_size}",
        #     "show": f"{self.page_size}"
        # }
        # response = requests.get(url, headers=headers, cookies=cookies, params=params)
        return response

    def extract_page_articles(self, page_res,start_page):
        all_articles = []
        if not page_res or page_res.status_code != 200:
            self.log_print.print(f"page {start_page} 页面请求失败，状态码：{page_res.status_code if page_res else '无响应'}")
            return False, all_articles
        soup = BeautifulSoup(page_res.text, 'html.parser')
        #  get article list only url is needed
        post_list = soup.select('#articles > dt')
        has_next = not soup.select_one("div.paging> span:last-child")
        page_info = soup.select_one("div.paging").get_text().strip()
        match = re.search(r'Total of\s+([\d,]+)\s+entries', page_info)
        if match:
            total_entries = int(match.group(1).replace(',', ''))
        else:
            total_entries = "未知"
        for article in post_list:
            a_list = article.select('a')
            text_url_dict ={}
            for a in a_list:
                if "href"  not in a.attrs:
                    continue
                url  =urllib.parse.urljoin(base=self.site , url=a.attrs['href'])
                text = a.get_text(strip=True)
                if text.startswith("arXiv"):
                    text_url_dict["abstract"] = url
                else:
                    text_url_dict[text] = url
            article_url  =  text_url_dict.get("abstract","") if "abstract" in text_url_dict else text_url_dict.get("html","")
            article_info ={
                "file_info": json.dumps(text_url_dict, ensure_ascii=False),
                "article_url": article_url,
            }
            all_articles.append(article_info)

        return has_next, all_articles ,total_entries

    def parse_article_detail(self, articles):
        response_dict = self.thread_handler.fetch_all(
            url_list=[article['article_url'] for article in articles],
        )
        for article in articles:
            response_text =response_dict.get(article['article_url'])
            if not response_text:
                self.log_print.print(f"文章 {article['article_url']} 请求失败")
                continue
            article_soup = BeautifulSoup(response_text)

            title_tag = article_soup.select_one("#abs h1.title")
            span_tag = title_tag.select_one("span.descriptor")
            if span_tag:
                span_tag.decompose()
            title_text = title_tag.text.strip()

            abstract_tag = article_soup.select_one("#abs blockquote.abstract")
            span_tag = abstract_tag.select_one("span.descriptor")
            if span_tag:
                span_tag.decompose()
            abstract_text = abstract_tag.text.strip()


            author_info_tags = article_soup.select("#abs div.authors a")
            author_list = {tag.attrs.get("href"): tag.get_text().strip() for tag in author_info_tags}

            subject_tag = article_soup.select_one("td.subjects")
            category_list = [cat.strip() for cat in subject_tag.text.split(";")] if subject_tag else []

            date_tag = article_soup.select_one("div.dateline")
            date_text = date_tag.get_text().strip()
            match = re.search(r'on\s+(.+?)\]', date_text)
            if match:
                date_str = match.group(1).strip()
                date_published = convert_date_robust(date_str)
            else:
                date_published = None

            doi_tag = article_soup.select_one("td.arxivdoi a")
            doi_url = doi_tag.attrs.get("href")

            article.update({
                "article_title": title_text,
                "abstract": abstract_text,
                "author_list": json.dumps(author_list, ensure_ascii=False),
                "category_list": json.dumps(category_list, ensure_ascii=False),
                "date_published": date_published,
                "article_doi": doi_url,
                "html": str(article_soup),
                "content": "",
            })


    def run(self):
        now_date = datetime.datetime.now().date()
        start_date = self.lod_date.get_string()
        if not start_date:
            self.log_print.print(f"没有日志记录，第一次运行，记录当前时间 {now_date.date()}")
            self.log_page.clear_value()
        else:
            start_date = datetime.datetime.strptime(start_date, "%Y-%m-%d").date()
            if now_date > start_date:
                self.log_print.print(f"新的一天 {now_date} ，清除进度，重新开始 当前时间 {now_date}   日志记录时间 {start_date}")
                self.log_page.clear_value()
            else:
                self.log_print.print(f"时间比较异常 报错 当前时间 {now_date}   日志记录时间 {start_date} 理论上日志时间应该偏小,至少要小一天")
                return

        start_page = self.log_page.get_int(default=1)
        while True:
            page_res = self.get_page(start_page)
            has_next, page_articles ,total_entries = self.extract_page_articles(page_res,start_page)
            if not page_articles:
                self.log_print.info(f"结束 当前page: {start_page} check url")
                break
            self.parse_article_detail(page_articles)
            self.postgreSQL_handler.insert_data_list(page_articles)
            if len(page_articles) < self.page_size:
                self.log_print.info(
                    f"结束 当前page: {start_page} check url   返回数据量 {len(page_articles) if page_articles else 0} 小于 {self.page_size}"
                )
                self.log_page.clear_value()
                self.lod_date.record_string(now_date.strftime("%Y-%m-%d"))
                break
            if has_next:
                self.log_print.print(f"完成 page {start_page} 数据插入，当前完成进度 {start_page * self.page_size} / {total_entries} 准备处理下一页")
                start_page += 1
                self.log_page.record_int(start_page)
                time.sleep(30)
            else:
                self.log_print.info(f"结束 当前page: {start_page} check url  没有下一页")
                self.log_page.clear_value()
                self.lod_date.record_string(now_date.strftime("%Y-%m-%d"))
                break

        table_name_read = self.table_name
        get_author = GetAuthorByTitle(table_name_read=table_name_read)
        get_author.run_thread()
if __name__ == "__main__":
    spider = spider_arxiv_org_ai_new()
    spider.run()