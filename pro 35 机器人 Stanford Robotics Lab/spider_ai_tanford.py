
import json
import os.path
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from myutil.cache import Cache
from myutil.handleDatetime import convert_date_robust
from myutil.handleMySQL import MySQLHandler
from myutil.handleRequest import SingleRequestHandler, AsyncRequestHandler
from myutil.handleSoup import extractSoup
from myutil.log_print import LogPrint
from myutil.maintainSourceInfo import MaintainSourceInfo
from myutil.uploadFile import FileUploader

class spider_ai_stanford:
    def __init__(self):
        """
        代理挂不了 不做高并发
        """
        self.db_name = "collection"
        self.table_name = "article_ai_stanford"
        self.site = "https://ai.stanford.edu/blog/index.html"  #
        self.source = "Stanford Robotics Lab"
        self.category = "机器人"
        self.hdfs_name = self.table_name
        self.delete_table_if_less = 20  # 删除表的条件，少于1000条数据就删除
        self.language = "en"
        self.log_print = LogPrint()
        self.log_page = Cache(f"log_page_{self.table_name}")
        # 日志
        self.mySQL_handler = MySQLHandler(db_name=self.db_name, table_name=self.table_name)
        test_url = self.site  # 报错
        # test_url = None
        self.single_handler = SingleRequestHandler(
            test_url=test_url,  # 测试链接，避免请求过多导致IP被封
        )
        self.async_handler = AsyncRequestHandler(
            max_workers=10,
            test_url=test_url,  # 测试链接，避免请求过多导致IP被封
        )
        self.headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "Accept-Language": "en,en-CN;q=0.9,zh-CN;q=0.8,zh;q=0.7",
            "Cache-Control": "max-age=0",
            "Connection": "keep-alive",
            "Referer": "https://www.cics-cert.org.cn/web_root/webpage/hotTopic.html",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "same-origin",
            "Sec-Fetch-User": "?1",
            "Upgrade-Insecure-Requests": "1",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36",
        }
        self.hdfs_handler = FileUploader(
            hdfs_name=self.hdfs_name,
            mode="async",
            max_workers=10,
            test_url=test_url,  # 测试链接，避免请求过多导致IP被封
            headers=self.headers
        )
        self.create_table()
        self.page_size =10

    def create_table(self):
        """
        创建Frontiers报告数据表
        """
        # 建表麻烦呀
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS `{self.table_name}`(
            `id` INT AUTO_INCREMENT COMMENT '主键ID',
            `article_title` VARCHAR(512) COMMENT '文章标题',
            `article_url` VARCHAR(512)  UNIQUE COMMENT '文章链接',
            `date_published` DATETIME  COMMENT '文章发布日期',
            `abstract` TEXT COMMENT '文章摘要',
            `content` TEXT COMMENT '文章内容',
            `author` JSON COMMENT '文章作者',
            `img_url`JSON COMMENT '文章图片链接，JSON格式',
            `tag` JSON COMMENT '文章标签，多个逗号分隔',
            `site` VARCHAR(128) COMMENT '网站名称',
            `source` VARCHAR(128) COMMENT '数据来源',
            `language` VARCHAR(16)  DEFAULT  'en' COMMENT '语言',
            `html` LONGTEXT COMMENT '文章HTML内容',
            `create_time` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '数据创建时间',
            `update_time` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '数据更新时间',
            PRIMARY KEY (`id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
        is_delete = self.mySQL_handler.drop_table(max_num=self.delete_table_if_less)
        if is_delete:
            self.log_page.clear_value()
        self.mySQL_handler.create_table(create_sql)
        source_info_data = {
            'source_name': self.source,
            'source_url': self.site,
            'category': self.category,
            'database_name': self.db_name,
            'mysql_table': self.table_name,
            'hdfs_path': self.hdfs_name,
            'is_mixed_data': 0,
            'where_condition': "无",
        }
        maintain_table = MaintainSourceInfo()
        maintain_table.insert_source_info(source_info_data, debug=True)
    def get_page(self, page_num):

        headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "Accept-Language": "en,en-CN;q=0.9,zh-CN;q=0.8,zh;q=0.7",
            "Cache-Control": "max-age=0",
            "Connection": "keep-alive",
            # "If-Modified-Since": "Wed, 27 Aug 2025 14:00:17 GMT",
            # "If-None-Match": "\"88d0a4e-5453-63d59355dfe40\"",
            "Referer": "https://ai.stanford.edu/blog/page/2/index.html",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "same-origin",
            "Sec-Fetch-User": "?1",
            "Upgrade-Insecure-Requests": "1",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36",
            "sec-ch-ua": "\"Google Chrome\";v=\"137\", \"Chromium\";v=\"137\", \"Not/A)Brand\";v=\"24\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"Windows\""
        }
        cookies = {
            # "_gid": "GA1.2.1800337709.1756802527",
            # "_gcl_au": "1.1.1487086775.1756802625",
            # "_ga_RBCPHN00S7": "GS2.1.s1756802624$o1$g0$t1756802624$j60$l0$h0",
            # "_ga_E08J4Q7DG7": "GS2.2.s1756802625$o1$g0$t1756802625$j60$l0$h0",
            # "_ga": "GA1.2.994387704.1756802527",
            # "_ga_MQ3TTX285Q": "GS2.2.s1756802527$o1$g1$t1756803121$j32$l0$h0"
        }
        if page_num == 1:
            page_url = "https://ai.stanford.edu/blog/index.html"
        else:
            page_url = f"https://ai.stanford.edu/blog/page/{page_num}/index.html"
        response = self.single_handler.fetch(page_url, headers=headers, cookies=cookies)
        return response

    def extract_page_articles(self, page_res,start_page):
        if not page_res or page_res.status_code != 200:
            self.log_print.print(f"page {start_page} 页面请求失败，状态码：{page_res.status_code if page_res else '无响应'}")
            return []
        soup = BeautifulSoup(page_res.text, 'html.parser')
        post_list = soup.select('div.posts  div.post-teaser')
        next_button = soup.select_one("div.paginate.pager a.button:last-child").text.strip()
        has_next = next_button == "Next"
        if not post_list:
            self.log_print.print(f"page {start_page} 页面数据为空")
            return []
        all_articles = []
        for post in post_list:
            article_title = extractSoup.extract_text(soup=post,selector="a.post-link > h2")
            href = extractSoup.extract_href(soup=post , selector="a.post-link")
            article_url = urljoin(self.site, href)
            author = extractSoup.extract_texts(selector="p.meta a" ,soup= post)
            abstract = extractSoup.extract_text(selector="div.excerpt div.excerpt-text" ,soup= post)
            all_articles.append({
                "article_title":article_title,
                "article_url": article_url,
                "author":author,
                "abstract": abstract,
                "site": self.site,
                "source": self.source,
                "language": self.language
            })
        return has_next, all_articles

    def parse_article_detail(self, articles):
        pic_url_list = []
        article_insert_list = []
        response_dict = self.async_handler.fetch_all(
            url_list=[article['article_url'] for article in articles],
            headers=self.headers
        )
        for article in articles:
            response_text =response_dict.get(article['article_url'])
            if not response_text:
                self.log_print.print(f"文章 {article['article_url']} 请求失败")
                continue
            soup = BeautifulSoup(response_text)
            content_div = soup.select_one('div.content')
            if not content_div:
                self.log_print.print(f"文章 {article['article_url']} 内容解析失败，未找到正文部分")
            # 提取图片链接
            date_published_str = extractSoup.extract_text(soup=content_div,selector="article header div.post-date")
            date_published = convert_date_robust(date_published_str)
            img_urls = extractSoup.extract_urls_relativeURL(soup=content_div,selector="img",relative_url=article['article_url'])
            article['img_url'] = img_urls
            article["tag"] = extractSoup.extract_texts(soup=content_div,selector="div.tag-list > a")
            article['date_published'] = date_published
            article['html'] = response_text
            article['content'] = extractSoup.extract_content(content_div)
            pic_url_list.extend(img_urls)
            article_insert_list.append(article)
        return pic_url_list, article_insert_list

    def run(self):
        start_page = self.log_page.get_int(default=1)
        while True:
            page_res = self.get_page(start_page)
            has_next, page_articles = self.extract_page_articles(page_res,start_page)
            if not page_articles:
                self.log_print.info(f"结束 当前page: {start_page} check url https://www.dlr.de/en/latest/news?page={start_page}")
                break
            pic_url_list, article_insert_list = self.parse_article_detail(page_articles)
            self.mySQL_handler.insert_data_list(article_insert_list)
            self.hdfs_handler.start_thread(pic_url_list)
            if len(page_articles) < self.page_size:
                self.log_print.info(
                    f"结束 当前page: {start_page} check url https://www.dlr.de/en/latest/news?page={start_page} 返回数据量 {len(page_articles) if page_articles else 0} 小于 {self.page_size}"
                )
                break
            if has_next:
                self.log_print.print(f"完成 page {start_page} 数据插入，准备处理下一页")
                start_page += 1
            else:
                break

if __name__ == "__main__":
    spider = spider_ai_stanford()
    spider.run()