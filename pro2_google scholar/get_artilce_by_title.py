import datetime
import json
import os.path
import sys
import urllib.parse
from urllib.parse import urljoin
import requests
from bs4 import BeautifulSoup
import re
import time

from executing import cache

from myutil.cache import Cache
from myutil.handleDatetime import convert_date_robust
from myutil.handlePostgreSQL import  PostgreSQLHandler
from myutil.handleRequest import SingleRequestHandler, AsyncRequestHandler, ThreadRequestHandler,CurlRequestHandler
from myutil.handleSoup import extractSoup
from myutil.log_print import LogPrint
from myutil.maintainSourceInfo import MaintainSourceInfoPG

class GetArticleByTitle:
    def __init__(self, title: str):
        self.title = title
        self.site= "https://scholar.google.com/"
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
        test_url =None
        self.single_handler = SingleRequestHandler(
            test_url=test_url,  # 测试链接，避免请求过多导致IP被封
        )
        self.log_print = LogPrint()
    def run(self):
        url = "https://scholar.google.com/scholar"
        params = {
            "hl": "zh-CN",
            "as_sdt": "0,5",
            "q": f"{self.title}",
            "btnG": ""
        }
        response = self.single_handler.fetch(url, headers=self.headers, cookies=self.cookies, params=params)
        if not response:
            self.log_print.print(f"请求失败: {url}")
            return None
        soup = BeautifulSoup(response.text, 'html.parser')
        result_list = soup.select("#gs_res_ccl_mid > div.gs_r.gs_or.gs_scl")
        if not result_list:
            self.log_print.print(f"未找到结果: {url} len(result_list)={len(result_list)}")
            return None
        article_list = []
        for result_item in result_list:
            article_info = {}
            title_tag = result_item.select_one("h3.gs_rt")
            title = title_tag.get_text()
            article_info["article_title"] = title
            article_url = title_tag.a['href'] if title_tag.a else None
            article_info["article_url"] = article_url
            article_list.append(article_info)
            cite_tag = result_item.select_one("div.gs_ri > div.gs_fl.gs_flb > a:nth-child(3)")
            if cite_tag and "被引用次数" in cite_tag.get_text():
                cited_by = int(re.search(r'被引用次数：(\d+)', cite_tag.get_text()).group(1))
                article_info["cited_num"] = cited_by
            else:
                article_info["cited_num"] = None
            article_info["html"] = str(result_item)
            self.extract_author_info(result_item, url, article_info)
        return article_list


    def extract_author_info(self, result_item, url , article_info):
        author_tag_p = result_item.select_one("div.gs_a.gs_fma_p")
        if not author_tag_p:
            author_tag = result_item.select_one("div.gs_a")
            publish_info = author_tag.get_text().replace("\xa0", " ").strip().split("-")[-1].strip()
            article_info["publish_info"] = publish_info
        else:
            author_tag = author_tag_p.select_one("div.gs_fmaa")
        author_dict = {}
        for link in author_tag.select("a"):
            name = link.get_text().strip()
            href = link.get("href")
            url = urljoin(url, href)
            author_dict[name] = url
        # 有些有url 有些没有url 所以要分开处理
        author_list = [item.strip() for item in author_tag.get_text().replace("\xa0", " ").split(",")]

        author_dict_list = []
        for idx, name in enumerate(author_list):
            author_item = {
                "name": name,
                "order": idx + 1,
                "url": author_dict.get(name, None)
            }
            author_dict_list.append(author_item)
        #
        article_info["author_dict_list"] = author_dict_list.copy()
        if "publish_info" in article_info:
            return
        author_tag.decompose()
        author_tag_p = result_item.select_one("div.gs_a.gs_fma_p")
        if not author_tag_p:
            author_tag_p = result_item.select_one("div.gs_a")
            if author_tag_p:
                publish_info = author_tag_p.get_text().replace("\xa0", " ").strip()
                article_info["publish_info"] = publish_info

if __name__ == '__main__':
    title = "Why and How Auxiliary Tasks Improve JEPA Representations"
    get_article = GetArticleByTitle(title)
    article_list = get_article.run()
    with open(f"article_list_{title.lower().replace(' ', '_')}.json", "w", encoding="utf-8") as f:
        json.dump(article_list, f, ensure_ascii=False, indent=4)
