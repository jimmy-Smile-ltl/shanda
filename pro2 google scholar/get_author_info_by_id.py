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

class GetInfoById:
    def __init__(self, scholar_id):
        self.scholar_id = scholar_id
        self.site= "https://scholar.google.com/"
        self.table_name = "author_info_google_scholar"
        self.log_print = LogPrint()
        self.log_finished = Cache("log_finished_get_info_by_id")
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
        self.thread_handler = ThreadRequestHandler(
            max_workers = 3,
            test_url=test_url,  # 测试链接，避免请求过多导致IP被封
            headers=self.headers,
            cookies=self.cookies
        )

    def is_has_record(self):
        """检查是否已经处理过该ID"""
        return self.log_finished.is_member_of_set(self.scholar_id)

    def get_home_info(self, author_info):
        url = "https://scholar.google.com/citations"
        params = {
            "user":f"{self.scholar_id}",
            "hl": "zh-CN",
            "oi": "sra"
        }
        response = self.single_handler.fetch(url, headers=self.headers, cookies=self.cookies, params=params)
        if not response:
            self.log_print.print(f"ID {self.scholar_id} 首页请求失败，跳过。")
            return
        soup = BeautifulSoup(response.text, 'html.parser')
        #  姓名
        name = extractSoup.extract_text(soup =soup , selector="#gsc_prf_in")
        author_info['name'] = name
        # 头像
        avatar_url = extractSoup.extract_href(soup=soup, selector="#gsc_prf_pua img", base_url=url)
        author_info['avatar_url'] = avatar_url
        # 主页链接
        profile_url =  urllib.parse.urljoin(url, '?' + urllib.parse.urlencode(params))
        author_info['profile_url'] = profile_url

        # 情况统计 全部 与 2020年后 学术指标
        scholar_index = {}
        stats_tag_list = soup.select("#gsc_rsb_st > tbody > tr")
        for stats_tag in stats_tag_list:
            stat_name = stats_tag.select_one("td.gsc_rsb_sc1").get_text().replace(" ", "").strip()
            all_value = stats_tag.select("td.gsc_rsb_std")[0].get_text()
            after_2020_value = stats_tag.select("td.gsc_rsb_std")[1].get_text()
            scholar_index[stat_name] = {
                "all": int(all_value) if all_value and all_value.isdigit() else 0,
                "after_2020": int(after_2020_value) if after_2020_value and after_2020_value.isdigit() else 0
            }
        author_info['scholar_index'] = scholar_index


        # 单位 职称
        affiliation = soup.select_one("#gsc_prf_i > div.gsc_prf_il").get_text()
        author_info['affiliation'] = affiliation
        # 领域
        category_list = extractSoup.extract_text_urls(soup=soup, selector="#gsc_prf_int a", base_url=url)
        author_info['category'] = category_list

        # 每年的引用数量 年份与数量 一一对应
        year_tag_list = soup.select("div.gsc_md_hist_b >span.gsc_g_t")
        count_tag_list = soup.select("div.gsc_md_hist_b > a.gsc_g_a")
        # year_tag_list 与 count_tag_list 一一对应
        cite_per_year = {}
        for year_tag, count_tag in zip(year_tag_list, count_tag_list):
            year = year_tag.get_text()
            count = count_tag.get_text()
            if year and count and count.isdigit():
                cite_per_year[year] = int(count)
        author_info['cite_per_year'] = cite_per_year

        # 开放获取数量
        open_access_tag = soup.select_one("div.gsc_rsb_m > div.gsc_rsb_m_a")
        # 不开放获取
        non_open_access_tag = soup.select_one("div.gsc_rsb_m > div.gsc_rsb_m_na")
        open_access_num = open_access_tag.get_text().strip().split(" ")[0]
        non_open_access_num = non_open_access_tag.get_text().strip().split(" ")[0]
        author_info['open_access_num'] = int(open_access_num) if open_access_num and open_access_num.isdigit() else 0
        author_info['non_open_access_num'] = int(non_open_access_num) if non_open_access_num and non_open_access_num.isdigit() else 0


    # 获取合作者 一次全部 不分页
    def get_coauthors(self, author_info):
        url = "https://scholar.google.com/citations"
        params = {
            "view_op": "list_colleagues",
            "hl": "zh-CN",
            "json": "",
            "user": f"{self.scholar_id}",
        }
        response = self.single_handler.fetch(url, headers=self.headers, cookies=self.cookies, params=params)
        collaborator_nore_soup = BeautifulSoup(response.text, 'html.parser')
        collaborator_tag_nore_list =  collaborator_nore_soup.select("div.gsc_ucoar")
        collaborator_list = []
        for collaborator_tag in collaborator_tag_nore_list:
            id  = collaborator_tag.attrs.get("id")
            name_tag = collaborator_tag.select_one("h3.gs_ai_name > a")
            name = name_tag.get_text()
            profile_url = "https://scholar.google.com" + name_tag.attrs.get("href")
            affiliation = collaborator_tag.select_one("div.gs_ai_aff").get_text()
            collaborator_list.append({
                "id": id,
                "name": name,
                "profile_url": profile_url,
                "affiliation": affiliation
            })
        author_info['collaborator_list'] = collaborator_list

    def get_articles(self, author_info):
        page = 0
        page_size = 100
        article_list = []
        while True:
            url = "https://scholar.google.com/citations"
            params = {
                "user": f"{self.scholar_id}",
                "hl": "zh-CN",
                "oi": "sra",
                "cstart": f"{page * page_size}",
                "pagesize": f"{page_size}",
            }
            data = {
                "json": "1"
            }
            response = self.single_handler.fetch(url, headers=self.headers, cookies=self.cookies, params=params, data=data,method="POST")
            if not response:
                self.log_print.print(f"ID {self.scholar_id} 文章请求失败，跳过。")
                return
            res_json = response.json()
            html = res_json.get("B")  # P N 都是1 不清楚含义
            soup_article_more = BeautifulSoup(html, 'html.parser')
            article_more_tag_list = soup_article_more.select("tr.gsc_a_tr")
            for article_tag in article_more_tag_list:
                title_tag = article_tag.select_one("td.gsc_a_t a")
                title = title_tag.get_text()
                article_url =  urljoin(base= self.site , url= title_tag.attrs.get("href") )
                authors = article_tag.select_one("div.gs_gray").get_text()  # 不是url  有只有文字,然后有省略号 ... 就不处理了,反正有url了`
                publication_info = article_tag.select("div.gs_gray")[1].get_text()
                cited_tag = article_tag.select_one("td.gsc_a_c a")
                cited_num = cited_tag.get_text() if cited_tag else "0"
                year = article_tag.select_one("td.gsc_a_y span").get_text()
                article_list.append({
                    "article_title": title,
                    "article_url": article_url,
                    "authors": authors,
                    "publication_info": publication_info,
                    "cited_num": int(cited_num) if cited_num and cited_num.isdigit() else 0,
                    "year": int(year) if year and year.isdigit() else 0
                })
            if len(article_more_tag_list) < page_size: # 不足一页了  最后一页
                break
            if cited_num and cited_num.isdigit() and int(cited_num) < 5:  # 引用数小于5的就不继续翻页了
                break
            page += 1
            time.sleep(1)  # 避免请求过快被封IP

        author_info['article_list'] = article_list


    def run(self):
        if self.is_has_record():
            print(f"ID {self.scholar_id} 已处理，跳过。")
            return
        author_info ={
            "scholar_id": self.scholar_id,
        }
        # 首页的很多信息
        self.get_home_info(author_info)

        #  获取合作者
        self.get_coauthors(author_info)

        #  获取论文列表 翻页,
        self.get_articles(author_info)

        self.log_finished.add_to_set(self.scholar_id)
        print(f"ID {self.scholar_id} 处理完成。")
        return author_info


if __name__ == "__main__":
    test_id = "DTthB48AAAAJ"
    getter = GetInfoById(test_id)
    author_info = getter.run()
    file = f"author_info_google_scholar_{test_id}.json"
    with open(file, "w", encoding="utf-8") as f:
        json.dump(author_info, f, ensure_ascii=False, indent=4)
