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


from myutil.cache import Cache
from myutil.handleDatetime import convert_date_robust
from myutil.handlePostgreSQL import  PostgreSQLHandler
from myutil.handleRequest import SingleRequestHandler, AsyncRequestHandler, ThreadRequestHandler,CurlRequestHandler
from myutil.handleSoup import extractSoup
from myutil.log_print import LogPrint
from myutil.maintainSourceInfo import MaintainSourceInfoPG

class GetArtilceTopPublications5Year_AI:
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
        self.log_finished = Cache("get_artilce_TopPublications5Year_AI_finished")
        self.log_current = Cache("get_artilce_TopPublications5Year_AI_current")
        self.log_start = Cache("get_artilce_TopPublications5Year_AI_start")
        self.db_name = "postgres"
        self.table_name = "artilce_TopPublications5Year_AI"
        self.postergreSQL_handler = PostgreSQLHandler(db_name=self.db_name, table_name=self.table_name,
                                                    return_type="dict")
        self.create_table()


    def load_publication_list(self):
        file_name  = "get_artilce_TopPublications5Year_AI.json"
        if os.path.exists(file_name):
            with open(file_name, "r", encoding="utf-8") as f:
                publication_list = json.load(f)
            return publication_list
        base_url = "https://scholar.google.es/citations?view_op=top_venues&hl=en&vq=eng_artificialintelligence"
        data_list = []
        url = "https://scholar.google.es/citations"
        params = {
            "view_op": "top_venues",
            "hl": "en",
            "vq": "eng_artificialintelligence"
        }
        response = self.single_handler.fetch(url, headers=self.headers, cookies=self.cookies, params=params)
        if not response:
            self.log_print.error(f"出版信息获取失败 请求失败: {url}")
            return []
        soup = BeautifulSoup(response.text, 'html.parser')
        trs = soup.select("#gsc_mvt_table  tr")
        for tr in trs:
            tds = tr.select("td")
            if len(tds) == 0:
                continue
            if len(tds) != 4:
                print("tds length != 4", tds)
                continue
            rank = tds[0].get_text()
            title = tds[1].get_text()
            h5_index = tds[2].get_text()
            h5_index_url = ""
            if tds[2].a:
                h5_index_url = urljoin(base=base_url, url=tds[2].a['href'])
            h5_median = tds[3].get_text()
            data_list.append({
                "rank": rank.split(".")[0] if "." in rank else rank,
                "title": title,
                "h5_index": h5_index,
                "h5_median": h5_median,
                "h5_index_url": h5_index_url,
            })
        with open(file_name, "w", encoding="utf-8") as f:
            json.dump(data_list, f, ensure_ascii=False, indent=4)
        return data_list

    def create_table(self):
        table_name = self.table_name
        sql = f"""
        CREATE TABLE IF NOT EXISTS spider.{table_name} (
            id BIGSERIAL PRIMARY KEY,
            publication_title VARCHAR(255) NOT NULL,
            title VARCHAR(512) NOT NULL,
            article_url VARCHAR(512),
            authors VARCHAR(255),
            publication_venue VARCHAR(255),
            cited_num INTEGER,
            cited_url VARCHAR(512),
            year VARCHAR(10),
            create_time TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
            update_time TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
        );
        -- 可选：触发器自动更新时间
        CREATE OR REPLACE FUNCTION update_timestamp()
        RETURNS TRIGGER AS $$
        BEGIN
            NEW.update_time = CURRENT_TIMESTAMP;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        
        CREATE TRIGGER update_article_publication_time
        BEFORE UPDATE ON spider.{table_name}
        FOR EACH ROW EXECUTE FUNCTION update_timestamp();
        
        """


    def handle_one_publication(self, publication,start = 0):
        self.log_print.info(f"开始处理 {publication['title']}")
        self.log_current.record_string(publication['title'])
        url = publication["h5_index_url"]
        if not url:
            self.log_print.error(f"出版信息获取失败 没有h5_index_url: {publication}")
            return
        while True:
            response = self.single_handler.fetch(url, headers=self.headers, cookies=self.cookies)
            if not response:
                self.log_print.error(f"出版信息获取失败 请求失败: {url}")
                return
            result_list = self.handle_one_page(response, publication)
            if not result_list:
                self.log_print.error(f"出版信息获取失败 解析失败: {url}")
                break
            if len(result_list) <= 20:
                self.log_print.error(f"数量不对 len(result_list) <= 20: {url} len(result_list)={len(result_list)}")
                break
            self.postergreSQL_handler.insert_data_list(result_list)
            start += 20
            self.log_start.record_int(start)

    def handle_one_page(self, response, publication):
        url = publication["h5_index_url"]
        data_list = []
        if  not response :
            self.log_print.error(f"请求失败 , url= {url} ")
            return []
        article_soup = BeautifulSoup(response.text, 'html.parser')
        trs = article_soup.select("#gsc_mpat_table tr")

        for tr in trs:
            tds = tr.select("td")
            if len(tds) == 0:
                continue
            if len(tds) != 3:
                print("tds length != 3", tds)
                continue
            title_tag = tr.select_one("td.gsc_mpat_t > div.gsc_mpat_ttl > a")
            article_title = title_tag.get_text().strip()
            article_url = urljoin(base=url, url=title_tag.get("href", "").strip()) if title_tag else ""
            author_tag = tr.select_one("td.gsc_mpat_t > div.gs_gray:nth-child(2)")
            authors = author_tag.get_text().strip() if author_tag else ""
            publication_tag = tr.select_one("td.gsc_mpat_t > div.gs_gray:nth-child(3)")
            publication_venue = publication_tag.get_text().strip() if publication_tag else ""
            cite_by_tag = tr.select_one("td.gsc_mpat_c a")
            cite_num = cite_by_tag.get_text().strip() if cite_by_tag else ""
            city_url = urljoin(base=url, url=cite_by_tag.get("href", "").strip()) if cite_by_tag else ""
            year_tag = tr.select_one("td.gsc_mpat_y span")
            year = year_tag.get_text().strip() if year_tag else "",

            data_dict = {
                "title": article_title,
                "article_url": article_url,
                "authors": authors,
                "publication_venue": publication_venue,
                "cited_num": cite_num,
                "cited_url": city_url,
                "year": year
            }
            data_dict.update(publication)
            data_list.append(data_dict.copy())
        return data_list


    def run(self):
        publication_list = self.load_publication_list()
        current_title = self.log_current.get_string()
        current_start = self.log_start.get_int()
        for publication in publication_list:
            if publication["title"] in self.log_finished:
                self.log_print.info(f"已完成 {publication['title']}")
                continue
            if current_title and publication["title"] != current_title:
                self.handle_one_publication(publication,start = 0)
            else:
                self.handle_one_publication(publication,start = current_start)
            self.log_finished.append_to_list(publication["title"])


if __name__ == '__main__':
    title = "Why and How Auxiliary Tasks Improve JEPA Representations"
    get_article = GetArtilceTopPublications5Year_AI(title)
    article_list = get_article.run()
    with open(f"article_list_{title.lower().replace(' ', '_')}.json", "w", encoding="utf-8") as f:
        json.dump(article_list, f, ensure_ascii=False, indent=4)
