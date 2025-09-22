# -*- coding: utf-8 -*-
# @Time    : 2025/7/16 08:53
# @Author  : Jimmy Smile
# @Project : 北大信研院
# @File    : handleCurl_cffiSession.py
# @Software: PyCharm
import asyncio
import os
import sys
import urllib
from typing import List, Dict, Optional
import concurrent.futures
import threading
import certifi
import curl_cffi
import execjs
from myutil.kdlProxy import ProxyUtil
import json
# asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
import time
from curl_cffi.requests.session import AsyncSession as curl_AsyncSession
from curl_cffi.requests.session import Session as curl_Session

class CurlSessionRequestHandler:
    def __init__(self, session: curl_cffi.requests.session, test_url):
        #
        # self.executeJS = execjs.compile(open("../pro21 科技文献 Cogent OA/main.js", mode="r", encoding="utf8").read())
        absolutePath = os.path.abspath(__file__)
        # 获取当前文件的绝对路径
        user = absolutePath.split(os.sep)[2]
        self.session = session
        if user == "JimmySmile" or user.find("immy") != -1:
            # print("当前电脑是JimmySmile, 证书位置使用默认位置")
            self.ca_bundle_path = certifi.where()  # 有中文路径，报错，是基于linux下面的一个包开发的，支持性欠缺
        elif user == "唐凯":
            # print("当前电脑是Jimmmy的工作台，证书位置位于 C:\cert\cacert.pem 这么做原因是 工作台含中文路径，curl_cffi不支持")
            self.ca_bundle_path = "C:\cert\cacert.pem"
        if test_url:
            self.proxyUtil = ProxyUtil(test_url=test_url)
            self.proxies = self.proxyUtil.get_proxy()
        else:
            self.proxyUtil = None
            self.proxies = None

    def fetch(self, session: curl_cffi.requests.Session = None, url: str = "", method='GET', retry_count=10,
              **kwargs) -> curl_cffi.requests.Response | None:
        if session:
            self.session = session
        if not url:
            raise ValueError("URL cannot be empty")
        for attempt in range(retry_count):
            try:
                if method == 'POST':
                    response = self.session.post(url, proxies=self.proxies,  verify=self.ca_bundle_path,**kwargs)
                else:
                    response = self.session.get(url, proxies=self.proxies, verify=self.ca_bundle_path, **kwargs)
                response.raise_for_status()
                response.encoding = "utf8"
                return response
            except Exception as e:
                if self.proxyUtil:
                    self.proxies = self.proxyUtil.get_proxy()
                time.sleep(1)
                if attempt == retry_count - 1:
                    return None
        return None

class CurlCffiAsyncSessionRequestHandler:
    """
    使用 curl_cffi.aio 实现的高性能、高伪装性的异步Web请求类。
    注意：IDE的静态分析器可能无法正确识别AsyncSession，这是一个已知的无害误报。
    """

    def __init__(self,  test_url: str | None = None, max_workers: int = 10,
                 method: str = 'GET', impersonate='chrome100', **kwargs):
        self.lock = asyncio.Lock()  # 创建一个 asyncio 锁
        self.proxies ={'http': 'http://t15142135039542:ykvgf8ax@d959.kdltps.com:15818',
                        'https': 'http://t15142135039542:ykvgf8ax@d959.kdltps.com:15818'
                      }
        absolutePath = os.path.abspath(__file__)
        # 获取当前文件的绝对路径
        user = absolutePath.split(os.sep)[2]
        if user == "JimmySmile" or user.find("immy") != -1:
            print("当前电脑是JimmySmile, 证书位置使用默认位置")
            self.ca_bundle_path = certifi.where()  # 有中文路径，报错，是基于linux下面的一个包开发的，支持性欠缺
        elif user == "唐凯":
            print(
                "当前电脑是Jimmmy的工作台，证书位置位于 C:\cert\cacert.pem 这么做原因是 工作台含中文路径，curl_cffi不支持")
            self.ca_bundle_path = "C:\cert\cacert.pem"
        self.executeJS = execjs.compile(open("../pro21 科技文献 Cogent OA/main.js", mode="r", encoding="utf8").read())
        self.flush_session()
        self.test_url = test_url
        self.max_workers = max_workers
        self.method = method.upper()
        # self.proxy_util = ProxyUtil(test_url) if test_url else None
        # self.proxies = self.proxy_util.get_proxy() if self.proxy_util else None
        self.impersonate = impersonate





    async def fetch_one(self, url: str,semaphore: asyncio.Semaphore, retry_count: int = 3, **kwargs) -> Dict[str, Optional[str | bool]]:
        # async with semaphore:
        for attempt in range(retry_count):
            try:
                if self.method == 'POST':
                    response = await self.curl_async_session.post(url, proxies=self.proxies,
                                                                  impersonate=self.impersonate,
                                                                  verify=self.ca_bundle_path,
                                                                  timeout=10,
                                                                    **kwargs)
                else:
                    response = await self.curl_async_session.get(url, proxies=self.proxies,
                                                                 impersonate=self.impersonate,
                                                                 verify=self.ca_bundle_path,
                                                                 timeout=10,
                                                                 **kwargs)
                response.raise_for_status()
                return {url: response.text, "status": True}
            except Exception as e:
                async with self.lock:
                    self.flush_session()
                # if self.proxy_util:
                #     self.proxies = self.proxy_util.get_proxy()
                if attempt == retry_count - 1:
                    return {url: None, "status": False}
        return {url: None, "status": False}

    async def _fetch_all(self, url_list: List[str], **kwargs) -> Dict[str, Optional[str]]:
        results = {}
        completed_count = 0
        failed_count = 0
        total_urls = len(url_list)
        semaphore = asyncio.Semaphore(self.max_workers)
        tasks = [self.fetch_one(url,semaphore, **kwargs) for url in url_list]
        for future in asyncio.as_completed(tasks):
            # 输出处理的进度
            result = await future
            results.update(result)
            if result.get("status"):
                completed_count += 1
            else:
                print(json.dumps(result))
                failed_count += 1
            print(f"\r进度: {completed_count}/{total_urls} 成功: {completed_count} 失败: {failed_count}", end="")
            sys.stdout.flush()
        print(f"\r所有Curl_cffi 异步请求任务处理完成。共:{total_urls} 成功: {completed_count} 失败: {failed_count}")
        return results

    def fetch_all(self, url_list: List[str] = [], **kwargs) -> Dict[
        str, Optional[str]]:
        """
        并发获取所有URL的内容，并使用 rich 库显示精美的进度条。
        注意：这是一个阻塞函数，适合在线程中运行。
        """
        self.flush_session()
        if not url_list:
            raise ValueError("URL list cannot be empty")

        results = asyncio.run(self._fetch_all(url_list, **kwargs))
        return results

    def flush_session(self):
        headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-language": "en,en-CN;q=0.9,zh-CN;q=0.8,zh;q=0.7",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36"
        }
        cookies = {
            "timezone": "480",
            "_gid": "GA1.2.133528753.1752460898",
            "MAID": "XiU97VTM1ME4MYyiVyj//w==",
            "_curator_id": "DE.V1.384442208d11.1752461016363",
            "optimizelyEndUserId": "oeu1752462427639r0.4834008325345577",
            "_hjSessionUser_864760": "eyJpZCI6ImU2ODlkNzcxLTc4ZTAtNTA1Yi1iODRjLTNjNzBhYzhiMDdmZSIsImNyZWF0ZWQiOjE3NTI0NjEwMTc0NjgsImV4aXN0aW5nIjp0cnVlfQ==",
            "displayMathJaxFormula": "true",
            "_gcl_au": "1.1.1250284419.1752462433",
            "hum_tandf_visitor": "d5a2291b-6b00-4875-a7a7-b8f7c76f56d2",
            "hum_tandf_synced": "true",
            "optimizelySession": "0",
            "_cm": "eyIxIjpmYWxzZSwiMiI6ZmFsc2UsIjMiOmZhbHNlfQ==",
            "MACHINE_LAST_SEEN": "2025-07-15T18%3A04%3A32.848-07%3A00",
            "JSESSIONID": "368E3FF6B84A793ED6D1F55597CC61A7",
            "OptanonAlertBoxClosed": "2025-07-16T01:39:25.556Z",
            "OptanonConsent": "isGpcEnabled=0&datestamp=Wed+Jul+16+2025+09%3A39%3A26+GMT%2B0800+(%E4%B8%AD%E5%9B%BD%E6%A0%87%E5%87%86%E6%97%B6%E9%97%B4)&version=202409.1.0&browserGpcFlag=0&isIABGlobal=false&hosts=&consentId=56be9072-385f-4e60-88b6-f4c1ca197892&interactionCount=1&isAnonUser=1&landingPath=NotLandingPage&groups=C0001%3A1%2CC0002%3A1%2CC0003%3A1%2CC0004%3A1&AwaitingReconsent=false&geolocation=%3B",
            "cf_clearance": "VSfNluKBYp2ilQ5bggjImEhf6BiTR2oSiEMPaYRLAR8-1752629970-1.2.1.1-QNZ5tpRU1WlbXWUSf1RJbEIXivlJAelPVszxKZlp1NsdbPY1SnvSQ_I2lymJmTH.OMYAwc0X3UmJfE6GqnbtioALoXyLjhALQ_ZhPXm6ajO3xII3Y32EM4iBx7Ixp732u4WtbfS9z2lLzp2JDDQSiijl1oqgc1w3KurnXB2eyaOhW3WFIO1yYnovpFqsvR6YSi7U4N80sH6VfCuL5RbtvlM3BUaX6NhJ.p4J31ym2dE",
            "_hjSession_864760": "eyJpZCI6ImVlNGU1MTE0LTAwZTktNGJjNC1iZDQyLTlkNmQwZmM5NjMwNCIsImMiOjE3NTI2Mjk5NjcxOTUsInMiOjAsInIiOjAsInNiIjowLCJzciI6MCwic2UiOjAsImZzIjowLCJzcCI6MH0=",
            "_ga_4819PJ6HEN": "GS2.1.s1752629967$o12$g0$t1752629967$j60$l0$h0",
            "_ga": "GA1.2.575950487.1752227647",
            "_gat_UA-3062505-46": "1",
            "_ga_0HYE8YG0M6": "GS2.1.s1752627891$o6$g1$t1752629969$j58$l0$h0"
        }
        self.curl_async_session = curl_AsyncSession(impersonate="chrome100",proxies=self.proxies)  # 使用curl_cffi的异步会话
        self.curl_async_session.headers.update(headers)
        self.curl_async_session.cookies.update(cookies)


class CurlCffiThreadedRequestHandler:
    """
    使用 curl_cffi 和 concurrent.futures.ThreadPoolExecutor 实现的高性能、高伪装性的多线程Web请求类。
    """

    def __init__(self, test_url: str | None = None, max_workers: int = 10,
                 method: str = 'GET', impersonate='chrome100', **kwargs):
        self.max_workers = max_workers
        self.method = method.upper()
        self.impersonate = impersonate
        self.kwargs = kwargs  # 存储额外的kwargs，如timeout
        self.proxies ={'http': 'http://t15142135039542:ykvgf8ax@d959.kdltps.com:15818',
                        'https': 'http://t15142135039542:ykvgf8ax@d959.kdltps.com:15818'
                      }
        absolutePath = os.path.abspath(__file__)
        user = absolutePath.split(os.sep)[2]
        if user == "JimmySmile" or user.find("immy") != -1:
            print("当前电脑是JimmySmile, 证书位置使用默认位置")
            self.ca_bundle_path = certifi.where()
        elif user == "唐凯":
            print(
                "当前电脑是Jimmmy的工作台，证书位置位于 C:\cert\cacert.pem 这么做原因是 工作台含中文路径，curl_cffi不支持")
            self.ca_bundle_path = "C:\cert\cacert.pem"



        # 对于多线程，每个线程需要一个独立的Session，或者Session是线程安全的。
        # curl_cffi.requests.Session 是线程安全的。但为了更好的隔离，可以在每个线程内部创建
        # 或者在初始化时创建，但在多线程场景下，如果涉及到cf_clearance刷新等，需要特别注意
        # 这里的策略是：主线程维护一个session用于flush_cf_clearance，worker线程直接用一个共享的session，
        # 因为curl_cffi的Session在内部做了线程安全处理。
        self.main_session = curl_Session(impersonate=self.impersonate,proxies=self.proxies)
        self.flush_session()
        # 线程安全的计数器和锁
        self.completed_count = 0
        self.failed_count = 0
        self.results_lock = threading.Lock()
        self.progress_lock = threading.Lock()  # 用于打印进度的锁



    def flush_session(self):

        for retry in range(8):
            headers = {
                "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
                "accept-language": "en,en-CN;q=0.9,zh-CN;q=0.8,zh;q=0.7",
                "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36"
            }
            cookies = {
                "timezone": "480",
                "_gid": "GA1.2.133528753.1752460898",
                "MAID": "XiU97VTM1ME4MYyiVyj//w==",
                "_curator_id": "DE.V1.384442208d11.1752461016363",
                "optimizelyEndUserId": "oeu1752462427639r0.4834008325345577",
                "_hjSessionUser_864760": "eyJpZCI6ImU2ODlkNzcxLTc4ZTAtNTA1Yi1iODRjLTNjNzBhYzhiMDdmZSIsImNyZWF0ZWQiOjE3NTI0NjEwMTc0NjgsImV4aXN0aW5nIjp0cnVlfQ==",
                "displayMathJaxFormula": "true",
                "_gcl_au": "1.1.1250284419.1752462433",
                "hum_tandf_visitor": "d5a2291b-6b00-4875-a7a7-b8f7c76f56d2",
                "hum_tandf_synced": "true",
                "optimizelySession": "0",
                "_cm": "eyIxIjpmYWxzZSwiMiI6ZmFsc2UsIjMiOmZhbHNlfQ==",
                "MACHINE_LAST_SEEN": "2025-07-15T18%3A04%3A32.848-07%3A00",
                "JSESSIONID": "368E3FF6B84A793ED6D1F55597CC61A7",
                "OptanonAlertBoxClosed": "2025-07-16T01:39:25.556Z",
                "OptanonConsent": "isGpcEnabled=0&datestamp=Wed+Jul+16+2025+09%3A39%3A26+GMT%2B0800+(%E4%B8%AD%E5%9B%BD%E6%A0%87%E5%87%86%E6%97%B6%E9%97%B4)&version=202409.1.0&browserGpcFlag=0&isIABGlobal=false&hosts=&consentId=56be9072-385f-4e60-88b6-f4c1ca197892&interactionCount=1&isAnonUser=1&landingPath=NotLandingPage&groups=C0001%3A1%2CC0002%3A1%2CC0003%3A1%2CC0004%3A1&AwaitingReconsent=false&geolocation=%3B",
                "cf_clearance": "VSfNluKBYp2ilQ5bggjImEhf6BiTR2oSiEMPaYRLAR8-1752629970-1.2.1.1-QNZ5tpRU1WlbXWUSf1RJbEIXivlJAelPVszxKZlp1NsdbPY1SnvSQ_I2lymJmTH.OMYAwc0X3UmJfE6GqnbtioALoXyLjhALQ_ZhPXm6ajO3xII3Y32EM4iBx7Ixp732u4WtbfS9z2lLzp2JDDQSiijl1oqgc1w3KurnXB2eyaOhW3WFIO1yYnovpFqsvR6YSi7U4N80sH6VfCuL5RbtvlM3BUaX6NhJ.p4J31ym2dE",
                "_hjSession_864760": "eyJpZCI6ImVlNGU1MTE0LTAwZTktNGJjNC1iZDQyLTlkNmQwZmM5NjMwNCIsImMiOjE3NTI2Mjk5NjcxOTUsInMiOjAsInIiOjAsInNiIjowLCJzciI6MCwic2UiOjAsImZzIjowLCJzcCI6MH0=",
                "_ga_4819PJ6HEN": "GS2.1.s1752629967$o12$g0$t1752629967$j60$l0$h0",
                "_ga": "GA1.2.575950487.1752227647",
                "_gat_UA-3062505-46": "1",
                "_ga_0HYE8YG0M6": "GS2.1.s1752627891$o6$g1$t1752629969$j58$l0$h0"
            }

            self.main_session = curl_Session(impersonate="chrome100",proxies=self.proxies)
            self.main_session.headers.update(headers)
            self.main_session.cookies.update(cookies)
            article_test_url ="https://www.tandfonline.com/doi/full/10.2147/IDR.S488933"
            try:
                response = self.main_session.get(article_test_url, impersonate="chrome100", verify=self.ca_bundle_path,proxies=self.proxies)
            except Exception as e:
                continue
            if response.status_code == 200:
                break
            else:
                time.sleep(1)
        else:
            print("curl_session 刷新失败，请检查网络连接或代理设置。")


    def _fetch_one_threaded(self, url: str, retry_count: int = 12) -> Dict[str, Optional[str | bool]]:
        """
        多线程环境下获取单个URL的内容。
        每个线程都使用共享的 main_session，因为 curl_cffi.requests.Session 是线程安全的。
        """
        # 为每个线程或请求创建一个临时的 RequestHandler 实例，它将使用共享的 main_session
        # 这种方式可以确保 fetch 方法内部的状态（如代理）在线程之间是隔离的，
        # 但 session 本身仍然是共享的。
        for attempt in range(retry_count):
            try:
                response =self.main_session.get(url, impersonate="chrome100", verify=self.ca_bundle_path,proxies=self.proxies)
                response.raise_for_status()
                if response.status_code == 200:
                    result = {url: response.text, "status": True}
                    return result
                else:
                    with self.progress_lock:
                        time.sleep(1)
                    continue
            except Exception as e:
                with self.progress_lock: # 等待更换代理
                    time.sleep(1)
                if attempt == retry_count - 1:
                    return {url: None, "status": False}
        else:
            return {url: None, "status": False}


    def fetch_all(self, url_list: List[str] = [], **kwargs) -> Dict[str, Optional[str]]:
        """
        并发获取所有URL的内容，使用多线程池。
        """

        if not url_list:
            raise ValueError("URL list cannot be empty")
        self.flush_session()
        self.total_urls_for_progress = len(url_list)  # 存储总数用于进度条
        self.completed_count = 0  # 重置计数器
        self.failed_count = 0  # 重置计数器
        all_results = {}

        # 允许在外部覆盖 kwargs
        current_kwargs = self.kwargs.copy()
        current_kwargs.update(kwargs)

        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # 提交任务到线程池
            future_to_url = {executor.submit(self._fetch_one_threaded, url, **current_kwargs): url for url in url_list}

            for future in concurrent.futures.as_completed(future_to_url):
                url = future_to_url[future]
                try:
                    result = future.result()
                    if result.get("status"):
                        self.completed_count += 1
                    else:
                        self.failed_count += 1
                    all_results.update(result)
                except Exception as exc:
                    # 这通常不应该发生，因为 _fetch_one_threaded 内部已经处理了异常并返回了结果字典
                    # 但为了健壮性，仍然捕获一下
                    with self.progress_lock:
                        self.failed_count += 1
                        sys.stdout.write(f"\n线程任务 {url} 生成异常: {exc}")
                        sys.stdout.flush()
                    all_results.update({url: None, "status": False})

        print(
            f"\r所有Curl_cffi 多线程请求任务处理完成。共:{self.total_urls_for_progress} 成功: {self.completed_count} 失败: {self.failed_count}")
        return all_results

if __name__ == "__main__":
    article_test_url = None
    async_handler = CurlCffiAsyncSessionRequestHandler(
        test_url=article_test_url,
    )
    url_list =['https://www.tandfonline.com/doi/full/10.1080/0886022X.2025.2466114', 'https://www.tandfonline.com/doi/full/10.1080/0886022X.2025.2466116', 'https://www.tandfonline.com/doi/full/10.1080/0886022X.2025.2466818', 'https://www.tandfonline.com/doi/full/10.1080/0886022X.2025.2466820', 'https://www.tandfonline.com/doi/full/10.1080/0886022X.2025.2466822', 'https://www.tandfonline.com/doi/full/10.1080/0886022X.2025.2466823', 'https://www.tandfonline.com/doi/full/10.1080/0886022X.2025.2466824', 'https://www.tandfonline.com/doi/full/10.1080/0886022X.2025.2468741', 'https://www.tandfonline.com/doi/full/10.1080/0886022X.2025.2469746', 'https://www.tandfonline.com/doi/full/10.1080/0886022X.2025.2469748', 'https://www.tandfonline.com/doi/full/10.1080/0886022X.2025.2470478', 'https://www.tandfonline.com/doi/full/10.1080/0886022X.2025.2471008', 'https://www.tandfonline.com/doi/full/10.1080/0886022X.2025.2471011', 'https://www.tandfonline.com/doi/full/10.1080/0886022X.2025.2471016', 'https://www.tandfonline.com/doi/full/10.1080/0886022X.2025.2471018', 'https://www.tandfonline.com/doi/full/10.1080/0886022X.2025.2471568', 'https://www.tandfonline.com/doi/full/10.1080/0886022X.2025.2472037', 'https://www.tandfonline.com/doi/full/10.1080/0886022X.2025.2472039', 'https://www.tandfonline.com/doi/full/10.1080/0886022X.2025.2472041', 'https://www.tandfonline.com/doi/full/10.1080/0886022X.2025.2472981', 'https://www.tandfonline.com/doi/full/10.1080/0886022X.2025.2472987', 'https://www.tandfonline.com/doi/full/10.1080/0886022X.2025.2472990', 'https://www.tandfonline.com/doi/full/10.1080/0886022X.2025.2473668', 'https://www.tandfonline.com/doi/full/10.1080/0886022X.2025.2473669', 'https://www.tandfonline.com/doi/full/10.1080/0886022X.2025.2474203', 'https://www.tandfonline.com/doi/full/10.1080/0886022X.2025.2474204', 'https://www.tandfonline.com/doi/full/10.1080/0886022X.2025.2474205', 'https://www.tandfonline.com/doi/full/10.1080/0886022X.2025.2474206', 'https://www.tandfonline.com/doi/full/10.1080/0886022X.2025.2474743', 'https://www.tandfonline.com/doi/full/10.1080/0886022X.2025.2474854', 'https://www.tandfonline.com/doi/full/10.1080/0886022X.2025.2476051', 'https://www.tandfonline.com/doi/full/10.1080/0886022X.2025.2476052', 'https://www.tandfonline.com/doi/full/10.1080/0886022X.2025.2476053', 'https://www.tandfonline.com/doi/full/10.1080/0886022X.2025.2476222', 'https://www.tandfonline.com/doi/full/10.1080/0886022X.2025.2476736', 'https://www.tandfonline.com/doi/full/10.1080/0886022X.2025.2476740', 'https://www.tandfonline.com/doi/full/10.1080/0886022X.2025.2477302', 'https://www.tandfonline.com/doi/full/10.1080/0886022X.2025.2477318', 'https://www.tandfonline.com/doi/full/10.1080/0886022X.2025.2477832', 'https://www.tandfonline.com/doi/full/10.1080/0886022X.2025.2477833', 'https://www.tandfonline.com/doi/full/10.1080/0886022X.2025.2477834', 'https://www.tandfonline.com/doi/full/10.1080/0886022X.2025.2478320', 'https://www.tandfonline.com/doi/full/10.1080/0886022X.2025.2478482', 'https://www.tandfonline.com/doi/full/10.1080/0886022X.2025.2478483', 'https://www.tandfonline.com/doi/full/10.1080/0886022X.2025.2478486', 'https://www.tandfonline.com/doi/full/10.1080/0886022X.2025.2478487', 'https://www.tandfonline.com/doi/full/10.1080/0886022X.2025.2478488', 'https://www.tandfonline.com/doi/full/10.1080/0886022X.2025.2479177', 'https://www.tandfonline.com/doi/full/10.1080/0886022X.2025.2479184', 'https://www.tandfonline.com/doi/full/10.1080/0886022X.2025.2479572']
    async_handler.fetch_all(url_list)