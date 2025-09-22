﻿# -*- coding: utf-8 -*-
# @Time    : 2025/6/27 14:00
# @Author  : Jimmy Smile
# @Project : 北大信研院
# @File    : uploader.py
# @Software: PyCharm
import asyncio
import concurrent.futures
import mimetypes
import re
import sys
import threading
import time
from io import BytesIO
from pathlib import Path
from typing import List, Dict, Optional, Union
from urllib.parse import unquote, urlparse

import aiohttp
import requests
from pathvalidate import sanitize_filename

from myutil.handleRequest import SingleRequestHandler
from myutil.kdlProxy import ProxyUtil


class FileUploader:
    """
    一个通用的文件上传类，支持多线程和异步模式。
    可以从URL列表下载指定类型的文件（包括文档和图片），并上传到HDFS。
    """

    def __init__(self,
                 hdfs_name: str,
                 mode: str = 'thread',
                 max_workers: int = 10,
                 method: str = 'GET',
                 strict_mime_check: bool = True,
                 test_url: Optional[str] = None,
                 headers: Optional[Dict] = None,
                 **kwargs):
        """
        初始化文件上传器。

        :param hdfs_name: HDFS上的目标目录名。 可以是加/ 的 比如 /daily/ 或者 daily/RMRB
        :param mode: 工作模式, 'thread' (多线程) 或 'async' (异步)。
        :param max_workers: 最大并发工作线程数。
        :param allowed_extensions: 允许的文件扩展名列表。如果为None, 默认支持多种文档和图片格式。
        :param strict_mime_check: 是否进行严格的MIME类型检查。默认为True。
        :param test_url: 用于测试的URL (传递给SingleRequestHandler)。
        :param headers: 自定义请求头。
        :param kwargs: 其他传递给 aiohttp 或 requests 的参数。
        """
        if hdfs_name.endswith('/'):
            # 如果以 / 结尾，则去掉最后的 /
            hdfs_name = hdfs_name[:-1]
        self.hdfs_name = hdfs_name.split('/')[-1]
        self.hdfs_addr = 'http://10.0.102.75:9049/api/big_data/HdfsClient/uploadFile'
        if hdfs_name.startswith('/'):
            self.hdfs_path = hdfs_name
        else:
            # 如果没有以 / 开头，则添加一个 /
            self.hdfs_path = f'/{hdfs_name}'
        if mode not in ['thread', 'async']:
            raise ValueError("mode 必须是 'thread' 或 'async'")
        self.mode = mode
        self.kwargs = kwargs
        self.max_workers = max_workers
        self.strict_mime_check = strict_mime_check
        self.handler = SingleRequestHandler(
            test_url=test_url,
        )
        self.method = method.upper()
        if test_url:
            self.proxy_util = ProxyUtil(test_url=test_url)
            self.proxies = self.proxy_util.get_proxy()
        else:
            self.proxy_util = None
            self.proxies = None



        # 定义允许的文件类型和对应的MIME类型
        self.allowed_extensions: List[str] = [
            # 文档
            '.pdf', '.docx', '.doc', '.ppt', '.pptx', '.xls', '.xlsx', '.txt', '.csv', '.rtf', '.epub',
            # 图片
            '.jpg', '.jpeg', '.png', '.gif', '.bmp', '.svg', '.webp', '.tiff',
            # 压缩包
            '.zip', '.rar', '.7z', '.tar', '.gz',
            # 音视频
            '.mp3', '.wav', '.mp4', '.mov', '.avi', '.mkv',
        ]

        self.mime_map = {
            # 文档
            '.pdf': 'application/pdf',
            '.docx': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
            '.doc': 'application/msword',
            '.txt': 'text/plain',
            # 图片
            '.png': 'image/png',
            '.jpg': 'image/jpeg',
            '.jpeg': 'image/jpeg',
            '.gif': 'image/gif',
            '.bmp': 'image/bmp',
            '.webp': 'image/webp',
        }

        self.headers = headers if headers else {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }


    def start(self, file_urls: List[str], show_progress: bool = True):
        """
        根据设定的模式启动上传任务。
        :param file_urls: 包含文件URL的列表。
        :param show_progress: 是否显示进度条。
        """
        if self.mode == 'thread':
            self._start_thread(file_urls, show_progress=show_progress)
        elif self.mode == 'async':
            asyncio.run(self._start_async(file_urls, show_progress=show_progress))
        else:
            print("不支持的模式")

    def start_async(self, file_urls: List[str] | List[dict], show_progress: bool = True):
        """
        启动异步上传任务。
        :param file_urls: 包含文件URL的列表。
        :param show_progress: 是否显示进度条。
        """
        if not file_urls:
            print("没有需要处理的文件链接。")
            return {}
        if isinstance(file_urls[0], str):
            return asyncio.run(self._start_async(file_urls, show_progress=show_progress))
        elif isinstance(file_urls[0], dict):
            upload_results = asyncio.run(self._start_async(file_urls, show_progress=show_progress))
            article_list_insert = file_urls.copy()
            for i, result in enumerate(upload_results):
                pdf_url = result['url']
                if not result['result']:  # 上传失败 hdfs_path 为空, 使用默认值
                    continue
                for article_data in article_list_insert:
                    if article_data['pdf_url'] == pdf_url and result['result']:
                        article_data['hdfs_path'] = result.get('hdfs_path', None)
                        break
            return article_list_insert
        return None

    def start_thread(self, file_urls: List[str] | List[dict], show_progress: bool = True):
        if not file_urls:
            print("没有需要处理的文件链接。")
            return {}
        if isinstance(file_urls[0], str):
            # 如果是字符串列表，直接调用线程处理
            return self._start_thread(file_urls, show_progress=show_progress)
        elif isinstance(file_urls[0], dict):
            if "pdf_url" not in file_urls[0]:
                raise ValueError("pdf_url 必须含有")
            upload_results = self._start_thread(file_urls, show_progress=show_progress)
            article_list_insert = file_urls.copy()
            for i, result in enumerate(upload_results):
                pdf_url = result['url']
                if not result['result']:  # 上传失败 hdfs_path 为空, 使用默认值
                    continue
                for article_data in article_list_insert:
                    if isinstance(article_data, dict) and article_data['pdf_url'] == pdf_url and result['result']:
                        article_data['hdfs_path'] = result.get('hdfs_path', None)
                        # print(f"hdfs_path: {article_data['hdfs_path']}")
                        break
            return article_list_insert
        return None

    def _start_thread(self, file_urls: List[str] | List[dict], show_progress: bool = True):
        """
        使用线程池并发处理所有文件链接。
        """
        self._total_files = len(file_urls)
        self._completed_count = 0
        self._error_count = 0  # 新增错误计数
        self._progress_lock =threading.Lock()

        if self._total_files == 0:
            if show_progress:
                print("没有需要处理的文件链接。")
            return

        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            if isinstance(file_urls[0], str):
                future_to_url = {executor.submit(self._handle_one_file, pdf_url, hdfs_path=None): pdf_url for pdf_url in
                                 file_urls}
            if isinstance(file_urls[0], dict):
                article_list_insert = file_urls.copy()
                if not "pdf_url" in file_urls[0]:
                    raise ValueError("字典列表中的每个字典必须包含 'pdf_url' 键。")
                # 如果是字典列表，提取URL
                future_to_url = {}
                for article_data in article_list_insert:
                    if isinstance(article_data, dict):
                        # 这个hdfs_path不一定是最后的结果
                        hdfs_path = None
                        if "article_doi" in article_data:  # 使用doi 为文件命名 科学
                            hdfs_path = self.get_hdfs_path(article_data.get("article_doi", ""), headers=None,
                                                       default_type='.pdf')
                        pdf_url = article_data.get('pdf_url', "")
                        if pdf_url:
                            future_to_url[
                                executor.submit(self._handle_one_file, pdf_url, hdfs_path=hdfs_path)] = pdf_url
            result_list = []
            for future in concurrent.futures.as_completed(future_to_url):
                pdf_url = future_to_url[future]
                try:
                    # 获取结果，但这里我们主要是为了确保任务完成，结果在 _handle_one_file 内部处理了
                    # 也可以让 _handle_one_file 返回结果字典，然后在这里收集
                    result = future.result()  # 尝试获取结果，如果 _handle_one_file 抛出异常，这里会捕获
                    result_list.append(result)
                    with self._progress_lock:
                        if result['result']:
                            self._completed_count += 1
                        else:
                            self._error_count += 1
                except Exception as e:
                    with self._progress_lock:
                        self._error_count += 1
                    if show_progress:
                        # 错误信息直接在 _handle_one_file 中打印会更详细，这里只做统计
                        pass
                        # print(f"\n处理文件时发生严重错误 (URL: {url}): {e}") # 避免重复打印

                if show_progress:
                    with self._progress_lock:  # 确保打印是线程安全的
                        percentage = ((self._completed_count + self._error_count) / self._total_files) * 100
                        print(
                            f"\r文件下载 开始处理 {self._total_files} 个文件，使用 {self.max_workers} 个线程。  进度: {self._completed_count + self._error_count}/{self._total_files} ({percentage:.2f}%) 成功: {self._completed_count} 失败: {self._error_count}",
                            end='\t')
                        sys.stdout.flush()
        if show_progress:
            print(f"\n所有任务处理完成。 成功: {self._completed_count} 失败: {self._error_count}")
        return result_list

    def _handle_one_file(self, file_url: str, hdfs_path: Optional[str] = None) -> Dict[str, Union[str, bool, None]]:
        """
        处理单个文件链接：下载、验证、上传。
        这个方法在多线程中执行，不应直接抛出异常，而是返回结果或记录内部状态。
        """
        try:
            # 1. 检查URL协议和文件扩展名
            if not file_url.startswith(('http://', 'https://')):
                print(f"跳过无效URL: {file_url}")
                return {'msg': 'invalid_url', 'url': file_url, "result": False, "hdfs_path": None}

            # 2. 下载文件，带重试逻辑
            response = None
            for _ in range(5):  # 重试3次
                # kwargs 会传递 timeout 等参数给 requests.get/post
                response = self.handler.fetch(file_url, method='GET', headers=self.headers, retry_count= 3 , **self.kwargs)
                if not response or response.status_code != 200 or response.text.strip().startswith("<!DOCTYPE html>"):
                    continue
                else:
                    break
            if not response or response.status_code != 200:
                print(f"下载失败或无效响应: {file_url}, 状态码: {response.status_code if response else 'N/A'}")
                return {'msg': 'download_failed', 'url': file_url, "result": False, hdfs_path: None}

            # 3. 验证下载内容是否有效
            if self.strict_mime_check:
                content_type = response.headers.get('Content-Type', '').strip().lower()
                # 注意：这里 original code uses aiohttp.ClientResponse.headers, but response is requests.Response.
                # headers.get() is compatible, but type hint should be Dict[str, str].
                expected_mime = self.mime_map.get(self.get_smart_file_extension(file_url, response.headers))
                if expected_mime and not content_type.startswith(expected_mime):
                    if not content_type.startswith('application/octet-stream'):
                        print(f"MIME类型不匹配或无效: {file_url}, Content-Type: {content_type}, 期望: {expected_mime}")
                        return {'msg': 'mime_type_mismatch', 'url': file_url, "result": False}

            if not response.content or response.headers.get("content-length") == '0':
                print(f"下载内容为空: {file_url}")
                return {'msg': 'empty_content', 'url': file_url, "result": False,"hdfs_path":None}

            if hdfs_path:
                # 如果提供了 hdfs_path，则使用它作为文件名
                file_name = hdfs_path.split('/')[-1]
            else:
                # 4. 获取安全文件名并上传到HDFS
                safe_filename = self.get_file_name(file_url, response.headers)
                if not safe_filename:
                    print(f"无法获取安全文件名: {file_url}")
                    return {'msg': 'filename_error', 'url': file_url, "result": False, "hdfs_path": None}
                file_name = safe_filename
                hdfs_path = self.get_hdfs_path(url=file_url, headers=response.headers, default_type='.pdf')
            # print(f"file_name:{file_name}")
            self._upload_content_to_hdfs(response.content, file_name)
            # print(f"成功处理: {file_url} -> HDFS: {hdfs_path}")
            return {'msg': 'success', 'url': file_url, "result": True, "hdfs_path": hdfs_path}

        except Exception as e:
            # 捕获所有其他异常并记录
            print(f"处理文件 {file_url} 时发生异常: {e}")
            return {'msg': 'exception', 'url': file_url, "result": False, "error": str(e)}

    async def _start_async(self, file_urls: List[str] | List[dict], show_progress: bool = True) -> List[
        Dict[str, Union[str, bool, None]]]:
        """
        使用asyncio和aiohttp异步处理所有文件链接，并显示实时进度。
        """
        total_files = len(file_urls)
        if total_files == 0:
            if show_progress:
                print("没有需要处理的文件链接。")
            return []
        sem = asyncio.Semaphore(self.max_workers)
        completed_count = 0
        error_count = 0
        progress_interval = max(1, total_files // 10)
        result_list = []
        async with aiohttp.ClientSession(headers=self.headers) as session:
            article_list_insert = []
            # 创建任务，并建立从任务到URL的映射，以便在出错时定位
            if isinstance(file_urls[0], dict):
                article_list_insert = file_urls.copy()
                if not "pdf_url" in file_urls[0]:
                    raise ValueError("字典列表中的每个字典必须包含 'pdf_url' 键。")
                tasks = []
                for article_data in article_list_insert:
                    if isinstance(article_data, dict):
                        if "article_doi" in article_data:  # 使用doi 为文件命名 科学
                            hdfs_path = self.get_hdfs_path(article_data.get("article_doi", ""), headers=None,
                                                           default_type='.pdf')
                        elif "pdf_url" in article_data and article_data.get('pdf_url', ""):
                            hdfs_path = self.get_hdfs_path(article_data.get("pdf_url", ""), headers=None,
                                                           default_type='.pdf')
                        else:
                            # print("article_doi  pdf_url  都不在keys")
                            pass

                        pdf_url = article_data.get('pdf_url', "")
                        if pdf_url:
                            tasks.append(asyncio.create_task(
                                self._handle_one_file_async(pdf_url, session, sem, hdfs_path=hdfs_path)))
            if isinstance(file_urls[0], str):
                tasks = [asyncio.create_task(self._handle_one_file_async(url, session, sem)) for url in file_urls]
            # await asyncio.gather(*tasks)
            # 使用 asyncio.as_completed 来在任务完成时立即处理
            for future in asyncio.as_completed(tasks):
                try:
                    result = await future
                    result_list.append(result)
                    if result['result']:
                        completed_count += 1
                    else:
                        error_count += 1
                except Exception as e:
                    error_count += 1
                    # if show_progress:
                    #     print(f"\n处理文件时发生严重错误  {e}")
                finally:
                    if show_progress:
                        percentage = (completed_count / total_files) * 100
                        print(
                            f"\r文件下载 进度: {completed_count}/{total_files} ({percentage:.2f}%)     {total_files} 个文件，使用异步模式.,最大并发 {self.max_workers}",
                            end='\t')
                        sys.stdout.flush()
        if show_progress:
            print(f"\r文件上传 路径 {self.hdfs_path} 所有异步任务处理完成。 完成{completed_count}/{total_files} <UNK>. 报错 {error_count}/{total_files} <UNK>")
        return result_list

    async def _handle_one_file_async(self, file_url: str, session: aiohttp.ClientSession, sem: asyncio.Semaphore,
                                     hdfs_path=None):
        """
        异步处理单个文件链接。
        """
        async with sem:
            try:
                if not file_url.startswith(('http://', 'https://')):
                    print(f"跳过无效URL: {file_url}")
                    return {'msg': 'invalid_url', 'url': file_url, "result": False, "hdfs_path": hdfs_path}
                retry_count = 6
                for retry in range(retry_count):
                    try:
                        timeout = aiohttp.ClientTimeout(total=120, connect=40, sock_read=40)
                        async with session.get(file_url, timeout=timeout, **self.kwargs, proxies=self.proxies) as resp:
                            resp.raise_for_status()
                            if retry < int(retry_count / 2):
                                if resp.status != 200:
                                    time_sleep = 2 + retry_count % 3
                                    await asyncio.sleep(time_sleep)  # 等待 retry 秒后重试
                                    self.proxies = self.proxy_util.get_proxy()
                                    continue
                                content = await resp.read()
                            else:
                                # 循环读取数据块并存入列表
                                byte_chunks = []
                                chunk_size = 1024 * 500  # 100kb 每个数据块的大小
                                async for chunk in resp.content.iter_chunked(chunk_size):
                                    byte_chunks.append(chunk)
                                # 使用 b''.join() 高效拼接所有字节块
                                content = b"".join(byte_chunks)
                            resp_headers = resp.headers
                            if not content or len(content) < 5 * 1024:
                                continue
                            if not hdfs_path:
                                hdfs_path = self.get_hdfs_path(url=file_url, headers=resp_headers, default_type='.pdf')
                                safe_filename = self.get_file_name(file_url, resp_headers)
                                # file_name = f"{self.hdfs_name}_{safe_filename}"
                                file_name = safe_filename
                            else:
                                file_name = hdfs_path.split('/')[-1]
                            await asyncio.to_thread(self._upload_content_to_hdfs, content, file_name)

                            return {'msg': 'success', 'url': file_url, "result": True, "hdfs_path": hdfs_path}
                    # except asyncio.TimeoutError:
                    #     # 这个异常现在主要由 connect=90 或 sock_read=90 触发
                    #     # print(f"下载超时: {file_url}")
                    #     continue
                    except Exception as e:
                        time_sleep = 2  + retry_count % 3
                        await asyncio.sleep(time_sleep)  # 等待 retry 秒后重试
                        continue
                else:
                    print(f"异步处理文件失败: {file_url}, 重试次数: {retry + 1}")
                    return {'msg': 'failed', 'url': file_url, "result": False, "hdfs_path": hdfs_path}
            except Exception as e:
                # 重新抛出异常
                print(f"异步处理文件时发生异常: {file_url}, 错误: {e}")


    def _upload_file_to_hdfs(self, file_path: Path):
        """
        将单个文件上传到HDFS，带重试逻辑。
        """
        for attempt in range(3):
            try:
                with open(file_path, 'rb') as f:
                    files = {'file': (file_path.name, f)}
                    data = {'parentPath': self.hdfs_path, 'type': '2'}
                    response = requests.post(self.hdfs_addr, data=data, files=files, timeout=60)
                if response.status_code == 200:
                    return
                else:
                    raise ConnectionError(f'HDFS上传失败，状态码: {response.status_code}, 消息: {response.text}')
            except Exception as e:
                if attempt < 2:
                    time.sleep(2)
                else:
                    raise

    def _upload_content_to_hdfs(self, content: bytes, file_name: str):
        """
         将response.content 传到HDFS，带重试逻辑。
         """
        for attempt in range(3):
            try:
                file_obj = BytesIO(content)
                files = {'file': (file_name, file_obj)}
                data = {'parentPath': self.hdfs_path, 'type': '2'}
                response = requests.post(self.hdfs_addr, data=data, files=files, timeout=60)
                if response.status_code == 200:
                    return
                else:
                    raise ConnectionError(f'HDFS上传失败，状态码: {response.status_code}, 消息: {response.text}')
            except Exception as e:
                if attempt < 2:
                    time.sleep(2)
                else:
                    print(f"上传内容到HDFS失败: {e}")

    def upload_all_local_files(self, local_path: str):
        """
        上传指定本地目录下的所有符合类型的文件。
        """
        all_files = []
        for ext in self.allowed_extensions:
            all_files.extend(Path(local_path).glob(f'**/*{ext}'))

        if not all_files:
            # print(f"在目录 '{local_path}' 下没有找到允许的文件类型: {self.allowed_extensions}")
            return

        print(f"找到 {len(all_files)} 个文件，开始上传...")
        for file_path in all_files:
            if not file_path.exists():
                continue
            try:
                self._upload_file_to_hdfs(file_path)
                print(f"已上传: {file_path.name}")
                for _ in range(3):
                    try:
                        file_path.unlink()
                        break
                    except PermissionError:
                        time.sleep(0.2)
            except Exception as e:
                print(f"上传本地文件失败: {file_path.name}, 错误: {e}")

    def get_file_name(self, url: str, headers: Optional[Union[dict, aiohttp.ClientResponse.headers]]) -> Optional[str]:
        # 文件类型
        file_extension = self.get_smart_file_extension(url, headers)
        file_extension = file_extension.replace(";","")
        if file_extension.lower() == ".svg":
            file_extension = ".html"
        if not file_extension:
            print(f"无法从URL '{url}' 获取有效的文件扩展名。")
            return None
        file_name = self.get_smart_filename(url,headers= headers)
        if not file_name:
            print(f"无法从URL '{url}' 获取有效的文件名基础。")
            return None
        # 返回一个安全的文件名
        return sanitize_filename(f"{file_name}{file_extension}")

    def get_smart_file_extension(
            self,
            url: str,
            headers: Optional[Dict[str, str]] = None,
            default_type: str = ".pdf"
    ) -> Optional[str]:
        """
        [全新设计] 根据URL和HTTP响应头，智能地判断并返回一个有效的文件扩展名。

        该函数会按照以下优先级进行判断，一旦找到有效的扩展名就会立即返回：
        1.  **从URL路径中直接解析** (e.g., /path/to/file.pdf)。
        2.  **从URL路径的最后部分解析** (用于干净URL, e.g., /path/to/id/pdf)。
        3.  **从响应头的 'Content-Disposition' 中解析**。
        4.  **从响应头的 'Content-Type' 中使用正则表达式模式匹配**。
        5.  **从响应头的 'Content-Type' 中使用内置库进行猜测**。

        :param url: 文件的来源URL。
        :param headers: (可选) requests库返回的响应头字典。
        :return: 一个小写的、以点开头的文件扩展名 (如 '.pdf')，如果无法判断则返回 None。

        """

        # ---  没有headers 使用url ---
        def _is_valid_extension(ext: str) -> bool:
            """
            检查扩展名是否看起来像一个有效的文件扩展名。
            """
            return ext in self.allowed_extensions

        if not headers:
            if not url or not isinstance(url, str):
                return None

            # --- 阶段一：优先从URL解析 ---
            try:
                path = Path(urlparse(url).path)

                # 1a. 尝试从常规路径后缀获取
                ext = path.suffix.lower()
                if _is_valid_extension(ext):
                    return ext
                else:
                    return default_type  # 默认返回.pdf

                # 1b. 尝试从干净URL的最后一个路径段获取
                path_segments = [seg for seg in path.parts if seg != '/']
                if path_segments:
                    # 假设最后一个路径段是文件类型
                    last_segment_ext = f".{path_segments[-1].lower()}"
                    # 简单的验证，确保它看起来像一个文件后缀 (例如，长度不超过5)
                    if 1 < len(last_segment_ext) <= 5:
                        return last_segment_ext
            except Exception as e:
                print(f"解析URL '{url}' 时发生错误: {e}")
                return default_type  # 默认返回.pdf
        else:
            # 2a. 尝试从 Content-Disposition 头获取
            content_disposition = headers.get('content-disposition')
            if content_disposition:
                match = re.search(r"filename\*=UTF-8''(.+)", content_disposition, re.IGNORECASE)
                if not match:
                    match = re.search(r'filename="?([^"]+)"?', content_disposition, re.IGNORECASE)
                if match:
                    filename = unquote(match.group(1).strip("'\" "))
                    # "热点周报2025年第26期（总149期）-水印.pdf;'" 会多一个;
                    ext = Path(filename).suffix.lower()
                    if ext:
                        return ext.replace(";","")

            # 2b. 尝试从 Content-Type 头推断
            content_type = headers.get('Content-Type', '').split(';')[0].strip().lower()
            if content_type:
                # 定义MIME类型匹配模式
                mime_patterns = [
                    (r'application/(x-)?pdf', '.pdf'),
                    (r'application/vnd\.openxmlformats-officedocument\.wordprocessingml\.document', '.docx'),
                    (r'application/msword', '.doc'),
                    (r'text/plain', '.txt'),
                    (r'image/jpeg', '.jpg'),
                    (r'image/png', '.png'),
                    (r'image/gif', '.gif'),
                ]
                for pattern, extension in mime_patterns:
                    if re.match(pattern, content_type):
                        return extension

                # 使用内置库作为最后备选
                guessed_ext = mimetypes.guess_extension(content_type)
                if guessed_ext:
                    return guessed_ext.lower()

            return default_type  # 默认返回.pdf




    # 文件名
    def get_smart_filename(self, url: str,
                           headers: Optional[Union[dict,None]]=None) -> Optional[str]:
        """
        [最终改良版] 从给定的URL中，智能地提取一个有意义的文件名基础（不含扩展名）。

        该函数会按照以下优先级进行判断：
        1.  优先从URL中提取DOI (e.g., 10.xxxx/xxxxx)。
        2.  对URL路径进行预处理，移除像 /pdf /download 这样的尾部动作词。
        3.  对预处理后的路径，智能判断有意义的部分。
            -   特别处理 `.../some-id/v1` 这样的结构。
            -   处理普通的文件名或ID。
        4.  最后，提供一个基于域名的备用方案。

        :param url: 文件的来源URL。
        :return: 一个字符串形式的文件名基础，如果URL无效则返回None。
        """
        if headers:
            # 2a. 尝试从 Content-Disposition 头获取
            content_disposition = headers.get('content-disposition')
            if content_disposition:
                match = re.search(r"filename\*=UTF-8''(.+)", content_disposition, re.IGNORECASE)
                if not match:
                    match = re.search(r'filename="?([^"]+)"?', content_disposition, re.IGNORECASE)
                if match:
                    filename = unquote(match.group(1).strip("'\" "))
                    if filename:
                        filename = Path(filename).stem
                        return filename

        if not url or not isinstance(url, str):
            return None

        try:
            # 为了处理 '         https://...' 这样的情况，先去除首尾空格
            url = url.strip()
            # 1. 优先尝试从URL中提取DOI作为文件名基础
            # 这个正则表达式可以匹配大多数DOI格式
            doi_pattern = re.compile(r"(10\.\d{4,9}/[-._;()/:A-Z0-9]+)", re.IGNORECASE)
            doi_match = doi_pattern.search(url)
            if doi_match:
                # 提取DOI主体部分，并替换特殊字符
                doi_str = doi_match.group(1)
                # 如果DOI后面跟着/pdf, /fulltext等，移除它们
                for suffix in ['/pdf', '/fulltext', '/view', '/download']:
                    if doi_str.lower().endswith(suffix):
                        doi_str = doi_str[:-len(suffix)]
                        break
                return doi_str.replace('/', '_').replace('.', '_')

            # 如果没有DOI，则解析URL路径
            parsed_url = urlparse(url)
            path = Path(parsed_url.path)

            # 将路径转为列表，方便操作
            path_segments = [seg for seg in path.parts if seg and seg != '/']

            # 2. 预处理路径：移除尾部的通用动作词
            action_words = ['download', 'pdf', 'fulltext', 'view']
            if path_segments and path_segments[-1].lower() in action_words:
                path_segments.pop()  # 移除最后一个元素

            if not path_segments:
                return parsed_url.netloc.replace('.', '_')

            # 3. 对预处理后的路径进行智能判断

            # 针对 .../14-681/v1 这样的结构
            # 如果最后一个段看起来像版本号 (v1, v2 ...)，就和前一个段合并
            if len(path_segments) >= 2 and re.match(r'(v\d+|^image|^pic)', path_segments[-1], re.IGNORECASE) or len(path_segments[-1])<20:
                base_name = f"{path_segments[-2]}_{path_segments[-1]}"
                return base_name.replace('.', '_')

            # 对于其他情况，我们认为最后一个（或预处理后剩下的最后一个）路径段最有意义
            # 这能正确处理 https://osf.io/5ayfm_v1/ 和其他常规URL
            # 也包括 /articles/12345 这样的情况
            # 同时，也处理了带有文件扩展名的情况，因为path.stem会自动移除后缀
            # 例如 /file.v1.pdf -> stem是 'file.v1'
            last_segment_path = Path(path_segments[-1])
            if last_segment_path.suffix:
                return last_segment_path.stem.replace('.', '_')

            return path_segments[-1].replace('.', '_')

        except Exception:
            # 捕获所有可能的解析错误，返回None
            return None
    # hdfs_path
    def get_hdfs_path(self, url: str, headers: Optional[Union[dict, aiohttp.ClientResponse.headers]] = None,
                      default_type: str = ".pdf"):
        """
        从URL中提取HDFS路径，默认使用hdfs_name作为目录名。
        :param url: 文件的来源URL。
        :param default_type: 如果无法从URL中获取文件扩展名，则使用此默认类型。
        :return: HDFS路径字符串。
        """
        if not url or not isinstance(url, str):
            return None
        # 这里可以根据需要进一步处理URL来生成更具体的HDFS路径
        file_name = self.get_smart_filename(url,headers=headers)
        file_extension = self.get_smart_file_extension(url, headers=headers)
        if not file_extension:
            file_extension = default_type
        if not file_name or not file_extension:
            print(f"无法从URL '{url}' 获取有效的文件名或扩展名。")
            return None
        return f"{self.hdfs_path}/{file_name}{file_extension}"

    # ... (在您的 FileUploader 类的末尾，添加以下新方法) ...

    # ★★★ 新增的POST专用方法 ★★★
    def start_post_thread(self, url, post_datas: List[Dict], article_dois: List[str], show_progress: bool = True,
                          **kwargs):
        """
        【新增】使用线程池并发处理所有基于POST请求的下载任务。

        :param post_tasks: 包含POST任务信息的字典列表。
                           每个字典应包含 'url' 和 'post_data'。
                           可选 'filename_meta' 用于生成文件名。
        :param show_progress: 是否显示进度条。
        """
        if not post_datas:
            print("POST任务列表为空，无需处理。")
            return []
        if len(post_datas) != len(article_dois):
            raise ValueError("post_datas 和 article_dois 列表长度不匹配。 必须一一对应")
        self._total_files = len(post_datas)
        self._completed_count = 0
        self._error_count = 0
        self._progress_lock = threading.Lock()

        print(f"开始处理 {self._total_files} 个POST下载任务，使用 {self.max_workers} 个线程。")

        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_task = {}
            for idx, post_data in enumerate(post_datas):
                article_doi = article_dois[idx]
                future_to_task.update(
                    {executor.submit(self._handle_one_post_file, url, post_data, article_doi, **kwargs): article_doi})
            result_list = []
            for future in concurrent.futures.as_completed(future_to_task):
                article_doi = future_to_task[future]
                try:
                    result = future.result()
                    result_list.append(result)
                    with self._progress_lock:
                        self._completed_count += 1
                except Exception as e:
                    with self._progress_lock:
                        self._error_count += 1
                    print(f"\n处理POST任务时发生严重错误 (URL: {article_doi}): {e}")

                if show_progress:
                    with self._progress_lock:
                        percentage = ((self._completed_count + self._error_count) / self._total_files) * 100
                        print(
                            f"\rPOST文件下载 进度: {self._completed_count + self._error_count}/{self._total_files} ({percentage:.2f}%) 成功: {self._completed_count} 失败: {self._error_count}",
                            end='\t')
                        sys.stdout.flush()

        if show_progress:
            print(f"\n所有POST任务处理完成。 成功: {self._completed_count} 失败: {self._error_count}")
        return result_list

    # ★★★ 新增的POST专用工作函数 ★★★
    def _handle_one_post_file(self, url: str, post_data: Dict, article_doi: str, **kwargs) -> Dict:
        """
        【新增】处理单个POST下载任务：下载、验证、上传。
        """
        try:
            # 1. 下载文件，使用POST方法
            response = None
            for _ in range(3):  # 重试逻辑
                # ★ 核心区别：调用 self.handler.fetch 时明确指定 method='POST' 和 data
                response = self.handler.fetch(url, method='POST', data=post_data, headers=self.headers, **self.kwargs)
                if response and response.status_code == 200:
                    break
                time.sleep(1)

            if not response or response.status_code != 200:
                print(f"POST下载失败: {url}, 状态码: {response.status_code if response else 'N/A'}")
                return {'msg': 'download_failed', "result": False,"article_doi":article_doi}

            # --- 后续逻辑与 _handle_one_file 类似 ---

            # 2. 验证下载内容是否有效 (这部分逻辑可以保持共用)
            if self.strict_mime_check and not self.mime_map.get(self.get_smart_file_extension(url, response.headers)):
                # ... MIME检查 ...
                pass

            if not response.content or response.headers.get("content-length") == '0':
                print(f"下载内容为空: {url}")
                return {'msg': 'empty_content', "result": False,"article_doi":article_doi,"hdfs_path":None}

            # 3. 获取安全文件名并上传到HDFS
            hdfs_path = self.get_hdfs_path(url=article_doi, headers=response.headers)
            if not hdfs_path:
                print(f"无法获取安全文件名: {url}")
                return {'msg': 'filename_error', "result": False,"article_doi":article_doi,"hdfs_path":None}

            file_name = hdfs_path.split('/')[-1]
            self._upload_content_to_hdfs(response.content, file_name)

            return {'msg': 'success', "result": True, "hdfs_path": hdfs_path,"article_doi":article_doi}

        except Exception as e:
            print(f"处理POST文件 {url} 时发生异常: {e}")
            return {'msg': 'exception', "result": False, "error": str(e),"article_doi":article_doi,"hdfs_path":None}

# urls = [
#     'https://www.frontierspartnerships.org/journals/journal-of-abdominal-wall-surgery/articles/10.3389/jaws.2025.14535/pdf',
#     'https://www.frontierspartnerships.org/journals/journal-of-abdominal-wall-surgery/articles/10.3389/jaws.2025.14723',
#     'https://osf.io/5ayfm_v1/download/',
#     'https://f1000research.com/articles/14-681/v1/pdf?article_uuid=c38730d8-fcd7-4274-9734-d011be489e10',
#     "https://pdf.dfcfw.com/pdf/H3_AP202507101706397488_1.pdf?1752166888000.pdf"
# ]
