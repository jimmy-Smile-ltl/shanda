
import asyncio
import concurrent.futures
import mimetypes
import re
import sys
import time
from io import BytesIO
from pathlib import Path
from typing import List, Dict, Optional, Union
from urllib.parse import unquote, urlparse

import aiohttp
import curl_cffi
import execjs
import requests
from pathvalidate import sanitize_filename

from myutil.handleRequest import SingleRequestHandler
import  os
import certifi
from curl_cffi.requests.session import AsyncSession as curl_AsyncSession
from curl_cffi.requests.session import Session as curl_Session
import urllib
import  urllib.parse
import threading
class Curl_cffiFileUploader:
    """
    一个通用的文件上传类，支持多线程和异步模式。
    可以从URL列表下载指定类型的文件（包括文档和图片），并上传到HDFS。
    """

    def __init__(self,
                 hdfs_name: str,
                 max_workers: int = 10,
                 strict_mime_check: bool = True,
                 test_url= None,
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
        self.kwargs = kwargs
        self.max_workers = max_workers
        self.strict_mime_check = strict_mime_check
        # 默认支持文档和图片
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
        self.executeJS = execjs.compile(open("../pro21 科技文献 Cogent OA/main.js", mode="r", encoding="utf8").read())
        absolutePath = os.path.abspath(__file__)
        # 获取当前文件的绝对路径
        user = absolutePath.split(os.sep)[2]
        if user == "JimmySmile" or user.find("immy") != -1:
            # print("当前电脑是JimmySmile, 证书位置使用默认位置")
            self.ca_bundle_path = certifi.where()  # 有中文路径，报错，是基于linux下面的一个包开发的，支持性欠缺
        elif user == "唐凯":
            # print(
            #     "当前电脑是Jimmmy的工作台，证书位置位于 C:\cert\cacert.pem 这么做原因是 工作台含中文路径，curl_cffi不支持")
            self.ca_bundle_path = "C:\cert\cacert.pem"
        self.proxies ={'http': 'http://t15142135039542:ykvgf8ax@d959.kdltps.com:15818',
                        'https': 'http://t15142135039542:ykvgf8ax@d959.kdltps.com:15818'
                      }
        self.lock = asyncio.Lock()
        self.flush_session()

    def start_async(self,file_urls: List[str] = [],
                    show_progress: bool = True):
        """
        启动异步上传任务。
        :param file_urls: 包含文件URL的列表。
        :param show_progress: 是否显示进度条。
        """
        if not file_urls:
            raise ValueError("file_urls 不能为空，请提供至少一个文件链接。")

        return asyncio.run(self._start_async(file_urls, show_progress=show_progress))

    async def _start_async(self, file_urls: List[str], show_progress: bool = True):
        """
        使用asyncio和aiohttp异步处理所有文件链接，并显示实时进度。
        """
        total_files = len(file_urls)
        if total_files == 0:
            if show_progress:
                print("没有需要处理的文件链接。")
            return
        completed_count = 0
        error_count = 0
        progress_interval = max(1, total_files // 10)
        result_list = []
        # ✅ 核心改动：创建Semaphore来控制并发
        sem = asyncio.Semaphore(self.max_workers)
        await self.flush_async_session()
        tasks = [asyncio.create_task(self._handle_one_file_async(url, sem)) for url in file_urls]
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
                if show_progress and (completed_count % progress_interval == 0 or completed_count == total_files):
                    percentage = (completed_count / total_files) * 100
                    print(
                        f"\r进度: {completed_count}/{total_files} ({percentage:.2f}%)     {total_files} 个文件，使用异步模式.,最大并发 {self.max_workers}",
                        end='\t')
                    sys.stdout.flush()
            except Exception as e:
                error_count += 1
                if show_progress:
                    print(f"\n处理文件时发生严重错误  {e}")
        if show_progress:
            print(
                f"\r文件上传 路径 {self.hdfs_name} 所有异步任务处理完成。 完成{completed_count}/{total_files} <UNK>. 报错 {error_count}/{total_files} <UNK>")
        handle_dict = {
            "hdfs_name": self.hdfs_name,
            "hdfs_path": self.hdfs_path,
            "total_files": total_files,
            "completed_count": completed_count,
            "error_count": error_count,
        }
        await  self.curl_async_session.close()
        return handle_dict, result_list

    async def _handle_one_file_async(self, file_url: str, sem: asyncio.Semaphore):
        """
        异步处理单个文件链接。
        """
        async with sem:
            try:
                if not file_url.startswith(('http://', 'https://')):
                    return {'msg': 'invalid_url', 'url': file_url, "result": False}
                retry_count = 8
                for retry in range(retry_count):
                    try:
                        if retry < int(retry_count / 2):
                            resp = await self.curl_async_session.get(file_url,verify=self.ca_bundle_path,timeout=30,proxies=self.proxies, **self.kwargs)
                            resp.raise_for_status()
                            if resp.status_code != 200:
                                await self.flush_async_session()
                                continue
                            content = resp.content
                            resp_headers = resp.headers
                            resp.close()
                        else:
                            resp = await self.curl_async_session.get(file_url,verify=self.ca_bundle_path,proxies=self.proxies, stream=True,timeout=30, **self.kwargs)
                            # 检查请求是否成功
                            resp.raise_for_status()
                            chunk_list = []
                            total_downloaded = 0
                            async for chunk in resp.aiter_bytes(chunk_size=1024 * 500 ):
                                if chunk:
                                    chunk_list.append(chunk)
                                    total_downloaded += len(chunk)
                                    print(f"\r{file_url} 流式下载 已下载: {total_downloaded / 1024:.2f} KB", end="")
                            content = b''.join(chunk_list)  # 拼接得到
                            resp_headers = resp.headers

                        safe_filename = self.get_file_name(file_url, resp_headers)
                        # file_name = f"{self.hdfs_name}_{safe_filename}"
                        file_name = safe_filename
                        await asyncio.to_thread(self._upload_content_to_hdfs, content, file_name)
                        hdfs_path = self.get_hdfs_path(url=file_url, headers=resp_headers, default_type='.pdf')
                        return {'msg': 'success', 'url': file_url, "result": True, "hdfs_path": hdfs_path}
                    except Exception as e:
                        # time_sleep = 1  # retry % 2 + 1
                        # await asyncio.sleep(time_sleep)  # 等待 retry 秒后重试
                        async with self.lock:
                            await self.flush_async_session()
                        continue
                else:
                    print(f"异步处理文件失败: {file_url}, 重试次数: {retry + 1}")
                    return {'msg': 'failed', 'url': file_url, "result": False, "hdfs_path": None}
            except Exception as e:
                # 重新抛出异常
                print(f"处理文件时发生异常: {file_url}, 错误: {e}")
                pass

    def start_thread(self, file_urls: List[str] = [], show_progress: bool = True) -> tuple[dict,list[dict]]:
        """
        启动多线程上传任务。
        :param file_urls: 包含文件URL的列表。
        :param show_progress: 是否显示进度条。
        """

        if not file_urls:
            # raise ValueError("file_urls 不能为空，请提供至少一个文件链接。")
            print("file_urls 不能为空，请提供至少一个文件链接。")
            return {},[]
        self._thread_completed_count = 0
        self._thread_error_count = 0
        self._thread_total_files = len(file_urls)
        self._progress_lock = threading.Lock()  # 用于多线程进度打印的锁

        if show_progress:
            print(f"开始处理 {self._thread_total_files} 个文件，使用 {self.max_workers} 个线程。")

        results_list = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # 提交任务到线程池
            future_to_url = {executor.submit(self._handle_one_file_thread, url): url for url in file_urls}

            for future in concurrent.futures.as_completed(future_to_url):
                url = future_to_url[future]
                try:
                    result = future.result()  # 获取线程的返回结果
                    results_list.append(result)
                    with self._progress_lock:  # 保护计数器更新和打印
                        if result['result']:
                            self._thread_completed_count += 1
                        else:
                            self._thread_error_count += 1
                        self._print_progress_thread(show_progress)

                except Exception as exc:
                    # 这通常不应该发生，因为 _handle_one_file_thread 内部应该处理了异常并返回结果字典
                    # 但为了健壮性，仍然捕获一下，并更新失败计数
                    with self._progress_lock:
                        self._thread_error_count += 1
                        self._print_progress_thread(show_progress, extra_msg=f"任务 {url} 发生异常: {exc}")
                    results_list.append(
                        {'msg': 'exception_in_thread_pool', 'url': url, "result": False, "error": str(exc)})

        if show_progress:
            print(
                f"\n文件上传 路径 {self.hdfs_name} 所有多线程任务处理完成。 完成{self._thread_completed_count}/{self._thread_total_files}. 报错 {self._thread_error_count}/{self._thread_total_files}")

        return {
            "hdfs_name": self.hdfs_name,
            "hdfs_path": self.hdfs_path,
            "total_files": self._thread_total_files,
            "completed_count": self._thread_completed_count,
            "error_count": self._thread_error_count,
        }, results_list

    def _handle_one_file_thread(self, file_url: str) -> Dict[str, Optional[Union[str, bool]]]:
        """
        多线程处理单个文件链接。
        此方法将在线程池中运行，并使用同步的 curl_cffi.Session。
        """
        # 每个线程可以直接使用 self.curl_session，因为它被设计为线程安全
        # 也可以在每个线程内部创建一个新的 Session，但 curl_cffi 声称 Session 是安全的
        # 这里我们使用共享的 self.curl_session (即父类初始化时创建的 self.curl_session)

        try:
            if not file_url.startswith(('http://', 'https://')):
                return {'msg': 'invalid_url', 'url': file_url, "result": False}

            retry_count = 32
            for retry in range(retry_count):
                try:
                    # if retry % 8 ==0 and retry > 0:
                    curl_session = self.flush_session()
                    # 确保 kwargs 传递
                    current_kwargs = self.kwargs.copy()
                    timeout_val = current_kwargs.pop('timeout', 60)  # 默认30秒
                    if retry < int(retry_count / 2):
                        # 使用同步 session
                        resp = curl_session.get(file_url, verify=self.ca_bundle_path, timeout=timeout_val,
                                                     proxies=self.proxies, **current_kwargs)
                        resp.raise_for_status()
                        if resp.status_code != 200:
                            time.sleep(1)
                            continue
                        content = resp.content
                        resp_headers = resp.headers
                    else:
                        resp = curl_session.get(file_url, verify=self.ca_bundle_path, stream=True,
                                                     timeout=timeout_val, proxies=self.proxies, **current_kwargs)
                        resp.raise_for_status()
                        if resp.status_code != 200:
                            time.sleep(1)
                            continue
                        chunk_list = []
                        total_downloaded = 0
                        for chunk in resp.iter_content(chunk_size=1024 * 500):  # 同步迭代器
                            if chunk:
                                chunk_list.append(chunk)
                                total_downloaded += len(chunk)
                                # print(f"\r{file_url} 流式下载 已下载: {total_downloaded / 1024:.2f} KB", end="")
                        content = b''.join(chunk_list)
                        resp_headers = resp.headers
                    safe_filename = self.get_file_name(file_url, resp_headers)
                    file_name = safe_filename
                    self._upload_content_to_hdfs(content, file_name)  # 这个方法本身就是同步的requests
                    hdfs_path = self.get_hdfs_path(url=file_url, headers=resp_headers, default_type='.pdf')
                    return {'msg': 'success', 'url': file_url, "result": True, "hdfs_path": hdfs_path}
                except Exception as e:
                    # 在这里刷新 session 可能会影响其他线程，最好是在初始化时刷新或有更智能的全局刷新机制
                    # 如果代理失效，可以尝试获取新的代理
                    time.sleep(1) # 等待代理自动切换
                    continue  # 继续重试
            else:
                print(f"多线程处理文件失败: {file_url}, 重试次数: {retry + 1}")
                return {'msg': 'failed', 'url': file_url, "result": False, "hdfs_path": None}
        except Exception as e:
            print(f"处理文件时发生异常: {file_url}, 错误: {e}")
            return {'msg': 'exception', 'url': file_url, "result": False, "hdfs_path": None, "error": str(e)}

    def _print_progress_thread(self, show_progress: bool, extra_msg: str = ""):
        """多线程安全的进度打印。"""
        if show_progress:
            percentage = ((
                                      self._thread_completed_count + self._thread_error_count) / self._thread_total_files) * 100
            status_line = (
                f"\r进度: {self._thread_completed_count + self._thread_error_count}/{self._thread_total_files} "
                f"({percentage:.2f}%) 成功: {self._thread_completed_count} 失败: {self._thread_error_count} "
                f"最大并发: {self.max_workers}"
            )
            if extra_msg:
                status_line += f" - {extra_msg}"
            sys.stdout.write(status_line + " " * (max(0, 80 - len(status_line))))  # 填充空白以清除旧行
            sys.stdout.flush()

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
                    # time.sleep(2)
                    pass
                else:
                    print(f"上传内容到HDFS失败: {e}")

    def get_file_name(self, url: str, headers: dict | aiohttp.client.ClientResponse) -> Optional[str]:
        # 文件类型
        file_extension = self.get_smart_file_extension(url, headers)
        if not file_extension:
            print(f"无法从URL '{url}' 获取有效的文件扩展名。")
            return None
        file_name = self.get_smart_filename(url)
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
                    ext = Path(filename).suffix.lower()
                    if ext:
                        return ext

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
    def get_smart_filename(self, url: str) -> Optional[str]:
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
            if len(path_segments) >= 2 and re.match(r'v\d+', path_segments[-1], re.IGNORECASE):
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
    def get_hdfs_path(self, url: str, headers: curl_cffi.requests.Headers | None = None, default_type: str = ".pdf"):
        """
        从URL中提取HDFS路径，默认使用hdfs_name作为目录名。
        :param url: 文件的来源URL。
        :param default_type: 如果无法从URL中获取文件扩展名，则使用此默认类型。
        :return: HDFS路径字符串。
        """
        if not url or not isinstance(url, str):
            return None
        # 这里可以根据需要进一步处理URL来生成更具体的HDFS路径
        file_name = self.get_smart_filename(url)
        file_extension = self.get_smart_file_extension(url, headers=headers)
        if not file_extension:
            file_extension = default_type
        if not file_name or not file_extension:
            print(f"无法从URL '{url}' 获取有效的文件名或扩展名。")
            return None
        return f"{self.hdfs_path}/{self.get_smart_filename(url)}{file_extension}"

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
                "_curator_id": "DE.V1.384442208d11.175246108363",
                "optimizelyEndUserId": "oeu1752462427639r0.4834008165345577",
                "_hjSessionUser_864760": "eyJpZCI6ImU2ODlkNzcxLTc4ZTAtNTA1Yi1iODRjLTNjNzBhYzhiMDdmZSIsImNyZWF0ZWQiOjE3NTI0NjEwMTc0NjgsImV4aXN0aW5nIjp0cnVlfQ==",
                "displayMathJaxFormula": "true",
                "_gcl_au": "1.1.1250284419.1752462433",
                "hum_tandf_visitor": "d5a2291b-6b00-4875-a7a7-b8f7c76f56d2",
                "hum_tandf_synced": "true",
                "optimizelySession": "0",
                "_cm": "eyIxIjpmYWxzZSwiMiI6ZmFsc2UsIjMiOmZhbHNlfQ==",
                "MACHINE_LAST_SEEN": "2025-07-15T18%3A04%3A16.848-07%3A00",
                "JSESSIONID": "368E3FF6B84A793ED6D1F55597CC61A7",
                "OptanonAlertBoxClosed": "2025-07-8T01:39:25.556Z",
                "OptanonConsent": "isGpcEnabled=0&datestamp=Wed+Jul+8+2025+09%3A39%3A26+GMT%2B0800+(%E4%B8%AD%E5%9B%BD%E6%A0%87%E5%87%86%E6%97%B6%E9%97%B4)&version=202409.1.0&browserGpcFlag=0&isIABGlobal=false&hosts=&consentId=56be9072-385f-4e60-88b6-f4c1ca197892&interactionCount=1&isAnonUser=1&landingPath=NotLandingPage&groups=C0001%3A1%2CC0002%3A1%2CC0003%3A1%2CC0004%3A1&AwaitingReconsent=false&geolocation=%3B",
                "cf_clearance": "VSfNluKBYp2ilQ5bggjImEhf6BiTR2oSiEMPaYRLAR8-1752629970-1.2.1.1-QNZ5tpRU1WlbXWUSf1RJbEIXivlJAelPVszxKZlp1NsdbPY1SnvSQ_I2lymJmTH.OMYAwc0X3UmJfE6GqnbtioALoXyLjhALQ_ZhPXm6ajO3xII3Y16EM4iBx7Ixp716u4WtbfS9z2lLzp2JDDQSiijl1oqgc1w3KurnXB2eyaOhW3WFIO1yYnovpFqsvR6YSi7U4N80sH6VfCuL5RbtvlM3BUaX6NhJ.p4J31ym2dE",
                "_hjSession_864760": "eyJpZCI6ImVlNGU1MTE0LTAwZTktNGJjNC1iZDQyLTlkNmQwZmM5NjMwNCIsImMiOjE3NTI2Mjk5NjcxOTUsInMiOjAsInIiOjAsInNiIjowLCJzciI6MCwic2UiOjAsImZzIjowLCJzcCI6MH0=",
                "_ga_4819PJ6HEN": "GS2.1.s1752629967$o12$g0$t1752629967$j60$l0$h0",
                "_ga": "GA1.2.575950487.1752227647",
                "_gat_UA-3062505-46": "1",
                "_ga_0HYE8YG0M6": "GS2.1.s1752627891$o6$g1$t1752629969$j58$l0$h0"
            }
            page_url = f"https://www.tandfonline.com/doi/pdf/10.1080/0886022X.2025.2479573?download=true"
            # 使用curl_cffi的异步会话
            curl_session = curl_Session(impersonate="chrome110", proxies=self.proxies)
            curl_session.headers.update(headers)
            curl_session.cookies.update(cookies)
            self.curl_session = curl_session
            return curl_session
            # try:
            #     response = self.curl_session.get(page_url, impersonate="chrome110", verify=self.ca_bundle_path,
            #                                      proxies=self.proxies)
            # except Exception as e:
            #     # with self._progress_lock: 不能在这里枷加🔒，这样每个线程都会暂停的,时间太长了
            #     time.sleep(1)
            #     continue
            # if response.status_code == 200:
            #     # self.curl_async_session.cookies.update(self.curl_session.cookies.get_dict())
            #     break
            # else:
            #     time.sleep(1)
        # else:
        #     # print("curl_session 刷新失败，请检查网络连接或代理设置。")
        #     return curl_session


    async def flush_async_session(self):
        for i in range(8):
            headers = {
                "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
                "accept-language": "en,en-CN;q=0.9,zh-CN;q=0.8,zh;q=0.7",
                "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36"
            }
            cookies = {
                "timezone": "480",
                "_gid": "GA1.2.133528753.1752460898",
                "MAID": "XiU97VTM1ME4MYyiVyj//w==",
                "_curator_id": "DE.V1.384442208d11.175246108363",
                "optimizelyEndUserId": "oeu1752462427639r0.4834008165345577",
                "_hjSessionUser_864760": "eyJpZCI6ImU2ODlkNzcxLTc4ZTAtNTA1Yi1iODRjLTNjNzBhYzhiMDdmZSIsImNyZWF0ZWQiOjE3NTI0NjEwMTc0NjgsImV4aXN0aW5nIjp0cnVlfQ==",
                "displayMathJaxFormula": "true",
                "_gcl_au": "1.1.1250284419.1752462433",
                "hum_tandf_visitor": "d5a2291b-6b00-4875-a7a7-b8f7c76f56d2",
                "hum_tandf_synced": "true",
                "optimizelySession": "0",
                "_cm": "eyIxIjpmYWxzZSwiMiI6ZmFsc2UsIjMiOmZhbHNlfQ==",
                "MACHINE_LAST_SEEN": "2025-07-15T18%3A04%3A16.848-07%3A00",
                "JSESSIONID": "368E3FF6B84A793ED6D1F55597CC61A7",
                "OptanonAlertBoxClosed": "2025-07-8T01:39:25.556Z",
                "OptanonConsent": "isGpcEnabled=0&datestamp=Wed+Jul+8+2025+09%3A39%3A26+GMT%2B0800+(%E4%B8%AD%E5%9B%BD%E6%A0%87%E5%87%86%E6%97%B6%E9%97%B4)&version=202409.1.0&browserGpcFlag=0&isIABGlobal=false&hosts=&consentId=56be9072-385f-4e60-88b6-f4c1ca197892&interactionCount=1&isAnonUser=1&landingPath=NotLandingPage&groups=C0001%3A1%2CC0002%3A1%2CC0003%3A1%2CC0004%3A1&AwaitingReconsent=false&geolocation=%3B",
                "cf_clearance": "VSfNluKBYp2ilQ5bggjImEhf6BiTR2oSiEMPaYRLAR8-1752629970-1.2.1.1-QNZ5tpRU1WlbXWUSf1RJbEIXivlJAelPVszxKZlp1NsdbPY1SnvSQ_I2lymJmTH.OMYAwc0X3UmJfE6GqnbtioALoXyLjhALQ_ZhPXm6ajO3xII3Y16EM4iBx7Ixp716u4WtbfS9z2lLzp2JDDQSiijl1oqgc1w3KurnXB2eyaOhW3WFIO1yYnovpFqsvR6YSi7U4N80sH6VfCuL5RbtvlM3BUaX6NhJ.p4J31ym2dE",
                "_hjSession_864760": "eyJpZCI6ImVlNGU1MTE0LTAwZTktNGJjNC1iZDQyLTlkNmQwZmM5NjMwNCIsImMiOjE3NTI2Mjk5NjcxOTUsInMiOjAsInIiOjAsInNiIjowLCJzciI6MCwic2UiOjAsImZzIjowLCJzcCI6MH0=",
                "_ga_4819PJ6HEN": "GS2.1.s1752629967$o12$g0$t1752629967$j60$l0$h0",
                "_ga": "GA1.2.575950487.1752227647",
                "_gat_UA-3062505-46": "1",
                "_ga_0HYE8YG0M6": "GS2.1.s1752627891$o6$g1$t1752629969$j58$l0$h0"
            }
            # 使用curl_cffi的异步会话
            self.curl_async_session = curl_AsyncSession(impersonate="chrome120",proxies=self.proxies)
            self.curl_async_session.headers.update(headers)
            self.curl_async_session.cookies.update(cookies)
            page_url = f"https://www.tandfonline.com/action/doSearch?afterYear=2020&BeforeYear=2020&pageSize=10&subjectTitle=&startPage=1"
            try:
                response =await  self.curl_async_session.get(page_url, impersonate="chrome110", verify=self.ca_bundle_path,proxies=self.proxies)
            except BaseException as exc:
                async  with self.lock:
                    asyncio.sleep(1)
                continue
            if response.status_code == 200:
                break
            else:
                time.sleep(1)
        else:
            print("curl_async_session 刷新失败，请检查网络连接或代理设置。")


if __name__ == '__main__':
    hdfs_name = "science_cogentoa"
    year = 2025
    journal_info =  {
        "journal_name": "Renal Failure",
        "journal_value": "irnf20",
        "journal_num": 347
      }
    hdfs_handler = Curl_cffiFileUploader(
        hdfs_name=hdfs_name + f"/{year}/{journal_info['journal_name']}",
        impersonate="chrome110"
    )
    pdf_urls =['https://www.tandfonline.com/doi/pdf/10.1080/0886022X.2025.2479573?download=true', 'https://www.tandfonline.com/doi/pdf/10.1080/0886022X.2025.2479574?download=true', 'https://www.tandfonline.com/doi/pdf/10.1080/0886022X.2025.2479575?download=true', 'https://www.tandfonline.com/doi/pdf/10.1080/0886022X.2025.2480243?download=true', 'https://www.tandfonline.com/doi/pdf/10.1080/0886022X.2025.2480245?download=true', 'https://www.tandfonline.com/doi/pdf/10.1080/0886022X.2025.2480246?download=true', 'https://www.tandfonline.com/doi/pdf/10.1080/0886022X.2025.2480749?download=true', 'https://www.tandfonline.com/doi/pdf/10.1080/0886022X.2025.2480751?download=true', 'https://www.tandfonline.com/doi/pdf/10.1080/0886022X.2025.2481201?download=true', 'https://www.tandfonline.com/doi/pdf/10.1080/0886022X.2025.2481202?download=true', 'https://www.tandfonline.com/doi/pdf/10.1080/0886022X.2025.2482121?download=true', 'https://www.tandfonline.com/doi/pdf/10.1080/0886022X.2025.2482124?download=true', 'https://www.tandfonline.com/doi/pdf/10.1080/0886022X.2025.2482125?download=true', 'https://www.tandfonline.com/doi/pdf/10.1080/0886022X.2025.2482127?download=true', 'https://www.tandfonline.com/doi/pdf/10.1080/0886022X.2025.2482885?download=true', 'https://www.tandfonline.com/doi/pdf/10.1080/0886022X.2025.2482888?download=true', 'https://www.tandfonline.com/doi/pdf/10.1080/0886022X.2025.2483386?download=true', 'https://www.tandfonline.com/doi/pdf/10.1080/0886022X.2025.2483389?download=true', 'https://www.tandfonline.com/doi/pdf/10.1080/0886022X.2025.2483986?download=true', 'https://www.tandfonline.com/doi/pdf/10.1080/0886022X.2025.2483990?download=true', 'https://www.tandfonline.com/doi/pdf/10.1080/0886022X.2025.2484471?download=true', 'https://www.tandfonline.com/doi/pdf/10.1080/0886022X.2025.2484616?download=true', 'https://www.tandfonline.com/doi/pdf/10.1080/0886022X.2025.2485375?download=true', 'https://www.tandfonline.com/doi/pdf/10.1080/0886022X.2025.2485390?download=true', 'https://www.tandfonline.com/doi/pdf/10.1080/0886022X.2025.2485475?download=true', 'https://www.tandfonline.com/doi/pdf/10.1080/0886022X.2025.2486551?download=true', 'https://www.tandfonline.com/doi/pdf/10.1080/0886022X.2025.2486557?download=true', 'https://www.tandfonline.com/doi/pdf/10.1080/0886022X.2025.2486558?download=true', 'https://www.tandfonline.com/doi/pdf/10.1080/0886022X.2025.2486562?download=true', 'https://www.tandfonline.com/doi/pdf/10.1080/0886022X.2025.2486563?download=true', 'https://www.tandfonline.com/doi/pdf/10.1080/0886022X.2025.2486564?download=true', 'https://www.tandfonline.com/doi/pdf/10.1080/0886022X.2025.2486565?download=true', 'https://www.tandfonline.com/doi/pdf/10.1080/0886022X.2025.2486566?download=true', 'https://www.tandfonline.com/doi/pdf/10.1080/0886022X.2025.2486567?download=true', 'https://www.tandfonline.com/doi/pdf/10.1080/0886022X.2025.2486568?download=true', 'https://www.tandfonline.com/doi/pdf/10.1080/0886022X.2025.2486620?download=true', 'https://www.tandfonline.com/doi/pdf/10.1080/0886022X.2025.2487211?download=true', 'https://www.tandfonline.com/doi/pdf/10.1080/0886022X.2025.2487212?download=true', 'https://www.tandfonline.com/doi/pdf/10.1080/0886022X.2025.2488138?download=true', 'https://www.tandfonline.com/doi/pdf/10.1080/0886022X.2025.2488139?download=true', 'https://www.tandfonline.com/doi/pdf/10.1080/0886022X.2025.2488140?download=true', 'https://www.tandfonline.com/doi/pdf/10.1080/0886022X.2025.2488236?download=true', 'https://www.tandfonline.com/doi/pdf/10.1080/0886022X.2025.2488876?download=true', 'https://www.tandfonline.com/doi/pdf/10.1080/0886022X.2025.2489712?download=true', 'https://www.tandfonline.com/doi/pdf/10.1080/0886022X.2025.2489715?download=true', 'https://www.tandfonline.com/doi/pdf/10.1080/0886022X.2025.2489722?download=true', 'https://www.tandfonline.com/doi/pdf/10.1080/0886022X.2025.2490200?download=true', 'https://www.tandfonline.com/doi/pdf/10.1080/0886022X.2025.2490202?download=true', 'https://www.tandfonline.com/doi/pdf/10.1080/0886022X.2025.2490203?download=true', 'https://www.tandfonline.com/doi/pdf/10.1080/0886022X.2025.2491156?download=true']
    hdfs_handler.start_thread(pdf_urls)