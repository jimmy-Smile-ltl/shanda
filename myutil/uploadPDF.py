# -*- coding: utf-8 -*-
# @Time    : 2025/6/19 13:24
# @Author  : Jimmy Smile
# @Project : 北大信研院
# @File    : uploadPDF.py
# @Software: PyCharm
import asyncio
import concurrent.futures
import os
import time
from pathlib import Path

import aiofiles
import aiohttp
import requests
from pathvalidate import sanitize_filename

from myutil.handleRequest import SingleRequestHandler


class PDFUploader:
    def __init__(self, hdfs_name: str, mode: str = 'thread', max_workers: int = 5, test_url=None, headers=None,
                 **kwargs):
        self.hdfs_name = hdfs_name
        if mode not in ['thread', 'async']:
            raise ValueError("mode must be 'thread' or 'async'")
        self.mode = mode
        self.kwargs = kwargs
        self.max_workers = max_workers
        self.hdfs_addr = 'http://10.0.102.75:9049/api/big_data/HdfsClient/uploadFile'
        self.hdfs_path = f'/{self.hdfs_name}'
        self.headers = headers if headers else {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
        }
        self.handler = SingleRequestHandler(test_url=test_url)

    def start(self, pdf_urls: list[str]):
        if self.mode == 'thread':
            self._start_thread(pdf_urls)
        elif self.mode == 'async':
            asyncio.run(self._start_async(pdf_urls))
        else:
            print("不支持的模式")

    def _start_thread(self, pdf_urls):
        # 并发处理所有PDF链接，并按进度输出信息。
        total_pdfs = len(pdf_urls)
        if total_pdfs == 0:
            print("没有需要处理的PDF链接。")
            return
        # 输出处理的进度
        completed_count = 0
        # 计算每多少个任务报告一次进度，至少为1
        progress_interval = max(1, total_pdfs // 10)
        # 使用字典将 future 映射回 url，这样出错时可以知道是哪个url
        # 这是一个比用列表更好的实践
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_url = {executor.submit(self._handle_one_pdf, url): url for url in pdf_urls}
            print(f"开始处理 {total_pdfs} 个PDF文件，使用 {self.max_workers} 个线程。")
            # as_completed 会在任务完成时立即返回 future
            for future in concurrent.futures.as_completed(future_to_url):
                url = future_to_url[future]
                try:
                    # 获取结果，如果任务中发生异常，这里会重新抛出
                    result = future.result()
                except Exception as e:
                    print(f"\n处理PDF时发生错误 (URL: {url}): {e}")
                completed_count += 1

                # 每完成一个区段的任务，或者全部完成时，打印进度
                if completed_count % progress_interval == 0 or completed_count == total_pdfs:
                    percentage = (completed_count / total_pdfs) * 100
                    # 使用 `\r` 和 `end=''` 可以在同一行刷新进度，看起来更美观
                    print(f"\r pdf 上传进度: {completed_count}/{total_pdfs} ({percentage:.2f}%)", end='\t\t')
        print(f"所有任务处理完成。")

    def _handle_one_pdf(self, pdf_url):
        try:
            if not pdf_url.lower().endswith('.pdf'):
                print(f"跳过非PDF链接: {pdf_url}")
                return
            retry_count = 5
            is_valid_pdf = False  # 标记是否下载到有效的PDF
            file_name = pdf_url.split('/')[-1].split('?')[0]  # 去除查询参数
            file_name = sanitize_filename(file_name)
            file_path = Path(file_name)
            for _ in range(retry_count):
                # 使用SingleRequestHandler下载PDF
                response = self.handler.fetch(pdf_url, method='GET', headers=self.headers, **self.kwargs)
                if not response or response.status_code != 200:
                    # print(f"下载失败:重试{_}次 pdf_url: {pdf_url}, status code: {response.status} ")
                    continue
                # 等于200也有问题 application/pdf;charset=utf-8 要兼容，不能直接相等
                if response.headers.get('Content-Type').find('pdf') == -1:
                    # print(f"下载的文件不是PDF: {pdf_url}")
                    continue
                if response.text.startswith("<script>") or response.text.startswith("<!DOCTYPE"):
                    # print(f"下载的文件是HTML页面而不是PDF: {pdf_url}")
                    continue
                if response.headers.get("content-length") == '0':
                    # print(f"下载的文件内容为空: {pdf_url}")
                    continue
                else:
                    # 都不是问题，检查PDF文件是否有效，大概率是我想要的PDF文件
                    is_valid_pdf = True
                    break
            if not is_valid_pdf:
                print(f"下载的文件无效或不是PDF: {pdf_url}")
                return
            file_path.parent.mkdir(parents=True, exist_ok=True)
            with open(file_path, 'wb') as f:
                f.write(response.content)
                f.close()
            # 上传到HDFS
            self._upload_file_to_hdfs(file_path)
            # 删除本地文件
            for _ in range(3):
                try:
                    file_path.unlink()
                    break
                except PermissionError:
                    time.sleep(0.2)
        except Exception as e:
            print(f"PDF处理失败，url: {pdf_url}, error: {e}")

    async def _start_async(self, pdf_urls):
        tasks = [self._handle_one_pdf_async(url) for url in pdf_urls]
        await asyncio.gather(*tasks)

    async def _handle_one_pdf_async(self, pdf_url):
        try:
            retry_count = 5
            is_valid_pdf = False  # 标记是否下载到有效的PDF
            file_name = pdf_url.split('/')[-1].split('?')[0]  # 去除查询参数
            file_name = sanitize_filename(file_name)
            file_path = Path(file_name)
            if not file_name.lower().endswith('.pdf'):
                return
            # 异步下载PDF
            for _ in range(retry_count):
                async with aiohttp.ClientSession() as session:
                    async with session.get(pdf_url, timeout=20) as resp:
                        if resp.status != 200:
                            print(f"下载失败:重试{_}次 pdf_url: {pdf_url}, status code: {resp.status} ")
                            continue
                        content = await resp.read()
                        file_path.parent.mkdir(parents=True, exist_ok=True)
                        async with aiofiles.open(file_path, 'wb') as f:
                            await f.write(content)
            # 检查下载的文件是否是有效的PDF
            if not file_path.exists() or file_path.stat().st_size == 0:
                print(f"下载的文件无效或大小为0: {pdf_url}")
                return

            # 上传到HDFS（同步，放到线程池）
            await asyncio.to_thread(self._upload_file_to_hdfs, file_path)
            # 删除本地文件
            file_path.unlink()
        except Exception as e:
            print(f"PDF处理失败，url: {pdf_url}, error: {e}")

    def _upload_file_to_hdfs(self, file: Path):
        for i in range(5):
            with open(str(file), 'rb') as f:
                files = [('file', (file.name, f))]
                data = {'parentPath': self.hdfs_path, 'type': '2'}
                response = requests.post(self.hdfs_addr, data=data, files=files)
                if response.status_code != 200:
                    raise ConnectionError(
                        f'hdfs upload failed, error code: {response.status_code}, message: {response.content}')
                if response.json().get('code') == "F0107" and response.json().get('message') == "创建文件目录失败":
                    print(f"创建文件目录失败: {response.json()}")
                else:
                    # print(f"文件上传成功: {file.name}")
                    break

    # 把当前文件夹下面的pdf全部上传
    def upload_all_pdfs(self, path):
        pdf_files = list(Path(path).glob('**/*.pdf'))
        if not pdf_files:
            # print("当前目录下没有PDF文件。")
            return
        print(f"找到 {len(pdf_files)} 个PDF文件，开始上传...")
        for pdf_file in pdf_files:
            if not os.path.exists(pdf_file):
                continue
            try:
                file_path = Path(pdf_file)
                self._upload_file_to_hdfs(pdf_file)
                print(f"已上传: {pdf_file.name}")
                for _ in range(5):
                    try:
                        pdf_file.unlink()
                        break
                    except PermissionError:
                        time.sleep(0.2)
            except Exception as e:
                print("上传本地pdf失败，",str(e))

