# -*- coding: utf-8 -*-
# @Time    : 2025/6/27 13:28
# @Author  : Jimmy Smile
# @Project : 北大信研院下载失败
# @Software: PyCharm
# -*- coding: utf-8 -*-
# @Time    : 2025/6/19 15:00
# @Author  : Jimmy Smile
# @Project : 北大信研院
# @File    : uploadImage.py
# @Software: PyCharm

import asyncio
import concurrent.futures
import re
import time
from io import BytesIO
from pathlib import Path

import aiohttp
import requests
from pathvalidate import sanitize_filename

from myutil.handleRequest import SingleRequestHandler, CurlRequestHandler


class DailyUploader:
    """
    一个用于下载网络图片并上传到HDFS的工具类。
    支持多线程（'thread'）和异步（'async'）两种模式。
    """

    def __init__(self, hdfs_name: str, mode: str = 'thread', max_workers: int = 5, test_url=None, headers=None,
                 curl=False,
                 **kwargs):
        """
        初始化DailyUploader。
        :param hdfs_name: HDFS上的目标目录名，也用作文件名前缀。
        :param mode: 'thread' (多线程) 或 'async' (异步)。
        :param max_workers: 线程池或并发任务的最大数量。
        :param test_url: 用于SingleRequestHandler的测试URL。
        :param headers: 自定义请求头。
        """
        self.hdfs_name = hdfs_name
        if mode not in ['thread', 'async']:
            raise ValueError("mode 必须是 'thread' 或 'async'")
        self.mode = mode
        self.kwargs = kwargs
        self.max_workers = max_workers
        self.hdfs_addr = 'http://10.0.102.75:9049/api/big_data/HdfsClient/uploadFile'
        if not hdfs_name.startswith('/daily'):
            self.hdfs_path = f'/daily/{self.hdfs_name}'
        self.headers = headers if headers else {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        if not curl:
            self.handler = SingleRequestHandler(test_url=test_url)
        else:
            self.handler = CurlRequestHandler(test_url=test_url)

    def start(self, image_urls: list[str]):
        """
        启动图片处理流程。
        :param image_urls: 包含图片URL的列表。
        """
        if self.mode == 'thread':
            self._start_thread(image_urls)
        elif self.mode == 'async':
            asyncio.run(self._start_async(image_urls))
        else:
            print("不支持的模式")

    def _start_thread(self, image_urls):
        """使用线程池并发处理图片。"""
        total_images = len(image_urls)
        if total_images == 0:
            print("没有需要处理的图片链接。")
            return
        completed_count = 0
        error_count = 0
        progress_interval = max(1, total_images // 10)
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_url = {executor.submit(self._handle_one_image, url): url for url in image_urls}
            print(f"开始处理 {total_images} 个图片文件，使用 {self.max_workers} 个线程。")
            for future in concurrent.futures.as_completed(future_to_url):
                url = future_to_url[future]
                try:
                    if future.result():
                        completed_count += 1
                    else:

                        error_count += 1
                except Exception as e:
                    print(f"处理图片时发生错误 (URL: {url}): {e}")

                if completed_count % progress_interval == 0 or completed_count == total_images:
                    percentage = (completed_count / total_images) * 100
                    print(f"\r图片上传 进度: {completed_count}/{total_images} ({percentage:.2f}%)  失败：{error_count}", end='')
        print("所有图片任务处理完成。")

    def _handle_one_image(self, image_url: str):
        """下载、上传并删除单个图片（同步）。"""
        try:
            # 1. 验证URL
            if not image_url.startswith(('http://', 'https://')):
                # print(f"无效的URL协议: {image_url}")
                return
            pic_suffixes = [
                # Images
                'png', 'jpg', 'jpeg', 'gif', 'bmp', 'svg', 'webp', "tif",
                # Audio
                'mp3', 'wav', 'ogg', 'flac',
                # Video
                'mp4', 'webm', 'mov', 'avi',
                # Documents
                'pdf', 'doc', 'docx', 'xls', 'xlsx', 'ppt', 'pptx',
                # Archives
                'zip', 'rar', '7z', 'tar', 'gz'
            ]
            extensions_pattern = '|'.join(pic_suffixes)
            file_pattern = re.compile(rf'\.({extensions_pattern})(\.\d+)?$', re.IGNORECASE)
            # 3. 验证URL是否是支持的文件类型
            url_without_params = image_url.lower().split('?')[0]
            match = file_pattern.search(url_without_params)
            if not (match and match.group(1)):  # group(2) 对应 (\.\d+)?
                print(image_url, "errror 文件格式后缀不在目录")

            # 2. 下载图片

            res = self.handler.fetch(image_url, headers=self.headers, **self.kwargs)
            if not res:
                # raise ConnectionError(f"下载失败")
                return  False

            # 3. 保存到本地
            original_filename = image_url.split('/')[-1].split('?')[0]
            safe_filename = sanitize_filename(original_filename)
            # 要使用的正则表达式 .2 这种结尾，是不对的
            pattern = r'\.\d+$'
            file_name = re.sub(pattern, '', safe_filename)
            for attempt in range(3):
                try:
                    self._upload_content_to_hdfs(content=res.content, file_name=file_name)
                    return True
                    break  # 成功删除后跳出循环
                except Exception as e:
                    # print(f"删除本地文件失败 (尝试 {attempt + 1}/3): {e}")
                    pass
        except Exception as e:
            print(f"处理图片时发生错误 (URL: {image_url})")
            # file_path = Path(file_name)
            #
            # with open(file_path, 'wb') as f:
            #     f.write(res.content)
            #
            # # 4. 上传到HDFS
            # self._upload_file_to_hdfs(file_path)
            # # 5. 删除本地文件
            # for attempt in range(3):
            #     try:
            #         if file_path.exists():
            #             file_path.unlink()
            #         break  # 成功删除后跳出循环
            #     except Exception as e:
            #         # print(f"删除本地文件失败 (尝试 {attempt + 1}/3): {e}")
            #         pass

        # except Exception as e:
        #     # 确保在发生错误时，如果文件已创建，则尝试删除
        #     if 'file_path' in locals() and file_path.exists():
        #         file_path.unlink()
        #     raise e  # 重新抛出异常，以便上层捕获并报告

    async def _start_async(self, image_urls: list[str]):
        """使用asyncio并发处理图片。"""
        tasks = [self._handle_one_image_async(url) for url in image_urls]
        await asyncio.gather(*tasks)
        print("所有图片任务处理完成。")

    async def _handle_one_image_async(self, image_url: str):
        """下载、上传并删除单个图片（异步）。"""
        try:
            # 1. 验证URL
            if not image_url.startswith(('http://', 'https://')):
                # print(f"无效的URL协议: {image_url}")
                return
            pic_suffixes = [
                # Images
                'png', 'jpg', 'jpeg', 'gif', 'bmp', 'svg', 'webp', "tif",
                # Audio
                'mp3', 'wav', 'ogg', 'flac',
                # Video
                'mp4', 'webm', 'mov', 'avi',
                # Documents
                'pdf', 'doc', 'docx', 'xls', 'xlsx', 'ppt', 'pptx',
                # Archives
                'zip', 'rar', '7z', 'tar', 'gz'
            ]
            extensions_pattern = '|'.join(pic_suffixes)
            file_pattern = re.compile(rf'\.({extensions_pattern})(\.\d+)?$', re.IGNORECASE)
            # 3. 验证URL是否是支持的文件类型
            url_without_params = image_url.lower().split('?')[0]
            match = file_pattern.search(url_without_params)
            if match and match.group(2):  # group(2) 对应 (\.\d+)?
                url_without_params = url_without_params.removesuffix(match.group(2))
            pic_suffixes = ('.png', '.jpg', '.jpeg', '.gif', '.bmp', '.webp')
            if not any(url_without_params.endswith(suffix) for suffix in pic_suffixes):
                # print(f"非标准图片URL后缀: {image_url}")
                return

            # 2. 异步下载图片
            async with aiohttp.ClientSession(headers=self.headers) as session:
                async with session.get(image_url, timeout=30) as resp:
                    if resp.status != 200:
                        raise ConnectionError(f"下载失败，状态码: {resp.status}")
                    content = await resp.read()

            # 3. 异步保存到本地
            original_filename = image_url.split('/')[-1].split('?')[0]
            safe_filename = sanitize_filename(original_filename)
            pattern = r'\.\d+$'
            file_name = re.sub(pattern, '', safe_filename)
            for attempt in range(3):
                try:
                    self._upload_content_to_hdfs(content=content, file_name=file_name)
                    break  # 成功删除后跳出循环
                except Exception as e:
                    # print(f"删除本地文件失败 (尝试 {attempt + 1}/3): {e}")
                    pass

            # file_path = Path(file_name)
            # async with aiofiles.open(file_path, 'wb') as f:
            #     await f.write(content)
            #
            # # 4. 上传到HDFS（同步操作，在线程池中运行）
            # await asyncio.to_thread(self._upload_file_to_hdfs, file_path)
            #
            # # 5. 删除本地文件
            # file_path.unlink()

        except Exception as e:
            # if 'file_path' in locals() and file_path.exists():
            #     file_path.unlink()
            print(f"图片处理失败 (URL: {image_url}): {e}")

    def _upload_file_to_hdfs(self, file: Path):
        """将单个文件上传到HDFS，支持重试。"""
        for attempt in range(3):  # 最多重试3次
            try:
                with open(str(file), 'rb') as f:
                    files = {'file': (file.name, f)}
                    data = {'parentPath': self.hdfs_path, 'type': '2'}
                    response = requests.post(self.hdfs_addr, data=data, files=files, timeout=60)

                if response.status_code == 200:
                    return response
                else:
                    raise ConnectionError(f'HDFS上传失败，状态码: {response.status_code}, 消息: {response.text}')
            except Exception as e:
                print(f"上传HDFS失败 (尝试 {attempt + 1}/3): {e}")
                if attempt < 2:
                    time.sleep(2)  # 等待2秒后重试
                else:
                    raise  # 最后一次尝试失败后，抛出异常

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

    # 上传本地图片到HDFS，当前路径下所有图片，在删除
    def upload_local_images(self, local_dir: str):
        """
        上传指定目录下的所有图片到HDFS。
        :param local_dir: 本地目录路径。
        """
        local_path = Path(local_dir)
        if not local_path.is_dir():
            print(f"指定的路径不是一个目录: {local_dir}")
            return

        image_files = list(local_path.glob('*.*'))
        if not image_files:
            print("目录中没有找到图片文件。")
            return

        for file in image_files:
            if file.suffix.lower() in ('.png', '.jpg', '.jpeg', '.gif', '.bmp', '.webp'):
                self._upload_file_to_hdfs(file)
                try:
                    file.unlink()  # 删除本地文件
                except Exception as e:
                    print(f"删除本地文件失败: {e}")
