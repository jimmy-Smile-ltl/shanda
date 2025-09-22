# -*- coding: utf-8 -*-
# @Time    : 2025/6/19 13:22
# @Author  : Jimmy Smile
# @Project : 北大信研院
# @File    : uploadPic.py
# @Software: PyCharm
# -*- coding: utf-8 -*-
# @Time    : 2025/5/30 15:47
# @Author  : Jimmy Smile
# @Project : 北大信研院
# @File    : downUpDelete.py
# @Software: PyCharm
import asyncio
import concurrent.futures
import time
from pathlib import Path

import aiofiles
import aiohttp
import requests


def handleRequest(url, proxies=None):
    # 这个是获取一天的所有新闻，但是新闻内容是摘要，前面几行，全部内容需要去详情页面
    count = 0
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
    }
    max_rettry = 10
    while True:
        count += 1
        if count > max_rettry:
            print(url + "多次访问失败=========================")
            return None
        try:
            res = requests.get(url, headers=headers, verify=False, proxies=proxies)
            if res.status_code == 200:
                return res
        except Exception as e:
            if max_rettry == count:
                print(url)
                print("详情访问问题" + str(e))


# 图片高清的时候，下载太慢了，怎么办呢？

class downUpDelete():
    def __init__(self, tableName: str):
        self.tableName = tableName
        self.description = "下载图片，上传到HDFS，并删除本地文件"

    def start(self, image_urls):
        """执行下载、上传和删除操作"""
        self.download_and_upload_images(image_urls)

    ## 两部分代码 合并 下载---》上传---》 本地删除
    def download_and_upload_images(self, image_urls: list):
        for image_url in image_urls:
            try:
                # 判断是不是正常的URL
                if not image_url.startswith(('http://', 'https://')):
                    print(f"无效的URL: {image_url}")
                    continue
                # 判断是不是图片链接，需要考虑带参数的情况 ，改为是否包含图片文件后
                file_name = self.tableName + "_" + image_url.split('/')[-1]  # 从URL中提取文件名
                file_path = Path(file_name)
                # 下载图片
                picRes = handleRequest(image_url)
                # 确保目录存在
                file_path.parent.mkdir(parents=True, exist_ok=True)
                with open(file_path, 'wb') as f:
                    f.write(picRes.content)
                # print("图片已保存到:", file_path, end="\t")
                # 上传到HDFS
                res = self.upload_file_to_hdfs(file_path)
                # print(res.json()["message"], end="\t")
                # 删除本地文件
                file_path.unlink()
                ##
                # print(f"已删除。")
            except Exception as e:
                print(f"图片处理失败，image_url: {image_url}")

    # 文件需要上传到hdfs集群，所以是要先下载到本地吗？
    def upload_file_to_hdfs(self, file: Path):
        HDFS_PATH = f'/test/{self.tableName}'
        HDFS_UPLOAD_ADDR = 'http://10.0.102.75:9049/api/big_data/HdfsClient/uploadFile'
        files = [('file', (file.name, open(str(file), 'rb')))]
        data = {'parentPath': HDFS_PATH, 'type': '2'}
        response = requests.request('POST', HDFS_UPLOAD_ADDR, headers={}, data=data, files=files)
        if response.status_code != 200:
            raise ConnectionError(
                f'hdfs upload failed, error code: {response.status_code}, message: {response.content}')
        return response


class downUpDeleteProxy():
    def __init__(self, tableName: str, proxire: dict):
        self.tableName = tableName
        self.proxies = proxire
        self.description = "下载图片，上传到HDFS，并删除本地文件"

    def start(self, image_urls):
        """执行下载、上传和删除操作"""
        self.download_and_upload_images(image_urls)

    def set_proxies(self, proxies: dict):
        """设置代理"""
        self.proxies = proxies

    ## 两部分代码 合并 下载---》上传---》 本地删除
    def download_and_upload_images(self, image_urls: list):
        for image_url in image_urls:
            try:
                # 判断是不是正常的URL
                if not image_url.startswith(('http://', 'https://')):
                    print(f"无效的URL: {image_url}")
                    continue
                # 判断是不是图片链接，需要考虑带参数的情况 ，改为是否包含图片文件后
                file_name = self.tableName + "_" + image_url.split('/')[-1]  # 从URL中提取文件名
                file_path = Path(file_name)
                # 下载图片
                picRes = handleRequest(image_url, self.proxies)
                # 确保目录存在
                file_path.parent.mkdir(parents=True, exist_ok=True)
                with open(file_path, 'wb') as f:
                    f.write(picRes.content)
                    f.close()  # 确保文件及时关闭
                # print("图片已保存到:", file_path, end="\t")
                # 上传到HDFS
                res = self.upload_file_to_hdfs(file_path)
            except Exception as e:
                print(f"图片处理失败，image_url: {image_url},报错信息: {e}")

    # 文件需要上传到hdfs集群，所以是要先下载到本地吗？
    def upload_file_to_hdfs(self, file: Path):
        HDFS_PATH = f'/test/{self.tableName}'
        HDFS_UPLOAD_ADDR = 'http://10.0.102.75:9049/api/big_data/HdfsClient/uploadFile'
        with open(str(file), 'rb') as f:  # 用 with 保证文件及时关闭
            files = [('file', (file.name, f))]
            data = {'parentPath': HDFS_PATH, 'type': '2'}
            response = requests.request('POST', HDFS_UPLOAD_ADDR, headers={}, data=data, files=files)
            if response.status_code != 200:
                raise ConnectionError(
                    f'hdfs upload failed, error code: {response.status_code}, message: {response.content}')
            f.close()  # 确保文件及时关闭
            # 删除本地文件
            file.unlink()
            return response


# 异步版本
class downUpDeleteAsync():
    def __init__(self, tableName: str):
        self.tableName = tableName
        self.description = "异步下载图片，上传到HDFS，并删除本地文件"

    async def start(self, image_urls):
        await self.download_and_upload_images(image_urls)

    async def download_and_upload_images(self, image_urls):
        tasks = [self.handle_one_image(image_url) for image_url in image_urls]
        await asyncio.gather(*tasks)

    async def handle_one_image(self, image_url):
        try:
            if not image_url.startswith(('http://', 'https://')):
                print(f"无效的URL: {image_url}")
                return
            pic_suffixes = ('.png', '.jpg', '.jpeg', '.gif', '.bmp')
            if not any(image_url.lower().endswith(suffix) for suffix in pic_suffixes):
                return
            file_name = self.tableName + "_" + image_url.split('/')[-1]
            file_path = Path(file_name)
            # 异步下载图片
            async with aiohttp.ClientSession() as session:
                async with session.get(image_url) as resp:
                    if resp.status != 200:
                        print(f"下载失败: {image_url}")
                        return
                    content = await resp.read()
            file_path.parent.mkdir(parents=True, exist_ok=True)
            async with aiofiles.open(file_path, 'wb') as f:
                await f.write(content)
            # 上传到HDFS（同步，放到线程池）
            await asyncio.to_thread(self.upload_file_to_hdfs, file_path)
            # 删除本地文件
            file_path.unlink()
        except Exception as e:
            print(f"图片处理失败，image_url: {image_url}, error: {e}")

    def upload_file_to_hdfs(self, file: Path):
        import requests
        HDFS_PATH = f'/test/{self.tableName}'
        HDFS_UPLOAD_ADDR = 'http://10.0.102.75:9049/api/big_data/HdfsClient/uploadFile'
        files = [('file', (file.name, open(str(file), 'rb')))]
        data = {'parentPath': HDFS_PATH, 'type': '2'}
        response = requests.request('POST', HDFS_UPLOAD_ADDR, headers={}, data=data, files=files)
        if response.status_code != 200:
            raise ConnectionError(
                f'hdfs upload failed, error code: {response.status_code}, message: {response.content}')
        return response


class downUpDeleteThread:
    def __init__(self, tableName: str, max_workers: int = 5):
        self.tableName = tableName
        self.description = "多线程下载图片，上传到HDFS，并删除本地文件"
        self.max_workers = max_workers

    def start(self, image_urls):
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [executor.submit(self.handle_one_image, url) for url in image_urls]
            concurrent.futures.wait(futures)
            # 遇到报错
            for future in futures:
                if future.exception() is not None:
                    continue
                    # print(f"处理图片时发生异常: {future.exception()}")
        if len(image_urls) > 0:
            print(f"已处理 {len(image_urls)} 张图片。", end="\t")
        else:
            print("没有图片需要处理。", end="\t")

    def handle_one_image(self, image_url):
        try:
            if not image_url.startswith(('http://', 'https://')):
                print(f"无效的URL: {image_url}")
                return
            pic_suffixes = ('.png', '.jpg', '.jpeg', '.gif', '.bmp')
            if not any(image_url.lower().endswith(suffix) for suffix in pic_suffixes):
                return
            file_name = self.tableName + "_" + image_url.split('/')[-1]
            file_path = Path(file_name)
            # 下载图片
            picRes = handleRequest(image_url)
            file_path.parent.mkdir(parents=True, exist_ok=True)
            with open(file_path, 'wb') as f:
                f.write(picRes.content)
                f.flush()  # 确保文件内容写入磁盘
                f.close()  # 确保文件及时关闭
            # 上传到HDFS
            self.upload_file_to_hdfs(file_path)
            # 删除本地文件
            for _ in range(3):
                try:
                    file_path.unlink()
                    break
                except PermissionError:
                    time.sleep(0.2)
        except Exception as e:
            print(f"图片处理失败，image_url: {image_url}, error: {e}")

    def upload_file_to_hdfs(self, file: Path):
        HDFS_PATH = f'/test/{self.tableName}'
        HDFS_UPLOAD_ADDR = 'http://10.0.102.75:9049/api/big_data/HdfsClient/uploadFile'
        with open(str(file), 'rb') as f:  # 用 with 保证文件及时关闭
            files = [('file', (file.name, f))]
            data = {'parentPath': HDFS_PATH, 'type': '2'}
            response = requests.request('POST', HDFS_UPLOAD_ADDR, headers={}, data=data, files=files)
        if response.status_code != 200:
            raise ConnectionError(
                f'hdfs upload failed, error code: {response.status_code}, message: {response.content}')
        return response
