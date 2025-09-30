# 代理主机: eu.smartproxycn.com
# 端口: 1000
# 账户名: v2odzv8d5ie1_session-AZSW
# 密码: yYtrMA92WokObeuB
import requests


class ProxyUtil():
    def __init__(self, test_url=None, headers=None, cookies=None):
        self.test_url = test_url
        self.cookies = cookies
        self.user_name = "v2odzv8d5ie1_session-AZSW"
        self.pwd = "yYtrMA92WokObeuB"
        self.port = "1000"
        self.host = "eu.smartproxycn.com"
        if headers is None:
            self.headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
            }
    def get_proxy(self):
        return  self.get_proxy_tunel()
    def get_proxy_tunel(self):
        return {
            'http': f'http://{self.user_name}:{self.pwd}@{self.host}:{self.port}',
            'https': f'http://{self.user_name}:{self.pwd}@{self.host}:{self.port}'
        }
    def test_tunel(self,url):
        try:
            print(f"正在测试代理: {url}")
            response = requests.get(url, headers=self.headers, cookies=self.cookies, timeout=10, proxies=self.get_proxy())
            if response.status_code == 200:
                print(f"代理测试成功: {response.text[:200]}")
                return True
            else:
                print(f"代理测试失败，状态码: {response.status_code}")
                return False
        except Exception as e:
            print(f"代理测试异常: {e}")
            return False

if __name__ == "__main__":

    proxy_util = ProxyUtil()
    test_url = "http://httpbin.org/ip"
    proxy_util.test_tunel(test_url)
    test_url = "https://www.google.com/"
    proxy_util.test_tunel(test_url)
    test_url = "https://scholar.google.com/"
    proxy_util.test_tunel(test_url)