import schedule
import time
from pro1_arxiv_org.spider_arxiv_org_ai_new import spider_arxiv_org_ai_new

def run_spider_job():
    """封装爬虫任务的函数"""
    print("定时任务启动：开始运行 arXiv 爬虫...")
    try:
        spider = spider_arxiv_org_ai_new()
        spider.run()
        print("爬虫任务执行完毕。")
    except Exception as e:
        print(f"执行爬虫任务时发生错误: {e}")


if __name__ == "__main__":
    # 设置定时任务：每天凌晨 19:00 执行一次 run_spider_job 函数 也就是10点 上班的时候
    schedule.every().day.at("19:00").do(run_spider_job)
    print("爬虫定时器已启动，将在每天 02:30 自动运行。")
    print("请保持此窗口/进程在后台运行。")

    # 启动时立即执行一次，以便测试和立即获取数据
    run_spider_job()

    # 循环等待，直到预定的任务时间
    while True:
        schedule.run_pending()
        time.sleep(60)  # 每 60 秒检查一次是否有任务需要执行
