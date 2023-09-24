# -*- coding:utf-8 -*-
# 君子爱财 取之YOODO!

# 爬取港交所数据

import sys

sys.path.append("./hkex")
from crawler import Crawler

sys.path.append("../../lib/utils/log")
from log import *


def usage():
    """ 展示帮助信息 """
    print("python3 ./main.py [stock|transaction]")
    print("     - stock: 爬取股票数据")
    print("     - transaction: 爬取交易数据")
    print("     - help: 展示帮助信息")


def crawl_stock(start_code):
    """ 爬取股票信息 """

    crawler = Crawler()

    crawler.crawl_stock(start_code)


def crawl_transaction(start_stock_code, lastest_day):
    """ 爬取交易信息 """

    crawler = Crawler()

    # 爬取恒生指数
    crawler.crawl_hsi_index()  # 恒生指数
    crawler.crawl_hz2083_index()  # 恒生科技指数

    # 爬取交易数据
    crawler.crawl_transaction(start_stock_code, lastest_day)


if __name__ == "__main__":
    # 校验参数
    if len(sys.argv) < 2:
        usage()
        exit(-1)

    # 日志初始化
    log_init("../../../log/crawler.log")

    func = sys.argv[1]
    if func == "stock":
        # 爬取股票信息
        start_code = sys.argv[2]

        crawl_stock(start_code)
    elif func == "transaction":
        # 爬取交易信息
        if len(sys.argv) != 4:
            print("Parameter is invalid!")
            print("python3 main.py transaction [start_stock_code] [start_date]")
            exit(-1)

        stock_code = sys.argv[2]  # 股票代码
        latest_day = sys.argv[3]  # 起始日期. 格式:1month, 3month, 6month, 1year, 2year

        crawl_transaction(stock_code, latest_day)

    else:
        usage()
