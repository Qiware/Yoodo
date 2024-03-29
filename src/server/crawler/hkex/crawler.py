# -*- coding:utf-8 -*-
# 君子爱财 取之YOODO!

# 爬取港交所数据

import sys
import json
import time
import logging

from hkex import *
from const import *

sys.path.append("../../lib/stock")
from stock import get_stock_code

sys.path.append("../../lib/utils/thread_pool")
from thread_pool import ThreadPool

sys.path.append("../../repo/database")
from database import Database

# 港交所KEY
HKEX_EXCHAGE_KEY = "hkex"

# 爬取交易数据返回字段序列
HKEX_TRANSACTION_TIMESTAMP = 0  # 交易时间戳
HKEX_TRANSACTION_OPEN_PRICE = 1  # 开盘价
HKEX_TRANSACTION_TOP_PRICE = 2  # 最高价
HKEX_TRANSACTION_BOTTOM_PRICE = 3  # 最低价
HKEX_TRANSACTION_CLOSE_PRICE = 4  # 收盘价
HKEX_TRANSACTION_VOLUME = 5  # 交易量
HKEX_TRANSACTION_TURNOVER = 6  # 交易额

# 消息类型
TYPE_STOCK_DATA = 1  # 股票数据
TYPE_STOCK_CODE = 2  # 股票代码

# 线程池配置
CRAWL_WORKER_NUM = 1  # 爬取线程数
CRAWL_QUEUE_CAPACITY = 1000  # 队列容量

# 爬虫服务
class Crawler:
    def __init__(self):
        """ 初始化处理 """
        self.hkex = HKEX()
        self.database = Database()

        self.workers = ThreadPool(CRAWL_QUEUE_CAPACITY, CRAWL_WORKER_NUM)
        self.workers.register(TYPE_STOCK_DATA, self._crawl_transaction)
        self.workers.register(TYPE_STOCK_CODE, self.crawl_stock_by_stock_code)

    def gen_date(self, year, month, mday):
        """ 生成日期: YYYYMMDD """

        return "%04d%02d%02d" % (year, month, mday)

    def gen_stock_key(self, exchange_code, stock_code):
        """ 生成股票KEY """
        return "%s:%05d" % (exchange_code, int(stock_code))

    def gen_index_key(self, exchange_code, index_code):
        """ 生成指数KEY """
        return "%s:%s" % (exchange_code, index_code)

    def crawl_stock_by_stock_code(self, stock_code):
        """ 爬取指定股票信息 """

        # 爬取股票数据
        data = self.hkex.get_stock(stock_code)
        if len(data) == 0:
            logging.error("Get stock data failed! stock_code:%s", stock_code)
            return None

        # 提取有效信息
        stock = dict()
        stock["stock_key"] = self.gen_stock_key(HKEX_EXCHAGE_KEY, data["stock_code"])  # 股票KEY
        stock["name"] = str(data["name"])  # 股票名称
        stock["total"] = int(data["total"])  # 总股本数量
        stock["market_cap"] = float(data["market_cap"])  # 总市值
        stock["product_type"] = data["product_type"]  # 产品类型
        if "first_classification" in data.keys():
            stock["first_classification"] = data["first_classification"]  # 一级分类
        if "second_classification" in data.keys():
            stock["second_classification"] = data["second_classification"]  # 二级分类
        if "introduction" in data.keys():
            stock["introduction"] = data["introduction"]  # 公司介绍

        timestamp = int(time.time())
        stock["create_time"] = time.localtime(timestamp)
        stock["update_time"] = time.localtime(timestamp)

        # 更新股票信息
        return self.database.set_stock(stock)

    def crawl_stock(self, start=HKEX_STOCK_CODE_MIN, end=HKEX_STOCK_CODE_MAX):
        """ 爬取全部股票信息 """
        stock_code = int(start)
        while stock_code <= int(end):
            print("Crawl stock data. stock_code:%s" % stock_code)
            self.workers.bpush(TYPE_STOCK_CODE, stock_code)
            stock_code += 1
        self.workers.wait()

    def gen_transaction(self, stock_code, stock_data, data):
        """ 生成交易数据
            @Param stock_code: 股票代码
            @Param stock_data: 股票信息
            @Param data: 交易数据
        """
        transaction = dict()

        # 股票KEY
        transaction["stock_key"] = self.gen_stock_key(HKEX_EXCHAGE_KEY, stock_code)

        # 交易日期(YYYYMMDD)
        transaction_timestamp = int(data["timestamp"])
        transaction_date = time.localtime(transaction_timestamp)

        transaction["date"] = self.gen_date(
            transaction_date.tm_year,
            transaction_date.tm_mon,
            transaction_date.tm_mday)

        # 开盘价
        if data["open_price"] is None:
            logging.error("Open price is none! date:%s stock_code:%s open_price:%f",
                          transaction["date"], stock_code, transaction["open_price"])
            return None

        transaction["open_price"] = float(data["open_price"])

        # 收盘价
        if data["close_price"] is None:
            logging.error("Close price is none! date:%s stock_code:%s close_price:%f",
                          transaction["date"], stock_code, transaction["close_price"])
            return None

        transaction["close_price"] = float(data["close_price"])

        # 最高价
        if data["top_price"] is None:
            logging.error("Top price is none! date:%s stock_code:%s top_price:%f",
                          transaction["date"], stock_code, transaction["top_price"])
            return None

        transaction["top_price"] = float(data["top_price"])

        # 最低价
        if data["bottom_price"] is None:
            logging.error("Bottom price is none! date:%s stock_code:%s bottom_price:%f",
                          transaction["date"], stock_code, transaction["bottom_price"])
            return None

        transaction["bottom_price"] = float(data["bottom_price"])

        # 交易量
        if data["volume"] is None:
            logging.error("Volume is none! date:%s stock_code:%s volume:%d",
                          transaction["date"], stock_code, transaction["volume"])
            return None

        transaction["volume"] = int(data["volume"])
        if transaction["volume"] <= 0:
            logging.error("Volume is none! date:%s stock_code:%s volume:%d",
                          transaction["date"], stock_code, transaction["volume"])
            return None

        # 交易额
        if data["turnover"] is None:
            logging.error("Turnover is invalid! date:%s stock_code:%s turnover:%f",
                          transaction["date"], stock_code, transaction["turnover"])
            return None

        transaction["turnover"] = float(data["turnover"])
        if transaction["turnover"] <= 0:
            logging.error("Turnover is invalid! date:%s stock_code:%s turnover:%d",
                          transaction["date"], stock_code, transaction["turnover"])
            return None

        # 换手率 = 交易量 / 总股本数
        if data["turnover_ratio"] is None:
            logging.error("Turnover ratio is none! date:%s stock_code:%s turnover_ratio:%f",
                          transaction["date"], stock_code, transaction["turnover_ratio"])
            return None

        if stock_data["total"] == 0:
            logging.error("Total of stock is invalid! date:%s stock_code:%s",
                          transaction["date"], stock_code)
            return None

        transaction["turnover_ratio"] = transaction["volume"] / stock_data["total"] * 100

        # 创建时间
        curr_timestamp = int(time.time())
        transaction["create_time"] = time.localtime(curr_timestamp)
        # 更新时间
        transaction["update_time"] = time.localtime(curr_timestamp)

        return transaction

    def _crawl_transaction(self, stock_data):
        """ 爬取指定股票交易信息
            @Param stock_code: 股票代码(类型: int. 如: 00700)
        """
        latest_day = HKEX_LASTEST_3MONTH
        exchange, stock_code = get_stock_code(stock_data["stock_key"])

        print("Crawl transaction. stock_code:%s" % stock_code)

        # 爬取交易数据
        data_list = self.hkex.get_kline_from_hkex(int(stock_code), latest_day)
        if len(data_list) == 0:
            logging.error("Get hkex kline failed! stock_code:%s latest_day:%s",
                          stock_code, latest_day)
            return None

        # 遍历交易数据
        for data in data_list:
            # 提取交易信息
            transaction = self.gen_transaction(int(stock_code), stock_data, data)
            if transaction is None:
                logging.error("Gen transaction failed! stock_code:%s ", stock_code)
                continue

            # 更新交易信息
            self.database.set_transaction(transaction)

        # 更新股票市值
        self.update_stock_market_cap(stock_data, data_list[-1]["close_price"])

        return None

    def update_stock_market_cap(self, stock, close_price):
        """ 更新股票市值
            @Param stock: 股票信息
            @Param close_price: 最新收盘价
        """
        latest_stock_data = dict()

        latest_stock_data["stock_key"] = stock["stock_key"]
        latest_stock_data["market_cap"] = stock["total"] * close_price

        self.database.set_stock(latest_stock_data)

    def crawl_all_transaction(self):
        """ 爬取交易信息
            @Param stock_code: 股票代码
            @Param latest_day: 开始日期. 格式: 1month, 6month, 1year, 2year
        """

        # 获取股票列表
        stock_list = self.database.get_all_stock()
        if len(stock_list) == 0:
            logging.error("Get stock list failed!")
            return

        # 放入爬取队列
        for stock in stock_list:
            self.workers.bpush(TYPE_STOCK_DATA, stock)

        # 等待处理结束
        self.workers.wait()

    def crawl_hsi_index(self):
        """ 获取'恒生指数'数据 """
        klines = self.hkex.get_hsi_kline()
        for kline in klines:
            # 生成指数数据
            transaction = self.gen_index_transaction(INDEX_CODE_HSI, kline)

            # 更新交易信息
            self.database.set_transaction(transaction)

    def crawl_hz2083_index(self):
        """ 获取'恒生科技指数'数据 """
        klines = self.hkex.get_hz2083_kline()
        for kline in klines:
            # 生成指数数据
            transaction = self.gen_index_transaction(INDEX_CODE_HZ2083, kline)

            # 更新交易信息
            self.database.set_transaction(transaction)

    def gen_index_transaction(self, index_code, data):
        """ 生成指数数据
            @Param index_code: 股票代码
            @Param data: 交易数据
            @Note: 注意事项 - 指数交易数据中成交量和换手率均为0!!!
        """
        transaction = dict()

        # 股票KEY
        transaction["stock_key"] = self.gen_index_key(HKEX_EXCHAGE_KEY, index_code)

        # 交易日期(YYYYMMDD)
        transaction_timestamp = int(data["timestamp"])
        transaction_date = time.localtime(transaction_timestamp)

        transaction["date"] = self.gen_date(
            transaction_date.tm_year,
            transaction_date.tm_mon,
            transaction_date.tm_mday)

        # 开盘价
        if data["open_price"] is None:
            logging.error("Open price is none! date:%s stock_key:%s open_price:%f",
                          transaction["date"], transaction["stock_key"], transaction["open_price"])
            return None

        transaction["open_price"] = float(data["open_price"])

        # 收盘价
        if data["close_price"] is None:
            logging.error("Close price is none! date:%s stock_key:%s close_price:%f",
                          transaction["date"], transaction["stock_key"], transaction["close_price"])
            return None

        transaction["close_price"] = float(data["close_price"])

        # 最高价
        if data["top_price"] is None:
            logging.error("Top price is none! date:%s stock_key:%s top_price:%f",
                          transaction["date"], transaction["stock_key"], transaction["top_price"])
            return None

        transaction["top_price"] = float(data["top_price"])

        # 最低价
        if data["bottom_price"] is None:
            logging.error("Bottom price is none! date:%s stock_key:%s bottom_price:%f",
                          transaction["date"], transaction["stock_key"], transaction["bottom_price"])
            return None

        transaction["bottom_price"] = float(data["bottom_price"])

        # 交易量
        transaction["volume"] = 0

        # 交易额
        if data["turnover"] is None:
            logging.error("Turnover is invalid! date:%s stock_key:%s turnover:%f",
                          transaction["date"], transaction["stock_key"], transaction["turnover"])
            return None

        transaction["turnover"] = float(data["turnover"])
        if transaction["turnover"] <= 0:
            logging.error("Turnover is invalid! date:%s stock_key:%s turnover:%d",
                          transaction["date"], transaction["stock_key"], transaction["turnover"])
            return None

        # 创建时间
        curr_timestamp = int(time.time())
        transaction["create_time"] = time.localtime(curr_timestamp)
        # 更新时间
        transaction["update_time"] = time.localtime(curr_timestamp)

        return transaction
