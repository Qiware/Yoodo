# -*- coding:utf-8 -*-

# 爬取港交所数据

import sys
import json
import time
import logging

from hkex import *

sys.path.append("../../lib/database")
from database import *

# 港交所KEY
HKEX_EXCHAGE_KEY = "hkex"

# 爬取交易数据返回字段序列
HKEX_TRANSACTION_TIMESTAMP = 0 # 交易时间戳
HKEX_TRANSACTION_OPEN_PRICE = 1 # 开盘价
HKEX_TRANSACTION_TOP_PRICE = 2 # 最高价
HKEX_TRANSACTION_BOTTOM_PRICE = 3 # 最低价
HKEX_TRANSACTION_CLOSE_PRICE = 4 # 收盘价
HKEX_TRANSACTION_VOLUME = 5 # 交易量
HKEX_TRANSACTION_TURNOVER = 6 # 交易额

# 爬虫服务
class Crawler():
    def __init__(self):
        ''' 初始化处理 '''
        self.hkex = HKEX()
        self.database = Database()

    def gen_date(self, year, month, mday):
        ''' 生成日期: YYYYMMDD '''

        return "%04d%02d%02d" % (year, month, mday)

    def gen_stock_key(self, exchange_code, stock_code):
        ''' 生成股票KEY '''
        return "%s:%05d" % (exchange_code, int(stock_code))

    def _crawl_stock(self, stock_code):
        ''' 爬取指定股票信息 '''

        try:
            # 爬取股票数据
            data = self.hkex.get_stock(stock_code)
            if len(data) == 0:
                logging.error("Get stock data failed! stock_code:%s", stock_code)
                return None

            # 提取有效信息
            stock = dict()
            stock["key"] = self.gen_stock_key(HKEX_EXCHAGE_KEY, data["sym"])
            stock["name"] = str(data["nm"])

            timestamp = int(time.time())
            stock["create_time"] = time.localtime(timestamp)
            stock["update_time"] = time.localtime(timestamp)

            # 更新股票信息
            return self.database.set_stock(stock)
        except Exception as e:
            logging.error("Crawl stock failed! stock_code:%d errmsg:%s", stock_code, e)
            return e

    def crawl_stock(self):
        ''' 爬取全部股票信息 '''
        stock_code = HKEX_STOCK_CODE_MIN
        while (stock_code <= HKEX_STOCK_CODE_MAX):
            print("Crawl stock data. stock_code:%s" % (stock_code))
            # 爬取股票数据
            self._crawl_stock(stock_code)

            stock_code += 1

    def gen_transaction(self, stock_code, data):
        ''' 生成交易数据 '''
        transaction = dict()

        # 股票KEY
        transaction["stock_key"] = self.gen_stock_key(HKEX_EXCHAGE_KEY, stock_code)

        # 交易日期(YYYYMMDD)
        transaction_timestamp = int(data[HKEX_TRANSACTION_TIMESTAMP])/1000
        transaction_date = time.localtime(transaction_timestamp)

        transaction["date"] = self.gen_date(
                transaction_date.tm_year,
                transaction_date.tm_mon,
                transaction_date.tm_mday)

        # 开盘价
        transaction["open_price"] = float(data[HKEX_TRANSACTION_OPEN_PRICE])
        if transaction["open_price"] <= 0:
            logging.error("Open price is invalid! stock_code:%s open_price:%f", stock_code, transaction["open_price"])
            return None
        # 收盘价
        transaction["close_price"] = float(data[HKEX_TRANSACTION_CLOSE_PRICE])
        if transaction["close_price"] <= 0:
            logging.error("Close price is invalid! stock_code:%s close_price:%f", stock_code, transaction["close_price"])
            return None
        # 最高价
        transaction["top_price"] = float(data[HKEX_TRANSACTION_TOP_PRICE])
        if transaction["top_price"] <= 0:
            logging.error("Top price is invalid! stock_code:%s top_price:%f", stock_code, transaction["top_price"])
            return None
        # 最低价
        transaction["bottom_price"] = float(data[HKEX_TRANSACTION_BOTTOM_PRICE])
        if transaction["bottom_price"] <= 0:
            logging.error("Bottom price is invalid! stock_code:%s bottom_price:%f", stock_code, transaction["bottom_price"])
            return None

        # 交易量
        transaction["volume"] = int(data[HKEX_TRANSACTION_VOLUME])
        if transaction["volume"] <= 0:
            logging.error("Volume is invalid! stock_code:%s volume:%d", stock_code, transaction["volume"])
            return None
        # 交易额
        transaction["turnover"] = float(data[HKEX_TRANSACTION_TURNOVER])
        if transaction["turnover"] <= 0:
            logging.error("Turnover is invalid! stock_code:%s turnover:%f", stock_code, transaction["turnover"])
            return None

        # 创建时间
        curr_timestamp = int(time.time())
        transaction["create_time"] = time.localtime(curr_timestamp)
        # 更新时间
        transaction["update_time"] = time.localtime(curr_timestamp)

        return transaction

    def _crawl_transaction(self, stock_code):
        ''' 爬取指定股票交易信息 '''

        # 爬取交易数据
        data_list = self.hkex.get_transaction(stock_code, HKEX_SPAN_DAY, HKEX_LASTEST_2YEAR)
        print(data_list)

        # 遍历交易数据
        for data in data_list:
            try:
                # 提取交易信息
                transaction = self.gen_transaction(stock_code, data)
                if transaction is None:
                    logging.error("Gen transaction failed! stock_code:%s ", stock_code)
                    continue

                # 更新交易信息
                self.database.set_transaction(transaction)
            except Exception as e:
                logging.error("Catch exception! stock_code:%s e:%s", stock_code, str(e))
                continue

        return None

    def crawl_transaction(self, stock_code):
        ''' 爬取交易信息 '''

        # 判断是否爬取所有交易
        if str(stock_code) != "all":
            # 爬取指定股票交易
            self._crawl_transaction(stock_code)
            return

        # 获取股票列表
        stock_list = self.database.get_all_stock()
        if len(stock_list) == 0:
            logging.error("Get stock list failed!")
            return

        # 获取交易数据
        for stock in stock_list:
            # 获取股票代码
            stock_key = stock["key"].split(":")
            exchange = stock_key[0]
            stock_code = int(stock_key[1])

            logging.info("Crawl transaction data. stock_key:%s", stock_key)

            # 获取交易数据
            self._crawl_transaction(stock_code)
