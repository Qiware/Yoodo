# -*- coding:utf-8 -*-

# 爬取港交所数据

import sys
import json
import time
import logging

from hkex import *
from const import *

sys.path.append("../../lib/repo/database")
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

    def crawl_stock_by_stock_code(self, stock_code):
        ''' 爬取指定股票信息 '''

        # 爬取股票数据
        data = self.hkex.get_stock(stock_code)
        if len(data) == 0:
            logging.error("Get stock data failed! stock_code:%s", stock_code)
            return None

        # 提取有效信息
        stock = dict()
        stock["stock_key"] = self.gen_stock_key(HKEX_EXCHAGE_KEY, data["stock_code"]) # 股票KEY
        stock["name"] = str(data["name"]) # 股票名称
        stock["total"] = int(data["total"]) # 总股本数量
        stock["market_cap"] = float(data["market_cap"]) # 总市值
        stock["product_type"] = data["product_type"] # 产品类型
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
        ''' 爬取全部股票信息 '''
        stock_code = int(start)
        while (stock_code <= int(end)):
            print("Crawl stock data. stock_code:%s" % (stock_code))
            # 爬取股票数据
            self.crawl_stock_by_stock_code(stock_code)

            stock_code += 1

    def gen_transaction(self, stock_code, stock_data, data):
        ''' 生成交易数据
            @Param stock_code: 股票代码
            @Param stock_data: 股票信息
            @Param data: 交易数据
        '''
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

        # 查询交易列表
        history_transaction_list = self.database.get_transaction_list(
                transaction["stock_key"], int(transaction["date"])-1, 20)

        # MA5平均价格
        transaction["ma5_avg_price"] = self.compute_avg_price(
                transaction["close_price"], history_transaction_list, 5)

        # MA10平均价格
        transaction["ma10_avg_price"] = self.compute_avg_price(
                transaction["close_price"], history_transaction_list, 10)

        # MA20平均价格
        transaction["ma20_avg_price"] = self.compute_avg_price(
                transaction["close_price"], history_transaction_list, 20)

        # MA5交易量
        transaction["ma5_volume"] = self.compute_volume(
                transaction["volume"], history_transaction_list, 5)

        # MA10交易量
        transaction["ma10_volume"] = self.compute_volume(
                transaction["volume"], history_transaction_list, 10)

        # MA20交易量
        transaction["ma20_volume"] = self.compute_volume(
                transaction["volume"], history_transaction_list, 20)

        # 创建时间
        curr_timestamp = int(time.time())
        transaction["create_time"] = time.localtime(curr_timestamp)
        # 更新时间
        transaction["update_time"] = time.localtime(curr_timestamp)

        return transaction

    def compute_avg_price(self, today_close_price, history_transaction_list, days):
        ''' 计算股票days天的平均价格
            @Param today_close_price: 今日收盘价
            @Param history_transaction_list: 交易列表
            @Param days: 天数
        '''

        total_price = today_close_price
        if len(history_transaction_list) == 0:
            return total_price

        tl = history_transaction_list
        if len(history_transaction_list) > (days-1):
            tl = history_transaction_list[:days-1]

        for item in tl:
            total_price += float(item["close_price"])

        return total_price / (len(tl)+1)

    def compute_volume(self, today_volume, history_transaction_list, days):
        ''' 计算股票days天的累积交易量
            @Param volume: 今日交易量
            @Param transaction_list: 交易列表
            @Param days: 天数
            @注意事项: 天数不足时, 则用平均数 * days返回.
        '''

        total_volume = today_volume
        if len(history_transaction_list) == 0:
            return total_volume * days

        tl = history_transaction_list
        if len(history_transaction_list) > (days-1):
            tl = history_transaction_list[:days-1]

        for item in tl:
            total_volume += item["volume"]

        if len(history_transaction_list) < (days-1):
            return  int((total_volume/(len(history_transaction_list)+1)) * days)

        return total_volume

    def _crawl_transaction(self, stock_code, stock_data, start_date):
        ''' 爬取指定股票交易信息 '''

        # 爬取交易数据
        data_list = self.hkex.get_kline_from_hkex(stock_code, start_date)
        if data_list is None:
            logging.error("Get hkex kline failed! stock_code:%s start_date:%s",
                          stock_code, start_date)
            return None

        # 遍历交易数据
        for data in data_list:
            # 提取交易信息
            transaction = self.gen_transaction(stock_code, stock_data, data)
            if transaction is None:
                logging.error("Gen transaction failed! stock_code:%s ", stock_code)
                continue

            # 更新交易信息
            self.database.set_transaction(transaction)

        return None

    def crawl_transaction(self, begin_stock_code, start_date):
        ''' 爬取交易信息
            @Param stock_code: 股票代码
            @Param start_date: 开始日期. 格式: YYYY-MM-DD
        '''

        # 获取股票列表
        stock_list = self.database.get_all_stock()
        if len(stock_list) == 0:
            logging.error("Get stock list failed!")
            return

        # 获取交易数据
        for stock in stock_list:
            # 获取股票代码
            stock_key = stock["stock_key"].split(":")
            exchange = stock_key[0]
            stock_code = int(stock_key[1])
            if stock_code < int(begin_stock_code):
                continue

            logging.info("Crawl transaction data. stock_key:%s", stock_key)

            # 获取交易数据
            self._crawl_transaction(stock_code, stock, start_date)
