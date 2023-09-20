# -*- coding:utf-8 -*-
# 君子爱财 取之YOODO!

# 爬取港交所数据

import sys
import json
import logging
import time
import urllib
import requests  # pip3 install requests

from const import *
from baidu import *

sys.path.append("../../repo/lib/digit")
from digit import *

# 获取多久的数据
HKEX_LASTEST_TODAY = 1  # 获取最近今天的数据(时)
HKEX_LASTEST_1MONTH = 2  # 获取最近1个月的数据
HKEX_LASTEST_3MONTH = 3  # 获取最近3个月的数据
HKEX_LASTEST_6MONTH = 4  # 获取最近6个月的数据
HKEX_LASTEST_1YEAR = 5  # 获取最近1年的数据
HKEX_LASTEST_2YEAR = 6  # 获取最近2年的数据
HKEX_LASTEST_5YEAR = 7  # 获取最近5年的数据
HKEX_LASTEST_10YEAR = 8  # 获取最近5年的数据
HKEX_LASTEST_THIS_YEAR = 9  # 获取本年至今的数据

# 时间维度
HKEX_SPAN_DAY = 6  # 按天维度
HKEX_SPAN_WEEK = 7  # 按周维度
HKEX_SPAN_MONTH = 8  # 按月维度

# 获取交易数据(交易时间、开盘价、最高价、最低价、收盘价、交易量、交易额)
# @Param span: 时间维度
# @Param int: 时间间隔
# @Param ric: 股票代码(如: 0700.HK)
# @Param token: TOKEN
# @Param callback: JS回调函数
# @Param _: 时间戳(14位)
HKEX_GET_CHART_DATA2_URL = "https://www1.hkex.com.hk/hkexwidget/data/getchartdata2?hchart=1&span=%d&int=%d&ric=%04d.HK&token=%s&qid=%d&callback=jQuery35106409699179416692_%d&_=%d"

# 获取股票数据(股票代码、企业名称等)
# @Param int: 时间间隔
# @Param sym: 股票代码(如: 700)
# @Param token: TOKEN
# @Param callback: JS回调函数
# @Param _: 时间戳(14位)
# HKEX_GET_EQUITY_QUOTE_URL = "https://www1.hkex.com.hk/hkexwidget/data/getequityquote?sym=%d&token=%s&lang=chn&qid=1690217104956&callback=jQuery35108442069917684831_%d&_=%d"
HKEX_GET_EQUITY_QUOTE_URL = "https://www1.hkex.com.hk/hkexwidget/data/getequityquote?sym=%d&token=%s&lang=chn&qid=%d&callback=jQuery35108442069917684831_%d&_=%d"

# 获取TOKEN
HKEX_GET_TOKEN_URL = "https://sc.hkex.com.hk/TuniS/www.hkex.com.hk/Market-Data/Securities-Prices/Equities/Equities-Quote?sym=3999&sc_lang=zh-hk"

# 错误码
HKEX_RESPONE_CODE_OK = "000"  # 正常


# 港交所
class HKEX():
    def __init__(self):
        """ 初始化 """
        self.token = self.get_token()
        # 百度
        self.baidu = Baidu()

    def get_token(self):
        """ 获取TOKEN """
        token_prefix = "evLtsL"

        headers = {'Content-Type': 'application/json'}

        url = HKEX_GET_TOKEN_URL

        # 获取TOKEN数据
        rsp = requests.get(url=url, headers=headers)
        if rsp is None:
            return dict()

        # 提取TOKEN数据
        lines = rsp.text.split('\n')
        for line in lines:
            line = line.strip()
            if line.find(token_prefix) == -1:
                continue
            segments = line.split("\"")
            return segments[1]

    def parse(self, text):
        """ 提取text中的JSON字串, 并返回JSON对象
            @Param text: 格式为jQuery35108442069917684831_1690217104956({.....})
            @思路: 定为第一个左大括号和最后一个右大括号.
        """
        if len(text) == 0:
            return dict()

        offset = text.find("(")
        if offset >= 0:
            data = text[offset + 1:-1]
            return json.loads(data)
        return dict()

    def get_stock(self, stock_code):
        """ 获取股票基础信息: 股票代码、企业名称、企业市值等 """
        headers = {'Content-Type': 'application/json'}

        timestamp = int(time.time() * 1000)

        url = HKEX_GET_EQUITY_QUOTE_URL % (int(stock_code), self.token, timestamp, timestamp, timestamp)

        # 发起拉取请求
        rsp = requests.get(url=url, headers=headers)
        if rsp is None:
            return dict()

        # 结果校验
        data = self.parse(rsp.text)
        if "data" not in data.keys():
            return dict()
        data = data["data"]
        if "responsecode" not in data.keys():
            return dict()
        if data["responsecode"] != HKEX_RESPONE_CODE_OK:
            return dict()
        if "quote" not in data.keys():
            return dict()

        # 结果解析
        return self.parse_stock_resp(data["quote"])

    def parse_stock_resp(self, resp):
        """ 解析股票信息 """
        data = dict()

        data["stock_code"] = resp["sym"]  # 股票代码
        data["name"] = str(resp["nm"])  # 股票名称
        data["total"] = str_to_digit(resp["amt_os"])  # 总股本数量
        data["market_cap"] = str_with_unit_to_digit(resp["mkt_cap"], resp["mkt_cap_u"])  # 总市值
        data["product_type"] = resp["product_type"].upper()  # 产品类型
        if "hsic_ind_classification" in resp.keys():
            data["first_classification"] = resp["hsic_ind_classification"]  # 一级分类
        if "hsic_sub_sector_classification" in resp.keys():
            data["second_classification"] = resp["hsic_sub_sector_classification"]  # 二级分类
        if "summary" in resp.keys():
            data["introduction"] = resp["summary"]  # 公司介绍

        return data

    def get_hsi_kline(self):
        """ 获取恒生指数K线数据: 开盘价、最高价、最低价、收盘价等
            @Param code: 股票代码
            @Param num: K线数量
        """
        return self.baidu.get_hsi_kline()

    def get_lastest_interval(self, lastest_day):
        if lastest_day == "1month":
            return HKEX_LASTEST_1MONTH
        if lastest_day == "3month":
            return HKEX_LASTEST_3MONTH
        if lastest_day == "6month":
            return HKEX_LASTEST_6MONTH
        if lastest_day == "1year":
            return HKEX_LASTEST_1YEAR
        if lastest_day == "2year":
            return HKEX_LASTEST_2YEAR
        return HKEX_LASTEST_1MONTH

    def get_hz2083_kline(self):
        """ 获取'恒生科技指数'K线数据: 开盘价、最高价、最低价、收盘价等
            @Param code: 股票代码
            @Param num: K线数量
        """
        return self.baidu.get_hz2083_kline()

    def get_kline_from_hkex(self, stock_code, lastest_day):
        """ 获取交易K线数据: 开盘价、最高价、最低价、收盘价、交易量、交易额等
            @Param code: 股票代码
            @Param num: K线数量
        """

        interval = self.get_lastest_interval(lastest_day)

        # 准备请求参数
        headers = {'Content-Type': 'application/json'}

        timestamp = int(time.time() * 1000)

        url = HKEX_GET_CHART_DATA2_URL % (
                HKEX_SPAN_DAY, interval,
                int(stock_code), self.token,
                timestamp, timestamp, timestamp)

        # 发起拉取请求
        rsp = requests.get(url=url, headers=headers)
        if rsp is None:
            logging.error("Get transaction failed!")
            return dict()

        # 结果解析
        data = self.parse(rsp.text)
        if "data" not in data.keys():
            logging.error("No 'data' field!")
            return dict()
        data = data["data"]
        if "responsecode" not in data.keys():
            logging.error("No 'responsecode' field!")
            return dict()
        if data["responsecode"] != HKEX_RESPONE_CODE_OK:
            logging.error("'responsecode' is invalid!")
            return dict()
        if ("datalist" not in data.keys()) or (not isinstance(data["datalist"], list)):
            logging.error("No 'datalist' field or invalid.")
            return dict()
        return self.parse_kline_from_hkex(data["datalist"])

    def parse_kline_from_hkex(self, data_list):
        """ 解析从HKEX获取的KLINE数据 """
        transaction_list = list()

        index_timestamp = 0  # 时间戳(单位: 毫秒)
        index_open_price = 1  # 开盘价
        index_top_price = 2  # 最高价
        index_bottom_price = 3  # 最低价
        index_close_price = 4  # 收盘价
        index_volume = 5  # 交易量
        index_turnover = 6  # 交易额

        for data in data_list:
            transaction = dict()

            # 时间戳
            transaction["timestamp"] = int(data[index_timestamp] / 1000)

            # 开盘价
            if data[index_open_price] is None:
                continue
            transaction["open_price"] = float(data[index_open_price])

            # 最高价
            if data[index_top_price] is None:
                continue
            transaction["top_price"] = float(data[index_top_price])

            # 最低价
            if data[index_bottom_price] is None:
                continue
            transaction["bottom_price"] = float(data[index_bottom_price])

            # 收盘价
            if data[index_close_price] is None:
                continue
            transaction["close_price"] = float(data[index_close_price])

            # 交易量
            if data[index_volume] is None:
                continue
            transaction["volume"] = float(data[index_volume])

            # 交易额
            if data[index_turnover] is None:
                continue
            transaction["turnover"] = float(data[index_turnover])

            # 换手率
            transaction["turnover_ratio"] = 0

            # 数据修正(只有收盘价大于0时)
            if (transaction["open_price"] < 0) \
                    and (transaction["top_price"] < 0) \
                    and (transaction["bottom_price"] < 0) \
                    and (transaction["close_price"] > 0):
                transaction["open_price"] = transaction["close_price"]
                transaction["top_price"] = transaction["close_price"]
                transaction["bottom_price"] = transaction["close_price"]

            transaction_list.append(transaction)

        return transaction_list


if __name__ == "__main__":
    hkex = HKEX()

    # 爬取企业信息
    stock_code = HKEX_STOCK_CODE_MIN
    while (stock_code <= HKEX_STOCK_CODE_MAX):
        print("Stock code: ", stock_code)
        print(hkex.get_stock(stock_code))
        stock_code += 1

    # 爬取交易数据
    # stock_code = HKEX_STOCK_CODE_MIN
    # while (stock_code <= HKEX_STOCK_CODE_MAX):
    #    print("Stock code: ", stock_code)
    #    print(hkex.get_kline(stock_code, "2023-07-01"))
    #    stock_code += 1
