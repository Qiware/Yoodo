# -*- coding:utf-8 -*-

# 爬取港交所数据

import json
import logging
import time
import urllib
import requests	# pip3 install requests

from const import *
from baidu import *

# 获取多久的数据
HKEX_LASTEST_TODAY = 1 # 获取最近今天的数据(时)
HKEX_LASTEST_1MONTH = 2 # 获取最近1个月的数据
HKEX_LASTEST_3MONTH = 3 # 获取最近3个月的数据
HKEX_LASTEST_6MONTH = 4 # 获取最近6个月的数据
HKEX_LASTEST_1YEAR = 5 # 获取最近1年的数据
HKEX_LASTEST_2YEAR = 6 # 获取最近2年的数据
HKEX_LASTEST_5YEAR = 7 # 获取最近5年的数据
HKEX_LASTEST_10YEAR = 8 # 获取最近5年的数据
HKEX_LASTEST_THIS_YEAR = 9 # 获取本年至今的数据

# 时间维度
HKEX_SPAN_DAY = 6 # 按天维度
HKEX_SPAN_WEEK = 7 # 按周维度
HKEX_SPAN_MONTH = 8 # 按月维度

# 获取交易数据(交易时间、开盘价、最高价、最低价、收盘价、交易量、交易额)
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
#HKEX_GET_EQUITY_QUOTE_URL = "https://www1.hkex.com.hk/hkexwidget/data/getequityquote?sym=%d&token=%s&lang=chn&qid=1690217104956&callback=jQuery35108442069917684831_%d&_=%d"
HKEX_GET_EQUITY_QUOTE_URL = "https://www1.hkex.com.hk/hkexwidget/data/getequityquote?sym=%d&token=%s&lang=chn&qid=%d&callback=jQuery35108442069917684831_%d&_=%d"

# 获取TOKEN
HKEX_GET_TOKEN_URL = "https://sc.hkex.com.hk/TuniS/www.hkex.com.hk/Market-Data/Securities-Prices/Equities/Equities-Quote?sym=3999&sc_lang=zh-hk"

# 错误码
HKEX_RESPONE_CODE_OK = "000" # 正常

# 港交所
class HKEX():
    def __init__(self):
        ''' 初始化 '''
        self.token = self.get_token()
        # 百度
        self.baidu = Baidu()

    def get_token(self):
        ''' 获取TOKEN '''
        token_prefix = "evLtsL"

        headers = { 'Content-Type' : 'application/json' }

        url =  HKEX_GET_TOKEN_URL

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
        ''' 提取text中的JSON字串, 并返回JSON对象
            @Param text: 格式为jQuery35108442069917684831_1690217104956({.....})
            @思路: 定为第一个左大括号和最后一个右大括号.
        '''
        if len(text) == 0:
            return dict()

        offset = text.find("(")
        if offset >= 0:
            data = text[offset+1:-1]
            return json.loads(data)
        return dict()

    def get_stock(self, stock_code):
        ''' 获取股票基础信息: 股票代码、企业名称、企业市值等 '''
        headers = { 'Content-Type' : 'application/json' }

        timestamp = int(time.time() * 1000)

        url =  HKEX_GET_EQUITY_QUOTE_URL % (int(stock_code), self.token, timestamp, timestamp, timestamp)

        # 发起拉取请求
        rsp = requests.get(url=url, headers=headers)
        if rsp is None:
            return dict()

        # 结果解析
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
        return data["quote"]

    def get_kline(self, stock_code, start_time):
        ''' 获取交易K线数据: 开盘价、最高价、最低价、收盘价、交易量、交易额、换手率等
            @Param code: 股票代码
            @Param num: K线数量
        '''
        return self.baidu.get_kline(stock_code, start_time, KLINE_KTYPE_DAY, KLINE_GROUP_HKEX)

if __name__ == "__main__":
    hkex = HKEX()

    # 爬取企业信息
    stock_code = HKEX_STOCK_CODE_MIN
    while (stock_code <= HKEX_STOCK_CODE_MAX):
        print("Stock code: ", stock_code)
        print(hkex.get_stock(stock_code))
        stock_code += 1

    # 爬取交易数据
    #stock_code = HKEX_STOCK_CODE_MIN
    #while (stock_code <= HKEX_STOCK_CODE_MAX):
    #    print("Stock code: ", stock_code)
    #    print(hkex.get_kline(stock_code, "2023-07-01"))
    #    stock_code += 1
