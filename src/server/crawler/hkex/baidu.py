# -*- coding:utf-8 -*-

# 爬取港交所数据

import json
import logging
import time
import urllib
import requests	# pip3 install requests

from const import *

# 时间维度
KLINE_KTYPE_DAY = 1 # 按天维度
KLINE_KTYPE_WEEK = 2 # 按周维度

# 交易所数据
KLINE_GROUP_HKEX = "quotation_kline_hk" # 港交所数据

# 获取交易K线数据(交易时间、开盘价、最高价、最低价、收盘价、交易量、交易额)
# @Param all: 是否全部交易数据(注：取值固定为1)
# @Param code: 股票代码(如: 00700)
# @Param start_time: 开始时间(如: 2004-06-16+00:00:00)
# @Param ktype: K线类型(如: 1表示日线)
KLINE_URL = "https://finance.pae.baidu.com/selfselect/getstockquotation?all=1&code=%05d&isIndex=false&isBk=false&isBlock=false&isFutures=false&isStock=true&newFormat=1&is_kc=0&start_time=%s+00:00:00&ktype=%d&group=%s&finClientType=pc"

# 从百度获取港交所数据
class Baidu():
    def __init__(self):
        ''' 初始化 '''
        pass

    def get_kline(self, stock_code, start_time, ktype, group):
        ''' 获取交易数据: 开盘价、最高价、最低价、收盘价、交易量、交易额、换手率等
            @Param stock_code: 股票代码
            @Param start_time: 起始时间(格式: YYYYMMDD)
            @Param ktype: K线类型
        '''

        # 准备请求参数
        headers = { 'Content-Type' : 'application/json' }

        timestamp = int(time.time() * 1000)

        year = start_time[0:4]
        month = start_time[4:6]
        day = start_time[6:8]

        start_date = "%s-%s-%s" % (year, month, day)

        url =  KLINE_URL % (int(stock_code), start_date, ktype, group)

        # 发起拉取请求
        rsp = requests.get(url=url, headers=headers)
        if rsp is None:
            logging.error("Get transaction failed!")
            return dict()

        # 结果解析
        data = json.loads(rsp.text)
        if data is None:
            logging.error("Get response field! stock_code:%s", stock_code)
            return dict()

        if ("Result" not in data.keys()) or (not isinstance(data["Result"], dict)):
            logging.error("No 'Result' field! stock_code:%s", stock_code)
            return dict()

        if "newMarketData" not in data['Result'].keys():
            logging.error("No 'newMarketData' field! stock_code:%s", stock_code)
            return dict()

        return self.parse_kline_resp(data["Result"]["newMarketData"])

    def parse_kline_resp(self, data):
        ''' 解析交易K线数据: 开盘价、最高价、最低价、收盘价、交易量、交易额、换手率等
            @Param data: 应答数据. 类型: dict.
        '''

        # 数据校验
        if 'keys' not in data.keys():
            logging.error("No 'keys' field!")
            return dict()

        if 'marketData' not in data.keys():
            logging.error("No 'marketData' field!")
            return dict()

        # 解析应答结果
        index_timestamp = 0 # 时间戳
        index_date = 1  # 日期(格式: 2004-06-16)
        index_open_price = 2  # 开盘价
        index_close_price = 3  # 收盘价
        index_volume = 4  # 成交量
        index_top_price = 5  # 最高价
        index_bottom_price = 6  # 最低价
        index_turnover = 7  # 成交额
        index_range = 8  # 涨跌额
        index_ratio = 9  # 涨跌率
        index_turnover_ratio = 10  # 换手率

        keys_dict = dict()
        keys_dict[index_timestamp] = "timestamp"
        keys_dict[index_date] = "time"
        keys_dict[index_open_price] = "open"
        keys_dict[index_close_price] = "close"
        keys_dict[index_volume] = "volume"
        keys_dict[index_top_price] = "high"
        keys_dict[index_bottom_price] = "low"
        keys_dict[index_turnover] = "amount"
        keys_dict[index_range] = "range"
        keys_dict[index_ratio] = "ratio"
        keys_dict[index_turnover_ratio] = "turnoverratio"

        # 校验KEY序列是否匹配
        for idx in keys_dict.keys():
            key = keys_dict[idx]
            if key != data["keys"][idx]:
                logging.error("Keys is invalid! key:%s idx:%d", key, idx)
                return None

        # 解析数据
        transaction_list = list()
        offset = 0

        data_list = data["marketData"].split(";")

        for item in data_list:
            values = item.split(",")

            transaction = dict()

            transaction["timestamp"] = int(values[index_timestamp]) # 时间戳
            transaction["open_price"] = float(values[index_open_price]) # 开盘价
            transaction["close_price"] = float(values[index_open_price]) # 收盘价
            transaction["volume"] = int(values[index_volume]) # 交易量
            transaction["top_price"] = float(values[index_top_price]) # 最高价
            transaction["bottom_price"] = float(values[index_bottom_price]) # 最低价
            transaction["turnover"] = float(values[index_turnover]) # 交易额
            #transaction["ratio"] = float(values[index_ratio]) # 涨跌率
            transaction["turnover_ratio"] = float(values[index_turnover_ratio]) # 换手率

            transaction_list.append(transaction)
        return transaction_list

if __name__ == "__main__":
    baidu = Baidu()

    # 爬取交易数据
    stock_code = HKEX_STOCK_CODE_MIN
    while (stock_code <= HKEX_STOCK_CODE_MAX):
        print("Stock code: ", stock_code)
        print(baidu.get_kline(stock_code, "2023-07-20", KLINE_KTYPE_DAY))
        stock_code += 1
