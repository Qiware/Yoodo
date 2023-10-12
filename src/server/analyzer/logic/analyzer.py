# -*- coding:utf-8 -*-
# 君子爱财 取之YOODO!

import sys
import json
import logging
import math
import pandas
import talib

sys.path.append("../../lib/data")
from data import Data

sys.path.append("../../lib/utils/dtime")
from dtime import get_current_date

sys.path.append("../../lib/utils/thread_pool")
from thread_pool import ThreadPool

# 默认工作线程数量
WORKER_NUM = 20
# 队列长度
WAIT_QUEUE_LEN = 1000

# 消息类型
TYPE_INDEX_KEY = 1
TYPE_STOCK_KEY = 2


class Analyzer():
    def __init__(self):
        """ 初始化 """
        # 数据模块
        self.data = Data()

        self.worker = ThreadPool(WAIT_QUEUE_LEN, WORKER_NUM)
        self.worker.register(TYPE_INDEX_KEY, self.analyze)
        self.worker.register(TYPE_STOCK_KEY, self.analyze)

    def load_stock(self):
        """ 加载股票列表 """
        # 获取股票列表
        stock_list = self.data.get_all_stock()
        for stock in stock_list:
            self.worker.bpush(TYPE_STOCK_KEY, stock["stock_key"])
        self.worker.wait()

    def load_index(self):
        """ 加载指数列表 """
        # 获取指数列表
        index_list = self.data.get_all_index()
        for index in index_list:
            self.worker.bpush(TYPE_INDEX_KEY, index["index_key"])
        self.worker.wait()

    def analyze(self, stock_key):
        logging.debug("Analyze stock technical index. stock_key:%s", stock_key)

        # 获取交易数据(按时间逆序)
        transaction_list = self.data.get_transaction_list(
            stock_key, int(get_current_date()), 100000)
        if (transaction_list is None) or (len(transaction_list) == 0):
            logging.error("Get transaction list failed! stock_key:%s", stock_key)
            return

        # 计算交易指标
        stock_index = self.compute(transaction_list)
        if stock_index is None:
            logging.error("Analyze stock index failed! stock_key:%s", stock_key)
            return

        # 更新交易指标
        self.update(stock_key, stock_index)

    def compute(self, transaction_list):
        """ 计算交易指标 """
        stock_index = dict()
        transaction_list = self.sort_by_date(transaction_list)

        top_price_list = self.get_top_price_list(transaction_list)
        bottom_price_list = self.get_bottom_price_list(transaction_list)
        close_price_list = self.get_close_price_list(transaction_list)
        volume_list = self.get_volume_list(transaction_list)

        # MA5PRC/MA10PRC/MA20PRC
        self.ma_price(stock_index, transaction_list, close_price_list, 5)
        self.ma_price(stock_index, transaction_list, close_price_list, 10)
        self.ma_price(stock_index, transaction_list, close_price_list, 20)

        # MA5VOL/MA10VOL/MA20VOL
        self.ma_volume(stock_index, transaction_list, volume_list, 5)
        self.ma_volume(stock_index, transaction_list, volume_list, 10)
        self.ma_volume(stock_index, transaction_list, volume_list, 20)

        # MACD指标
        self.macd(stock_index, transaction_list, close_price_list)

        # KDJ指标
        self.kdj(stock_index, transaction_list,
                 top_price_list, bottom_price_list, close_price_list)

        # RSI指标
        self.rsi(stock_index, transaction_list, close_price_list)

        # BOLL指标
        self.boll(stock_index, transaction_list, close_price_list)

        # AD指标
        self.ad(stock_index, transaction_list, top_price_list,
                bottom_price_list, close_price_list, volume_list)

        # ADOSC指标
        self.adosc(stock_index, transaction_list, top_price_list,
                   bottom_price_list, close_price_list, volume_list)

        # OBV指标
        self.obv(stock_index, transaction_list, close_price_list, volume_list)

        # SAR指标
        self.sar(stock_index, transaction_list, top_price_list, bottom_price_list)

        # WILLR指标
        self.willr(stock_index, transaction_list,
                   top_price_list, bottom_price_list, close_price_list)

        # CCI指标
        self.cci(stock_index, transaction_list,
                 top_price_list, bottom_price_list, close_price_list)

        # EMA指标
        self.ema(stock_index, transaction_list, close_price_list)

        return stock_index

    def ma_price(self, stock_index, transaction_list, close_price_list, days):
        """ 计算收盘价移动平均线
            matype的取值类型:
                * 0: SMA(默认)
                * 1: EMA
                * 2: WMA
                * 3: DEMA
                * 4: TEMA
                * 5: TRIMA
                * 6: KAMA
                * 7: MAMA
                * 8: T3
        """

        index_name = "MA%dPRC" % (days)

        # 计算MA指标
        ma = talib.MA(pandas.Series(close_price_list), timeperiod=days, matype=0)

        idx = 0
        for item in ma:
            date = transaction_list[idx]["date"]
            if math.isnan(item):
                idx += 1
                continue
            if date not in stock_index.keys():
                stock_index[date] = dict()
            stock_index[date][index_name] = float(item)
            idx += 1
        return

    def ma_volume(self, stock_index, transaction_list, volume_list, days):
        """ 计算交易量移动平均线
            matype的取值类型:
                * 0: SMA(默认)
                * 1: EMA
                * 2: WMA
                * 3: DEMA
                * 4: TEMA
                * 5: TRIMA
                * 6: KAMA
                * 7: MAMA
                * 8: T3
        """

        index_name = "MA%dVOL" % (days)

        # 计算MA指标
        ma = talib.MA(pandas.Series(volume_list), timeperiod=days, matype=0)

        idx = 0
        for item in ma:
            date = transaction_list[idx]["date"]
            if math.isnan(item):
                idx += 1
                continue
            if date not in stock_index.keys():
                stock_index[date] = dict()
            stock_index[date][index_name] = float(item)
            idx += 1
        return

    def dema(self, stock_index, transaction_list):
        """ 计算DEMA指标 """

        # 抽取交易量列表
        close_price_list = list()
        for transaction in transaction_list:
            close_price_list.append(float(transaction["close_price"]))

        # 计算DEMA指标
        dema = talib.DEMA(pandas.Series(close_price_list), timeperiod=30)

        idx = 0
        for item in dema:
            date = transaction_list[idx]["date"]
            if math.isnan(item):
                idx += 1
                continue
            if date not in stock_index.keys():
                stock_index[date] = dict()
            stock_index[date]["DEMA"] = float(item)
            idx += 1
        return

    def macd(self, stock_index, transaction_list, close_price_list):
        """ 计算MACD指标 """

        diff, dea, macd = talib.MACD(
            pandas.Series(close_price_list),
            fastperiod=12, slowperiod=26, signalperiod=9)

        # 存储MACD值
        idx = 0
        while idx < len(macd):
            date = transaction_list[idx]["date"]

            if (math.isnan(diff[idx])) or math.isnan(dea[idx]) or math.isnan(macd[idx]):
                idx += 1
                continue

            value = dict()
            value["DIFF"] = diff[idx]
            value["DEA"] = dea[idx]
            value["MACD"] = macd[idx]

            if date not in stock_index.keys():
                stock_index[date] = dict()
            stock_index[date]["MACD"] = value

            idx += 1
        return

    def kdj(self, stock_index, transaction_list, top_price_list, bottom_price_list, close_price_list):
        """ 计算KDJ指标 """

        k, d = talib.STOCH(
            pandas.Series(top_price_list),
            pandas.Series(bottom_price_list),
            pandas.Series(close_price_list),
            fastk_period=5,
            slowk_period=3, slowk_matype=0,
            slowd_period=3, slowd_matype=0)
        j = 3 * k - 2 * d

        # 存储KDJ值
        idx = 0
        while idx < len(k):
            date = transaction_list[idx]["date"]

            value = dict()
            if (math.isnan(k[idx])) or math.isnan(d[idx]) or math.isnan(j[idx]):
                idx += 1
                continue

            value["K"] = k[idx]
            value["D"] = d[idx]
            value["J"] = j[idx]

            if date not in stock_index.keys():
                stock_index[date] = dict()
            stock_index[date]["KDJ"] = value

            idx += 1
        return

    def rsi(self, stock_index, transaction_list, close_price_list):
        """ 计算RSI指标 """

        rsi = talib.RSI(pandas.Series(close_price_list), timeperiod=14)

        # 存储RSI值
        idx = 0
        while idx < len(rsi):
            date = transaction_list[idx]["date"]

            if math.isnan(rsi[idx]):
                idx += 1
                continue

            if date not in stock_index.keys():
                stock_index[date] = dict()
            stock_index[date]["RSI"] = rsi[idx]
            idx += 1
        return

    def obv(self, stock_index, transaction_list, close_price_list, volume_list):
        """ 计算OBV指标 """

        obv = talib.OBV(pandas.Series(close_price_list), pandas.Series(volume_list), )

        # 存储OBV值
        idx = 0
        while idx < len(obv):
            date = transaction_list[idx]["date"]

            if math.isnan(obv[idx]):
                idx += 1
                continue

            if date not in stock_index.keys():
                stock_index[date] = dict()
            stock_index[date]["OBV"] = obv[idx]
            idx += 1
        return

    def boll(self, stock_index, transaction_list, close_price_list):
        """ 计算BOLL指标 """

        upper, middle, lower = talib.BBANDS(
            pandas.Series(close_price_list),
            timeperiod=20,
            # number of non-biased standard deviations from the mean
            nbdevup=2, nbdevdn=2,
            # Moving average type: simple moving average here
            matype=0)

        # 存储BOLL值
        idx = 0
        while idx < len(upper):
            date = transaction_list[idx]["date"]

            if (math.isnan(upper[idx])) or (math.isnan(middle[idx])) or (math.isnan(lower[idx])):
                idx += 1
                continue

            value = dict()
            value["UPPER"] = upper[idx]
            value["MIDDLE"] = middle[idx]
            value["LOWER"] = lower[idx]

            if date not in stock_index.keys():
                stock_index[date] = dict()
            stock_index[date]["BOLL"] = value
            idx += 1
        return

    def ad(self, stock_index, transaction_list, top_price_list, bottom_price_list, close_price_list, volume_list):
        """ 计算AD指标 """

        ad = talib.AD(
            pandas.Series(top_price_list),
            pandas.Series(bottom_price_list),
            pandas.Series(close_price_list),
            pandas.Series(volume_list))

        # 存储AD值
        idx = 0
        while idx < len(ad):
            date = transaction_list[idx]["date"]

            if math.isnan(ad[idx]):
                idx += 1
                continue

            if date not in stock_index.keys():
                stock_index[date] = dict()
            stock_index[date]["AD"] = ad[idx]
            idx += 1
        return

    def adosc(self, stock_index, transaction_list,
              top_price_list, bottom_price_list, close_price_list, volume_list):
        """ 计算ADOSC指标 """

        adosc = talib.ADOSC(
            pandas.Series(top_price_list),
            pandas.Series(bottom_price_list),
            pandas.Series(close_price_list),
            pandas.Series(volume_list),
            fastperiod=3, slowperiod=10)

        # 存储ADOSC值
        idx = 0
        while idx < len(adosc):
            date = transaction_list[idx]["date"]

            if math.isnan(adosc[idx]):
                idx += 1
                continue

            if date not in stock_index.keys():
                stock_index[date] = dict()
            stock_index[date]["ADOSC"] = adosc[idx]
            idx += 1
        return

    def sar(self, stock_index, transaction_list, top_price_list, bottom_price_list):
        """ 计算SAR指标 """

        sar = talib.SAR(
            pandas.Series(top_price_list),
            pandas.Series(bottom_price_list),
            acceleration=0, maximum=0)

        # 存储SAR值
        idx = 0
        while idx < len(sar):
            date = transaction_list[idx]["date"]

            if math.isnan(sar[idx]):
                idx += 1
                continue

            if date not in stock_index.keys():
                stock_index[date] = dict()
            stock_index[date]["SAR"] = sar[idx]
            idx += 1
        return

    def willr(self, stock_index, transaction_list, top_price_list, bottom_price_list, close_price_list):
        """ 计算WILLR指标 """

        willr = talib.WILLR(
            pandas.Series(top_price_list),
            pandas.Series(bottom_price_list),
            pandas.Series(close_price_list),
            timeperiod=14)

        # 存储WILLR值
        idx = 0
        while idx < len(willr):
            date = transaction_list[idx]["date"]

            if math.isnan(willr[idx]):
                idx += 1
                continue

            if date not in stock_index.keys():
                stock_index[date] = dict()
            stock_index[date]["WILLR"] = willr[idx]
            idx += 1
        return

    def cci(self, stock_index, transaction_list,
            top_price_list, bottom_price_list, close_price_list):
        """ 计算CCI指标 """

        cci = talib.CCI(
            pandas.Series(top_price_list),
            pandas.Series(bottom_price_list),
            pandas.Series(close_price_list),
            timeperiod=14)

        # 存储CCI值
        idx = 0
        while idx < len(cci):
            date = transaction_list[idx]["date"]

            if math.isnan(cci[idx]):
                idx += 1
                continue

            if date not in stock_index.keys():
                stock_index[date] = dict()
            stock_index[date]["CCI"] = cci[idx]
            idx += 1
        return

    def ema(self, stock_index, transaction_list, close_price_list):
        """ 计算EMA指标 """

        ema = talib.EMA(
            pandas.Series(close_price_list),
            timeperiod=6)

        # 存储EMA值
        idx = 0
        while idx < len(ema):
            date = transaction_list[idx]["date"]

            if math.isnan(ema[idx]):
                idx += 1
                continue

            if date not in stock_index.keys():
                stock_index[date] = dict()
            stock_index[date]["EMA"] = ema[idx]
            idx += 1
        return

    def sort_by_date(self, transaction_list):
        """ 交易列表重排序: 按时间有序 """

        sort_list = list()

        count = len(transaction_list)

        idx = 0
        while idx < count:
            sort_list.append(transaction_list[count - idx - 1])
            idx += 1

        return sort_list

    def update(self, stock_key, stock_index):
        """ 更新股票交易指数 """
        for date, data in stock_index.items():
            item = dict()
            item["stock_key"] = stock_key
            item["date"] = date
            item["data"] = json.dumps(data)
            self.data.set_technical_index(item)

    def get_top_price_list(self, transaction_list):
        top_price_list = list()
        for transaction in transaction_list:
            top_price_list.append(float(transaction["top_price"]))
        return top_price_list

    def get_bottom_price_list(self, transaction_list):
        bottom_price_list = list()
        for transaction in transaction_list:
            bottom_price_list.append(float(transaction["bottom_price"]))
        return bottom_price_list

    def get_close_price_list(self, transaction_list):
        close_price_list = list()
        for transaction in transaction_list:
            close_price_list.append(float(transaction["close_price"]))
        return close_price_list

    def get_volume_list(self, transaction_list):
        volume_list = list()
        for transaction in transaction_list:
            volume_list.append(float(transaction["volume"]))
        return volume_list
