# -*- coding:utf-8 -*-
# 君子爱财 取之YOODO!

import sys
import json
import logging
import math
import pandas
import talib
import time
import threading

sys.path.append("../../repo/data")
from data import Data

sys.path.append("../../repo/lib/dtime")
from dtime import get_current_date

# 默认工作线程数量
WORKER_NUM = 10
# 队列长度
WAIT_QUEUE_LEN = 1000

class Analyzer():
    def __init__(self, worker_num=10):
        ''' 初始化
            @Param worker_num: 工作线程数量
        '''
        # 数据模块
        self.data = Data()

        # 待处理队列
        self.wait_queue = list()
        self.push_count = 0
        self.is_load_finished = False

        # 启动一个加载线程
        lt = threading.Thread(target=self.load, args=())
        lt.setDaemon(True)
        lt.start()

        # 启动多个工程线程
        if worker_num <= 0:
            worker_num = WORKER_NUM
        for i in range(worker_num):
            wt = threading.Thread(target=self.handle, args=())
            wt.setDaemon(True)
            wt.start()

    def load(self):
        ''' 加载股票列表 '''
        # 获取股票列表
        stock_list = self.data.get_all_stock()
        for stock in stock_list:
            # 加载待处理队列
            self.wait_queue.append(stock["stock_key"])
            self.push_count += 1
            logging.debug("Push stock_key:%s count:%s", stock["stock_key"], self.push_count)
            while(len(self.wait_queue) >= WAIT_QUEUE_LEN):
                time.sleep(1)
        self.is_load_finished = True

    def handle(self):
        ''' 构建股票指数 '''
        while(True):
            if self.is_finished():
                break
            try:
                stock_key = self.wait_queue.pop()
                logging.debug("Threading[%s] Pop stock_key:%s",
                              threading.current_thread().ident, stock_key)
            except Exception as e:
                time.sleep(1)
                logging.error("Wait queue empty! err:%s", e)
                continue
            self.analyze(stock_key)

    def is_finished(self):
        ''' 是否处理结束 '''
        return (self.is_load_finished == True) and (len(self.wait_queue) == 0)

    def wait(self):
        ''' 等待处理结束 '''
        while(not self.is_finished()):
            print("Stock push count:%s. queue wait count:%s",
                  self.push_count, len(self.wait_queue))
            time.sleep(1)
        print("Analyzing was done! push count:%s", self.push_count)

    def analyze(self, stock_key):
        # 获取交易数据(按时间逆序)
        transaction_list = self.data.get_transaction_list(
                stock_key, get_current_date(), 100000)
        if transaction_list is None:
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
        ''' 计算交易指标 '''
        stock_index = dict()
        transaction_list = self.sort(transaction_list)

        # MA5PRC/MA10PRC/MA20PRC
        self.ma_price(stock_index, transaction_list, 5)
        self.ma_price(stock_index, transaction_list, 10)
        self.ma_price(stock_index, transaction_list, 20)

        # MA5VOL/MA10VOL/MA20VOL
        self.ma_volume(stock_index, transaction_list, 5)
        self.ma_volume(stock_index, transaction_list, 10)
        self.ma_volume(stock_index, transaction_list, 20)

        # MACD指标
        self.macd(stock_index, transaction_list)

        # KDJ指标
        self.kdj(stock_index, transaction_list)

        # RSI指标
        self.rsi(stock_index, transaction_list)

        # BOLL指标
        self.boll(stock_index, transaction_list)

        # AD指标
        self.ad(stock_index, transaction_list)

        # ADOSC指标
        self.adosc(stock_index, transaction_list)

        # OBV指标
        self.obv(stock_index, transaction_list)

        # SAR指标
        self.sar(stock_index, transaction_list)

        # WILLR指标
        self.willr(stock_index, transaction_list)

        # CCI指标
        self.cci(stock_index, transaction_list)

        # EMA指标
        self.ema(stock_index, transaction_list)

        return stock_index

    def ma_price(self, stock_index, transaction_list, days):
        ''' 计算收盘价移动平均线
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
        '''

        index_name = "MA%dPRC" % (days)

        # 抽取收盘价列表
        close_price_list = list()
        for transaction in transaction_list:
            close_price_list.append(float(transaction["close_price"]))

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

    def ma_volume(self, stock_index, transaction_list, days):
        ''' 计算交易量移动平均线
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
        '''

        index_name = "MA%dVOL" % (days)

        # 抽取交易量列表
        volume_list = list()
        for transaction in transaction_list:
            volume_list.append(float(transaction["volume"]))

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
        ''' 计算DEMA指标 '''

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

    def macd(self, stock_index, transaction_list):
        ''' 计算MACD指标 '''

        # 抽取收盘价列表
        close_price_list = list()
        for transaction in transaction_list:
            close_price_list.append(float(transaction["close_price"]))

        diff, dea, macd = talib.MACD(
                pandas.Series(close_price_list),
                fastperiod=12, slowperiod=26, signalperiod=9)

        # 存储MACD值
        idx = 0
        while(idx < len(macd)):
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

    def kdj(self, stock_index, transaction_list):
        ''' 计算KDJ指标 '''

        # 抽取各列数据
        close_price_list = list()
        top_price_list = list()
        bottom_price_list = list()
        for transaction in transaction_list:
            close_price_list.append(float(transaction["close_price"]))
            top_price_list.append(float(transaction["top_price"]))
            bottom_price_list.append(float(transaction["bottom_price"]))

        k, d = talib.STOCH(
                pandas.Series(top_price_list),
                pandas.Series(bottom_price_list),
                pandas.Series(close_price_list),
                fastk_period=5,
                slowk_period=3, slowk_matype=0,
                slowd_period=3, slowd_matype=0)
        j = 3*k - 2*d

        # 存储KDJ值
        idx = 0
        while(idx < len(k)):
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

    def rsi(self, stock_index, transaction_list):
        ''' 计算RSI指标 '''

        # 抽取收盘价列表
        close_price_list = list()
        for transaction in transaction_list:
            close_price_list.append(float(transaction["close_price"]))

        rsi = talib.RSI(pandas.Series(close_price_list), timeperiod=14)

        # 存储RSI值
        idx = 0
        while(idx < len(rsi)):
            date = transaction_list[idx]["date"]

            if (math.isnan(rsi[idx])):
                idx += 1
                continue

            if date not in stock_index.keys():
                stock_index[date] = dict()
            stock_index[date]["RSI"] = rsi[idx]
            idx += 1
        return

    def obv(self, stock_index, transaction_list):
        ''' 计算OBV指标 '''

        # 抽取收盘价列表
        volume_list = list()
        close_price_list = list()
        for transaction in transaction_list:
            volume_list.append(float(transaction["volume"]))
            close_price_list.append(float(transaction["close_price"]))

        obv = talib.OBV(pandas.Series(close_price_list), pandas.Series(volume_list),)

        # 存储OBV值
        idx = 0
        while(idx < len(obv)):
            date = transaction_list[idx]["date"]

            if (math.isnan(obv[idx])):
                idx += 1
                continue

            if date not in stock_index.keys():
                stock_index[date] = dict()
            stock_index[date]["OBV"] = obv[idx]
            idx += 1
        return

    def boll(self, stock_index, transaction_list):
        ''' 计算BOLL指标 '''

        # 抽取收盘价列表
        close_price_list = list()
        for transaction in transaction_list:
            close_price_list.append(float(transaction["close_price"]))

        upper, middle, lower = talib.BBANDS(
                pandas.Series(close_price_list),
                timeperiod=20,
                # number of non-biased standard deviations from the mean
                nbdevup=2, nbdevdn=2,
                # Moving average type: simple moving average here
                matype=0)

        # 存储BOLL值
        idx = 0
        while(idx < len(upper)):
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

    def ad(self, stock_index, transaction_list):
        ''' 计算AD指标 '''

        # 抽取特征数据
        top_price_list = list()
        bottom_price_list = list()
        close_price_list = list()
        volume_list = list()
        for transaction in transaction_list:
            top_price_list.append(float(transaction["top_price"]))
            bottom_price_list.append(float(transaction["bottom_price"]))
            close_price_list.append(float(transaction["close_price"]))
            volume_list.append(float(transaction["volume"]))

        ad = talib.AD(
                pandas.Series(top_price_list),
                pandas.Series(bottom_price_list),
                pandas.Series(close_price_list),
                pandas.Series(volume_list))

        # 存储AD值
        idx = 0
        while(idx < len(ad)):
            date = transaction_list[idx]["date"]

            if (math.isnan(ad[idx])):
                idx += 1
                continue

            if date not in stock_index.keys():
                stock_index[date] = dict()
            stock_index[date]["AD"] = ad[idx]
            idx += 1
        return

    def adosc(self, stock_index, transaction_list):
        ''' 计算ADOSC指标 '''

        # 抽取特征数据
        top_price_list = list()
        bottom_price_list = list()
        close_price_list = list()
        volume_list = list()
        for transaction in transaction_list:
            top_price_list.append(float(transaction["top_price"]))
            bottom_price_list.append(float(transaction["bottom_price"]))
            close_price_list.append(float(transaction["close_price"]))
            volume_list.append(float(transaction["volume"]))

        adosc = talib.ADOSC(
                pandas.Series(top_price_list),
                pandas.Series(bottom_price_list),
                pandas.Series(close_price_list),
                pandas.Series(volume_list),
                fastperiod=3, slowperiod=10)

        # 存储ADOSC值
        idx = 0
        while(idx < len(adosc)):
            date = transaction_list[idx]["date"]

            if (math.isnan(adosc[idx])):
                idx += 1
                continue

            if date not in stock_index.keys():
                stock_index[date] = dict()
            stock_index[date]["ADOSC"] = adosc[idx]
            idx += 1
        return

    def sar(self, stock_index, transaction_list):
        ''' 计算SAR指标 '''

        # 抽取特征数据
        top_price_list = list()
        bottom_price_list = list()
        for transaction in transaction_list:
            top_price_list.append(float(transaction["top_price"]))
            bottom_price_list.append(float(transaction["bottom_price"]))

        sar = talib.SAR(
                pandas.Series(top_price_list),
                pandas.Series(bottom_price_list),
                acceleration=0, maximum=0)

        # 存储SAR值
        idx = 0
        while(idx < len(sar)):
            date = transaction_list[idx]["date"]

            if (math.isnan(sar[idx])):
                idx += 1
                continue

            if date not in stock_index.keys():
                stock_index[date] = dict()
            stock_index[date]["SAR"] = sar[idx]
            idx += 1
        return

    def willr(self, stock_index, transaction_list):
        ''' 计算WILLR指标 '''

        # 抽取特征数据
        top_price_list = list()
        bottom_price_list = list()
        close_price_list = list()
        for transaction in transaction_list:
            top_price_list.append(float(transaction["top_price"]))
            bottom_price_list.append(float(transaction["bottom_price"]))
            close_price_list.append(float(transaction["close_price"]))

        willr = talib.WILLR(
                pandas.Series(top_price_list),
                pandas.Series(bottom_price_list),
                pandas.Series(close_price_list),
                timeperiod=14)

        # 存储WILLR值
        idx = 0
        while(idx < len(willr)):
            date = transaction_list[idx]["date"]

            if (math.isnan(willr[idx])):
                idx += 1
                continue

            if date not in stock_index.keys():
                stock_index[date] = dict()
            stock_index[date]["WILLR"] = willr[idx]
            idx += 1
        return

    def cci(self, stock_index, transaction_list):
        ''' 计算CCI指标 '''

        # 抽取特征数据
        top_price_list = list()
        bottom_price_list = list()
        close_price_list = list()
        for transaction in transaction_list:
            top_price_list.append(float(transaction["top_price"]))
            bottom_price_list.append(float(transaction["bottom_price"]))
            close_price_list.append(float(transaction["close_price"]))

        cci = talib.CCI(
                pandas.Series(top_price_list),
                pandas.Series(bottom_price_list),
                pandas.Series(close_price_list),
                timeperiod=14)

        # 存储CCI值
        idx = 0
        while(idx < len(cci)):
            date = transaction_list[idx]["date"]

            if (math.isnan(cci[idx])):
                idx += 1
                continue

            if date not in stock_index.keys():
                stock_index[date] = dict()
            stock_index[date]["CCI"] = cci[idx]
            idx += 1
        return

    def ema(self, stock_index, transaction_list):
        ''' 计算EMA指标 '''

        # 抽取特征数据
        close_price_list = list()
        for transaction in transaction_list:
            close_price_list.append(float(transaction["close_price"]))

        ema = talib.EMA(
                pandas.Series(close_price_list),
                timeperiod=6)

        # 存储EMA值
        idx = 0
        while(idx < len(ema)):
            date = transaction_list[idx]["date"]

            if (math.isnan(ema[idx])):
                idx += 1
                continue

            if date not in stock_index.keys():
                stock_index[date] = dict()
            stock_index[date]["EMA"] = ema[idx]
            idx += 1
        return


    def sort(self, transaction_list):
        ''' 交易列表重排序: 按时间有序 '''

        sort_list = list()

        count = len(transaction_list)

        idx = 0
        while(idx < count):
            sort_list.append(transaction_list[count-idx-1])
            idx += 1

        return sort_list

    def update(self, stock_key, stock_index):
        ''' 更新股票交易指数 '''
        for date, data in stock_index.items():
            item = dict()
            item["stock_key"] = stock_key
            item["date"] = date
            item["data"] = json.dumps(data)
            self.data.set_transaction_index(item)
