# -*- coding:utf-8 -*-
# 君子爱财 取之YOODO!

import sys
import logging
import json
import math
import talib
import pandas

sys.path.append("../../repo/data")
from data import Data
sys.path.append("../../repo/lib/log")
from log import *
sys.path.append("../../repo/lib/dtime")
from dtime import *

class Analyzer():
    def __init__(self):
        self.data = Data()

    def rebuild(self, start_stock_code):
        ''' 重建所有股票指标 '''

        # 获取股票列表
        stock_list = self.data.get_all_stock()
        for stock in stock_list:
            fields = stock["stock_key"].split(":")
            stock_code = int(fields[1])
            if stock_code < start_stock_code:
                continue

            # 获取交易数据(按时间逆序)
            transaction_list = self.data.get_transaction_list(
                    stock["stock_key"], get_current_date(), 100000)

            # 计算交易指标
            stock_index = self.compute(transaction_list)

            # 更新交易指标
            self.update(stock["stock_key"], stock_index)

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

        print(stock_index)

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
        print("计算AD指标")

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
        print("计算ADOSC指标")

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
        print("计算SAR指标")

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
        print("计算WILLR指标")

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
        print("计算CCI指标")

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
        print("计算EMA指标")

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
