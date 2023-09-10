# -*- coding:utf-8 -*-

import talib
import akshare

class Index():
    # 计算MA指数
    def ma():
        pass

    # 计算RSI指数
    def rsi():
        pass

    # 计算KDJ指数
    def kdj():
        pass

    # 计算MACD指数
    def macd():
        pass

if __name__ == "__main__":
    # 计算MA指标
    df = akshare.stock_zh_a_hist(
            symbol='000001',
            period='daily',
            start_date='20230101',
            end_date='20230906',
            adjust='')
    print(df.head())
    print(len(df), df["收盘"])

    ma =  talib.MA(df["收盘"], timeperiod=5)
    print(len(ma))
    print(ma)

    # MACD指标
    diff, dea, macd = talib.MACD(df["收盘"], fastperiod=12, slowperiod=26, signalperiod=9)
    print("diff:", diff)
    print("dea:", dea)
    print("macd:", macd)

