# encoding=utf-8

# 从富途牛牛抓取交易数据

import json
import time
import urllib
import requests	# pip3 install requests

# 获取最近一个月的交易数据
FTNN_TOKEN = "yPqZ0RnuejLBn7f6mfJRIrko"

# 获取交易数据(交易时间、开盘价、最高价、最低价、收盘价、交易量、交易额)
# @Param stockId: 股票ID
# @Param marketType: 市场类型
# @Param marketCode: 市场代码
# @Param instrumentType: 
KLINE_URL = "https://www.futunn.com/quote-api/quote-v2/get-kline?stockId=54047868453564&marketType=1&type=2&marketCode=1&instrumentType=3"

class Stock():
    def __init__(self):
        ''' 初始化 '''
        pass

    def get_kline(self, stock_code, num):
        ''' 获取股票K线数据
            @Param code: 股票代码
            @Param num: K线数量
        '''

        session = requests.session()

        session.cookies["cipher_device_id"] = "1690550590136935"
        session.cookies["device_id"] = "1690550590136935"
        session.cookies["_gid"] = "GA1.2.1888744140.1690550591"
        session.cookies["csrfToken"] = FTNN_TOKEN

        print("cookie:", session.cookies)
        
        # 准备请求参数
        headers = {
                    'Content-Type' : 'application/json'
                }

        timestamp = int(time.time() * 1000)

        url =  KLINE_URL

        # 发起拉取请求
        rsp = session.get(url=url, headers=headers)
        if rsp is None:
            return "Get data failed!"

        return rsp.text


if __name__ == "__main__":
    stock = Stock()

    stock_code = 700
    print(stock.get_kline(stock_code, 7))
