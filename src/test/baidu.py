
# 从百度抓取交易数据

import json
import time
import urllib
import requests	# pip3 install requests

# 获取交易数据(交易时间、开盘价、最高价、最低价、收盘价、交易量、交易额)
# @Param code: 股票代码(如: 00700)
#HKEX_GET_CHART_DATA_URL = "https://finance.pae.baidu.com/selfselect/getstockquotation?all=1&code=%05d&isIndex=false&isBk=false&isBlock=false&isFutures=false&isStock=true&newFormat=1&is_kc=0&start_time=2004-06-16+00:00:00&ktype=1&group=quotation_kline_hk&finClientType=pc"
HKEX_GET_CHART_DATA_URL = "https://finance.pae.baidu.com/selfselect/getstockquotation?all=1&code=%05d&isIndex=false&isBk=false&isBlock=false&isFutures=false&isStock=true&newFormat=1&is_kc=0&start_time=2022-07-28+00:00:00&ktype=1&group=quotation_kline_hk&finClientType=pc"

HK_STOCK_CODE_MIN = 1 # 港股股票代码最小值
HK_STOCK_CODE_MAX = 3999 # 港股股票代码最大值

class Stock():
    def __init__(self):
        ''' 初始化 '''
        pass

    def get_khex_kline(self, stock_code, num):
        ''' 获取港交所K线数据
            @Param code: 股票代码
            @Param num: K线数量
        '''

        # 准备请求参数
        headers = {
                    'Content-Type' : 'application/json'
                }

        timestamp = int(time.time() * 1000)

        url =  HKEX_GET_CHART_DATA_URL % (int(stock_code))

        # 发起拉取请求
        rsp = requests.get(url=url, headers=headers)
        if rsp is None:
            return "Get data failed!"

        return rsp.text


if __name__ == "__main__":
    stock = Stock()

    print(stock.get_khex_kline("00700", 7))

    print("%05d" % (1))
