
# 从港交所抓取交易数据

import json
import time
import urllib
import requests	# pip3 install requests

# 获取最近一个月的交易数据
HKEX_TOKEN = "evLtsLsBNAUVTPxtGqVeG1crQXMREqQ%2B0z2ejZuywjQGTLuNt9HwCQb0WW7lSmgy"

# 时间间隔
HKEX_TODAY = 1 # 获取最近今天的数据(时)
HKEX_1MONTH = 2 # 获取最近1个月的数据(天)
HKEX_3MONTH = 3 # 获取最近3个月的数据(天)
HKEX_6MONTH = 4 # 获取最近6个月的数据(天)
HKEX_1YEAR = 5 # 获取最近1年的数据(天)
HKEX_2YEAR = 6 # 获取最近2年的数据(天)
HKEX_5YEAR = 7 # 获取最近5年的数据(周)
HKEX_10YEAR = 8 # 获取最近5年的数据(月)

# 获取交易数据(交易时间、开盘价、最高价、最低价、收盘价、交易量、交易额)
# @Param int: 时间间隔
# @Param ric: 股票代码
# @Param token: TOKEN
# @Param callback: 回调
# @Param _: 时间戳(14位)
#HKEX_GET_CHART_DATA_URL = "https://www1.hkex.com.hk/hkexwidget/data/getchartdata2?hchart=1&span=6&int=%d&ric=%04d.HK&token=%s&qid=1690163621846&callback=jQuery35106409699179416692_1690163264203&_=1690163264217"
HKEX_GET_CHART_DATA_URL = "https://www1.hkex.com.hk/hkexwidget/data/getchartdata2?hchart=1&span=6&int=%d&ric=%04d.HK&token=%s&qid=1690163621846&callback=jQuery35106409699179416692_%d&_=%d"

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

        url =  HKEX_GET_CHART_DATA_URL % (HKEX_2YEAR, int(stock_code), HKEX_TOKEN, timestamp, timestamp)

        # 发起拉取请求
        rsp = requests.get(url=url, headers=headers)
        if rsp is None:
            return "Get data failed!"

        return rsp.text


if __name__ == "__main__":
    stock = Stock()

    stock_code = HK_STOCK_CODE_MIN
    while (stock_code <= HK_STOCK_CODE_MAX):
        print("Stock code: ", stock_code)
        print(stock.get_khex_kline(stock_code, 7))
        stock_code += 1
