# -*- coding:utf-8 -*-
# 君子爱财 取之YOODO!

def get_stock_code_by_key(stock_key):
    """ 通过股票KEY获取股票代码 """
    segment = stock_key.split(":")
    stock_code = segment[1]
    return stock_code
