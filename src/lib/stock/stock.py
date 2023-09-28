# -*- coding:utf-8 -*-
# 君子爱财 取之YOODO!

def get_stock_code(stock_key):
    """ 获取股票代码 """
    fields = stock_key.split(":")
    exchange = fields[0]
    stock_code = fields[1]
    return exchange, stock_code
