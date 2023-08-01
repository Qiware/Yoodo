# -*- coding:utf-8 -*-
# 君子爱财 取之有道

# 功能描述: 用于评估模型的预测结果准确性

import sys
import time
import joblib
import logging

sys.path.append("../lib/log")
from log import *

from data import Data

# 拉取训练交易数据条目
GET_TRANSACTION_NUM = 1000

# 单条训练样本拥有的交易数据条目
TRAIN_DATA_TRANSACTION_NUM = 30

# 模型效果评估
class Evaluator():
    def __init__(self):
        ''' 初始化 '''
        # 数据处理
        self.data = Data()

    def evaluate(self, date, days):
        ''' 模型评估
            @Param date: 被预测日期
            @Param days: 预测周期
        '''

        # 获取股票列表
        stock_list = self.data.get_all_stock()
        for stock in stock_list:
            stock_key = stock["key"]

            print("Evaluate predict. stock_key:%s date:%s days:%s" %(stock_key, date, days))

            # 获取股票最近的(days + 1)条记录
            transaction_list = self.data.get_transaction_list(
                    stock_key, date, days+1)
            if (transaction_list is None):
                logging.error("Get transaction list failed! stock_key:%s date:%s days:%d",
                              stock_key, date, days)
                continue
            elif len(transaction_list) < (days+1):
                logging.error("Get transaction list not enough! stock_key:%s date:%s days:%d",
                              stock_key, date, days)
                continue

            # 计算实际涨幅数据
            base = transaction_list[days] # 基准交易
            lastest = transaction_list[0] # 被预测日期最后一条交易

            real_price = lastest["close_price"]
            real_ratio = (lastest["close_price"] - base["close_price"]) / base["close_price"] * 100

            # 更新数据库
            self.data.update_predict_real(stock_key, base["date"], days, real_price, real_ratio)

        return None

if __name__ == "__main__":

    date = int(sys.argv[1])
    days = int(sys.argv[2])

    log_init("../../log/evalutor.log" + str(date) + "-" + str(days))

    # 新建对象
    ev = Evaluator()

    # 结果预测
    ev.evaluate(date, days)
