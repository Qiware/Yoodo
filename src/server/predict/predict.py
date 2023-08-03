# -*- coding:utf-8 -*-
# 君子爱财 取之有道

import sys
import time
import joblib
import logging

import numpy as np
import matplotlib.pyplot as plt
from sklearn.metrics import r2_score
from sklearn.preprocessing import PolynomialFeatures
from sklearn.linear_model import LinearRegression
from sklearn.neural_network import MLPRegressor
from sklearn.model_selection import train_test_split

sys.path.append("../../lib/log")
from log import *

sys.path.append("../../lib/data")
from data import Data

sys.path.append("../../lib/model")
from model import Model


sys.path.append("../../lib/repo/dtime")
from dtime import *

# 拉取训练交易数据条目
GET_TRANSACTION_NUM = 1000

# 单条训练样本拥有的交易数据条目
TRAIN_DATA_TRANSACTION_NUM = 30

# 股票预测
class Predicter():
    def __init__(self):
        ''' 初始化 '''
        # 数据处理
        self.data = Data()

    def predict(self, date, days):
        ''' 数据预测 '''

        # 加载模型
        model = Model(days)

        # 获取股票列表
        stock_list = self.data.get_all_stock()
        for stock in stock_list:
            stock_key = stock["key"]

            # 加载特征数据
            feature = self.data.load_feature(stock_key, date, days)
            if feature is None:
                logging.info("Load feature failed! stock_key:%s date:%s days:%d", stock_key, date, days)
                continue

            # 进行结果预测
            ratio = model.predict(feature)

            print("predict: %s %s %s %s %s" % (stock_key, date, days, ratio[0], stock["name"]))
            logging.info("predict: %s %s %s %s %s", stock_key, date, days, ratio[0], stock["name"])

            # 更新预测结果
            self.data.update_predict(stock_key, date, days, float(ratio[0]))
        
        return None

    def evaluate(self, date, days):
        ''' 评估预测结果
            @Param date: 被预测日期
            @Param days: 预测周期
        '''

        if days == 0:
            logging.error("Parameter 'days' invalid! days:%d", days)
            return None

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
            start_date = int(base["date"])
            start_timestamp = date_to_timestamp(start_date)
            end_date = int(transaction_list[days-1]["date"])
            end_timestamp = date_to_timestamp(end_date)

            timestamp = start_timestamp
            while(timestamp < end_timestamp):
                self.data.update_predict_real(stock_key, date, days, real_price, real_ratio)
                timestamp += 86400

        return None
