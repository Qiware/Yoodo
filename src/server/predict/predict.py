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