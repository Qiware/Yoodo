# -*- coding:utf-8 -*-
# 君子爱财 取之YOODO!

import sys
import logging

from sklearn.preprocessing import MinMaxScaler
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split

sys.path.append("../../repo/lib/log")
from log import *

sys.path.append("../../repo/data")
from data import Data

sys.path.append("../../repo/model")
from model import *
from regressor import Regressor
from classifier import Classifier


sys.path.append("../../repo/lib/dtime")
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

    def get_potential_stock(self, date):
        ''' 获取潜力股 '''
        return self.data.get_potential_stock(date)

    def predict(self, model_type, date, days):
        ''' 数据预测
            @Param model_type: 预测模型(r:线性模型 c:分类模型)
            @Param date: 预测日期
            @Param days: 预测周期
        '''

        # 加载模型
        scaler = StandardScaler()
        model = None
        if model_type == MODEL_REGRESSOR:
            model = Regressor(days)
        elif model_type == MODEL_REGRESSOR:
            model = Classifier(days)

        # 获取股票列表
        stock_list = self.data.get_good_stock()
        for stock in stock_list:
            stock_key = stock["stock_key"]

            # 加载特征数据
            base_date, feature = self.data.load_feature(stock_key, date, days)
            if feature is None:
                logging.error("Load feature failed! stock_key:%s date:%s days:%d",
                             stock_key, date, days)
                continue

            # 进行结果预测
            feature_scaled = None
            if stock_key == "hkex:00001":
                feature_scaled = scaler.fit_transform(feature)
            else:
                feature_scaled = scaler.transform(feature)
            print(feature_scaled)

            ratio = model.predict(feature_scaled)

            print("predict: %s %s %s %s %s" % (stock_key, date, days, ratio[0], stock["name"]))
            logging.info("predict: %s %s %s %s %s", stock_key, date, days, ratio[0], stock["name"])

            # 更新预测结果
            self.data.update_predict(stock_key, date, days, base_date, float(ratio[0]))
        
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
        stock_list = self.data.get_good_stock()
        for stock in stock_list:
            stock_key = stock["stock_key"]

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

            print("Evaluate predict. stock_key:%s date:%s days:%s base_date:%s" %(stock_key, date, days, base["date"]))

            # 更新数据库
            self.data.update_predict_real(
                    stock_key, base["date"], days, real_price, real_ratio)

        return None
