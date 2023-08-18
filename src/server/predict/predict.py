# -*- coding:utf-8 -*-
# 君子爱财 取之有道

import sys
import logging

from sklearn.preprocessing import MinMaxScaler
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split

sys.path.append("../../lib/log")
from log import *

sys.path.append("../../lib/data")
from data import Data

sys.path.append("../../lib/model")
#from model import Model
from regressor import Regressor
from classifier import Classifier


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

        # 特征归一化处理
        scaler = StandardScaler()
        #scaler = MinMaxScaler()

        # 加载训练数据
        feature, target = self.data.load_train_data("20230701", days)

        # 划分训练集和测试集
        feature_train, feature_test, target_train, target_test = train_test_split(
                feature, target, test_size=0.05, random_state=1)

        feature_train_scaled = scaler.fit_transform(feature_train)

        # 加载模型
        #model = Classifier(days)
        model = Regressor(days)

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
            #print(feature)
            feature_scaled = scaler.transform(feature)
            print(feature_scaled)

            #feature_inverse_scaled = scaler.inverse_transform(feature_scaled)
            #print(feature_inverse_scaled)

            ratio = model.predict(feature_scaled)
            #ratio = [0]

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
