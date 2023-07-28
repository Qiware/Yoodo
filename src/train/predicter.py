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

sys.path.append("../lib/log")
from log import *
sys.path.append("../lib/database")
from database import *
from model import Model

# 拉取训练交易数据条目
GET_TRANSACTION_NUM = 1000

# 单条训练样本拥有的交易数据条目
TRAIN_DATA_TRANSACTION_NUM = 30

# 股票预测
class Predicter():
    def __init__(self, date, days):
        ''' 初始化
            @Param date: 预测的起始日期
            @Param days: 预测days天的趋势
        '''
        # 基础信息
        self.date = date
        self.days = days
        # 连接数据库
        self.database = Database()
        # 模型模块
        self.model = Model(date, days)

    def group_transaction_by_days(self, transaction_list, days):
        ''' 交易数据间隔days分组 '''

        transaction_group = list()
        group_num = int(len(transaction_list) / days)

        num = int(0)
        while (num < group_num):
            index = num * days

            item = dict()

            item["stock_key"] = transaction_list[index]["stock_key"] # 股票KEY
            item["open_price"] = transaction_list[index]["open_price"] # 开盘价(取第一天开盘价)
            item["close_price"] = transaction_list[index+days-1]["close_price"] # 收盘价(取最后一天收盘价)

            item["top_price"] = transaction_list[index]["top_price"] # 最高价
            item["bottom_price"] = transaction_list[index]["bottom_price"] # 最低价
            item["volume"] = transaction_list[index]["volume"] # 交易量
            item["turnover"] = transaction_list[index]["turnover"] # 交易额

            index += 1
            while (index < (num+1)*days):
                item["top_price"] = max(item["top_price"], transaction_list[index]["top_price"])
                item["bottom_price"] = min(item["bottom_price"], transaction_list[index]["bottom_price"])
                item["volume"] = item["volume"] + transaction_list[index]["volume"]
                item["turnover"] = item["turnover"] + transaction_list[index]["turnover"]
                index += 1
            logging.debug("Transaction group data: %s", item)
            transaction_group.append(item)
            num += 1

        logging.info("transaction_list:%d transaction_group:%d",
                     len(transaction_list), len(transaction_group))

        return transaction_group

    def gen_feature_by_transaction_list(self, stock_key, transaction_list):
        ''' 通过交易列表生成特征数据. 返回格式: [x1, x2, x3, ...]
            @Param stock_key: 股票KEY
            @Param transaction_list: 交易数据
            @Param fname: 训练数据输出文件
        '''
        # 判断参数合法性
        if len(transaction_list) < (TRAIN_DATA_TRANSACTION_NUM+1):
            logging.error("Transaction data not enough! stock_key:%s", stock_key)
            return None

        feature = list()

        # 生成特征数据(注意: 最有一个作为起始基准)
        index = TRAIN_DATA_TRANSACTION_NUM - 1
        while (index >= 0):
            curr = transaction_list[index]
            prev = transaction_list[index+1]
            feature.append(self.model.ratio(prev["close_price"], curr["open_price"]))
            feature.append(self.model.ratio(prev["close_price"], curr["close_price"]))
            feature.append(self.model.ratio(prev["close_price"], curr["top_price"]))
            feature.append(self.model.ratio(prev["close_price"], curr["bottom_price"]))
            feature.append(self.model.ratio(prev["volume"], curr["volume"]))
            feature.append(self.model.ratio(prev["turnover"], curr["turnover"]))
            index -= 1

        logging.info("Generate feature by transaction list success. stock_key:%s transaction_list:%d feature:%d",
                     stock_key, len(transaction_list), len(feature))

        return feature

    def load_featue(self, stock_key, date, days):
        ''' 加载特征数据 '''
        feature = list()

        # 查询所需数据
        transaction_list = self.database.get_transaction(stock_key, date, TRAIN_DATA_TRANSACTION_NUM*(days+1))

        # 交易数据聚合分组
        transaction_group = self.group_transaction_by_days(transaction_list, days)

        # 生成训练样本
        item = self.gen_feature_by_transaction_list(stock_key, transaction_group)
        if item is None:
            logging.error("Generate feature by transaction list failed! stock_key:%s transaction_list:%d", stock_key, len(transaction_list))
            return None

        feature.append(item)

        return feature

    def predict(self, date, days):
        ''' 数据预测 '''

        # 获取股票列表
        stock_list = self.database.get_all_stock()
        for stock in stock_list:
            stock_key = stock["key"]

            # 加载特征数据
            feature = self.load_featue(stock_key, date, days)
            if feature is None:
                logging.info("Load feature failed! stock_key:%s date:%s days:%d", stock_key, date, days)
                continue

            # 进行结果预测
            ratio = self.model.get_predict_model().predict(feature)

            print("predict: %s %s %s" % (stock_key, ratio[0], stock["name"]))
            logging.info("predict: %s %s %s", stock_key, ratio[0], stock["name"])

            # 更新预测结果
            self.update_predict(stock_key, date, days, float(ratio[0]))
        
        return None

    def update_predict(self, stock_key, date, days, ratio):
        ''' 更新预测数据 '''
        # 获取最新价格
        curr_price = float(0)
        transaction_list = self.database.get_transaction(stock_key, date, 1)
        if len(transaction_list) != 0:
            curr_price = transaction_list[0]["close_price"]

        # 准备数据
        data = dict()
        data["stock_key"] = stock_key
        data["date"] = date
        data["days"] = int(days)
        data["curr_price"] = curr_price
        data["price"] = curr_price + curr_price*(ratio/100)
        data["ratio"] = ratio

        curr_timestamp = int(time.time())
        data["create_time"] = time.localtime(curr_timestamp)
        data["update_time"] = time.localtime(curr_timestamp)

        # 更新数据
        self.database.set_predict(data)

        return

if __name__ == "__main__":

    log_init("../../log/predicter.log")

    date = int(sys.argv[1])
    days = int(sys.argv[2])

    pred = Predicter(date, days)

    date = sys.argv[1]

    # 进行模型训练
    pred.predict(date, days)
