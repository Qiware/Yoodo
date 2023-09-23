# -*- coding:utf-8 -*-
# 君子爱财 取之YOODO!

import sys
import logging

import matplotlib.pyplot as plt
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split
from sklearn.metrics import r2_score, mean_squared_error

sys.path.append("../../lib/model")
from model import *
from regressor import Regressor
from classifier import Classifier

sys.path.append("../../lib/data")
from data import Data

sys.path.append("../../lib/utils/log")
from log import *

# 拉取训练交易数据条目
GET_TRANSACTION_MAX_NUM = 120


# 股票预测
class Trainer():
    def __init__(self, model_type, days, is_build=False):
        """ 初始化
            @Param model_type: 训练模型(r:回归模型; c:分类模型)
            @Param days: 预测周期
            @Param is_build: 是否重建
        """
        # 指定数据源
        self.data = Data()
        self.scaler = StandardScaler()
        self.model_type = model_type
        if model_type == MODEL_REGRESSOR:
            self.model = Regressor(days, is_build)
        elif model_type == MODEL_CLASSIFIER:
            self.model = Classifier(days, is_build)

    def train(self, date, days, is_build=False):
        """ 模型训练 """
        # 加载训练数据
        feature, target = self.data.load_train_data(date, days)

        # 划分训练集和测试集
        x_train, x_test, y_train, y_test = train_test_split(
            feature, target, test_size=0.001, random_state=1)

        # 训练模型
        x_train_scaled = self.scaler.fit_transform(x_train)

        self.model.fit(x_train_scaled, y_train)  # 训练

        self.model.dump()  # 固化模型

        # 模型评估
        x_test_scaled = self.scaler.transform(x_test)

        y_predict = self.model.predict(x_test_scaled)

        # 计算R2值
        r2 = r2_score(y_test, y_predict)

        # 计算MSE值
        mse = mean_squared_error(y_test, y_predict)

        print('R2值为：', r2)
        print('MSE值为：', mse)

        # 预测结果
        # 获取股票列表
        for date in range(20230901, 20230918):
            stock_list = self.data.get_good_stock()
            for stock in stock_list:
                stock_key = stock["stock_key"]

                # 加载特征数据
                base_date, feature = self.data.load_feature(stock, date, days)
                if feature is None:
                    logging.error("Load feature failed! stock_key:%s date:%s days:%d",
                                  stock_key, date, days)
                    continue

                # 进行结果预测
                feature_scaled = self.scaler.transform(feature)

                ratio = self.model.predict(feature_scaled)

                print("predict: %s %s %s %s %s" % (stock_key, date, days, ratio[0], stock["name"]))
                logging.info("predict: %s %s %s %s %s", stock_key, date, days, ratio[0], stock["name"])

                # 更新预测结果
                self.data.update_predict(stock_key, date, days, base_date, float(ratio[0]))

        print('R2值为：', r2)
        print('MSE值为：', mse)

        """结果可视化"""
        xy = range(0, len(y_test))
        plt.figure(figsize=(4, 3))
        plt.scatter(xy, y_test, color="red", label="Sample Point", linewidth=3)
        plt.plot(xy, y_predict, color="orange", label="Predict line", linewidth=2)
        plt.legend()
        plt.show()

        return None

    def build(self, date, days):
        """ 构建训练模型 """

        # 生成训练数据
        self.data.gen_train_data(self.model_type, date, days, GET_TRANSACTION_MAX_NUM)

        # 进行模型训练
        self.train(date, days, True)
