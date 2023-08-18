# -*- coding:utf-8 -*-
# 君子爱财 取之有道

import sys
import joblib
import logging

import matplotlib.pyplot as plt
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split
from sklearn.metrics import r2_score, mean_squared_error

sys.path.append("../../lib/repo/log")
from log import *

sys.path.append("../../lib/data")
from data import Data

sys.path.append("../../lib/model")
from regressor import Regressor
from classifier import Classifier

# 拉取训练交易数据条目
GET_TRANSACTION_MAX_NUM = 1000

# 股票预测
class Trainer():
    def __init__(self):
        # 指定数据源
        self.data = Data()

    def train(self, date, days, is_rebuild=False):
        ''' 模型训练 '''
        # 加载训练数据
        feature, target = self.data.load_train_data(date, days)

        # 划分训练集和测试集
        x_train, x_test, y_train, y_test = train_test_split(
                feature, target, test_size=0.05, random_state=1)

        # 新建模型
        scaler = StandardScaler()

        #model = Classifier(days, is_rebuild)
        model = Regressor(days, is_rebuild)

        feature_num = int(len(x_train) / TRAIN_GROUP_NUM)

        print("Train processing ...")

        # 训练模型
        x_train_scaled = scaler.fit_transform(x_train)

        model.fit(x_train_scaled, y_train) # 训练

        model.dump() # 固化模型

        # 模型评估
        x_test_scaled = scaler.transform(x_test)

        y_predict = model.predict(x_test_scaled)

        # 计算R2值
        r2 = r2_score(y_test, y_predict)

        # 计算MSE值
        mse = mean_squared_error(y_test, y_predict)

        print('模型评估值为：', r2)
        print('R2值为：', r2)
        print('MSE值为：', mse)

        '''结果可视化'''
        xy = range(0, len(y_test))
        plt.figure(figsize=(8,6))
        plt.scatter(xy, y_test,color="red",label="Sample Point",linewidth=3)
        plt.plot(xy, y_predict,color="orange",label="Predict line",linewidth=2)
        plt.legend()
        plt.show()

        return None

    def rebuild(self, date, days):
        ''' 重建模型 '''

        # 生成训练数据
        self.data.gen_train_data_by_days(date, days, GET_TRANSACTION_MAX_NUM)

        # 进行模型训练
        self.train(date, days, True)

    def update(self, date, days):
        ''' 更新模型 '''

        # 生成训练数据
        # 注意事项: 不仅需要训练数据, 还有一个基准数据和目标结果数据, 因此是+2.
        self.data.gen_train_data_by_days(date, days, days*(TRAIN_DATA_TRANSACTION_NUM+2))

        # 进行模型训练
        self.train(date, days, False)
