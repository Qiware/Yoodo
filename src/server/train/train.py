# -*- coding:utf-8 -*-
# 君子爱财 取之有道

import sys
import joblib
import logging

import numpy as np
import matplotlib.pyplot as plt
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

# 训练分组数
TRAIN_GROUP_NUM = 100

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
        feature_train, feature_test, target_train, target_test = train_test_split(
                feature, target, test_size=0.05, random_state=1)

        # 新建模型
        model = Regressor(days, is_rebuild)

        feature_num = int(len(feature_train) / TRAIN_GROUP_NUM)

        print("Train processing ...")

        group_index = 0
        while(True):
            # 计算偏移量
            begin = group_index * feature_num
            if begin >= len(feature_train):
                break
            end = (group_index + 1) * feature_num

            # 训练模型
            if end <= len(feature_train):
                model.fit(feature_train[begin:end], target_train[begin:end]) # 训练
                print("Train processing %f ..." % (float(end)/len(feature_train)* 100))
            else:
                model.fit(feature_train[begin:], target_train[begin:]) # 训练
                print("Train processing 100...")
            group_index += 1

        model.dump() # 固化模型

        # 模型评估
        predict_test = model.predict(feature_test)

        # 计算R2值
        r2 = r2_score(target_test, predict_test)

        # 计算MSE值
        mse = mean_squared_error(target_test, predict_test)

        print('R2值为：', r2)
        print('MSE值为：', mse)

        '''结果可视化'''
        xx = range(0, len(target_test))
        plt.figure(figsize=(8,6))
        plt.scatter(xx, target_test,color="red",label="Sample Point",linewidth=3)
        plt.plot(xx, predict_test,color="orange",label="Fitting Line",linewidth=2)
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
