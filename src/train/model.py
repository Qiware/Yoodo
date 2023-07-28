# encoding=utf-8

import os
import sys
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

class Model():
    def __init__(self, days, is_rebuild=False):
        ''' 初始化
            @Param days: 预测days的模型
            @Param is_rebuild: 是否重建模型
        '''
        self.days = days
        if is_rebuild:
            self.model = self.new()
        else:
            self.model = self._load()

    def new(self):
        ''' 新建模型 '''
        return MLPRegressor(
                hidden_layer_sizes=(500, 100),
                activation='tanh', solver='adam',
                alpha=0.0001, batch_size='auto',
                learning_rate='constant',
                learning_rate_init=0.001,
                power_t=0.5, max_iter=5000,
                shuffle=True, random_state=None,
                tol=0.0001, verbose=False,
                warm_start=False, momentum=0.9,
                nesterovs_momentum=True,
                early_stopping=False,
                validation_fraction=0.1,
                beta_1=0.9, beta_2=0.999,
                epsilon=1e-08, n_iter_no_change=10, max_fun=15000)

    def ratio(self, start_val, end_val):
        ''' 波动比率
            @Param start_val: 开始值
            @Param end_val: 结束值
        '''
        return (end_val - start_val) / start_val * 100

    def _gen_model_fpath(self, days):
        ''' 生成预测模型的路径 '''
        return "./model/%ddays.mod" % (int(days))

    def _load(self):
        ''' 加载预测模型 '''

        fpath = self._gen_model_fpath(self.days)
        if not os.path.isfile(fpath):
            logging.error("Model is not exist! fpath:%s", fpath)
            return self.new()

        # 加载模型
        return joblib.load(fpath)

    def predict(self, feature):
        ''' 预测结果 '''
        return self.model.predict(feature)

    def dump(self):
        ''' DUMP模型 '''
        joblib.dump(self.model, self._gen_model_fpath(self.days))

    def fit(self, feature, target):
        ''' 模型训练 '''
        return self.model.fit(feature, target)
