# encoding=utf-8

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
    def __init__(self, date, days):
        self.date = date
        self.days = days
        self.predict_model = None

    def ratio(self, start_val, end_val):
        ''' 波动比率
            @Param start_val: 开始值
            @Param end_val: 结束值
        '''
        return (end_val - start_val) / start_val * 100

    def gen_model_fpath(self, date, days):
        ''' 生成预测模型的路径 '''
        return "./model/%s-%ddays.mod" % (str(date), int(days))

    def load_model(self):
        ''' 加载预测模型 '''

        # 模型已加载, 则直接返回.
        if self.predict_model is not None:
            return self.predict_model

        fpath = self.gen_model_fpath(self.date, self.days)
        if not os.exists(fpath):
            return None

        # 加载模型
        self.predict_model = joblib.load(fpath)

        return self.predict_model

    def get_predict_model(self):
        ''' 获取预测模型 '''
        return self.predict_model
