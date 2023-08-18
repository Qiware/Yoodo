# encoding=utf-8

# 训练模型: 基类型

import os
import sys
import joblib
import logging

from sklearn.neural_network import MLPRegressor

sys.path.append("../lib/log")
from log import *

class Model():
    def __init__(self, days, is_rebuild=False):
        ''' 初始化
            @Param days: 预测days的模型
            @Param is_rebuild: 是否重建模型
        '''
        self.days = days
        if is_rebuild:
            self.model = self._new()
        else:
            self.model = self._load()

    def _new(self):
        ''' 新建模型 '''
        return MLPRegressor(
                hidden_layer_sizes=(500, 400, 300, 200, 200, 200),
                activation='tanh',
                max_iter=10000,
                learning_rate = "adaptive",
                learning_rate_init=0.0001)

    def ratio(self, start_val, end_val):
        ''' 波动比率
            @Param start_val: 开始值
            @Param end_val: 结束值
        '''
        return (end_val - start_val) / start_val * 100

    def _gen_model_fpath(self, days):
        ''' 生成预测模型的路径 '''
        return "../../../model/%ddays.mod" % (int(days))

    def _load(self):
        ''' 加载预测模型 '''

        fpath = self._gen_model_fpath(self.days)
        if not os.path.isfile(fpath):
            logging.error("Model is not exist! fpath:%s", fpath)
            return self._new()

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
