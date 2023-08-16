# encoding=utf-8

# 训练模型: 分类模型

from sklearn.neural_network import MLPClassifier

from model import *

class Classifier(Model):
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
        return MLPClassifier(
                hidden_layer_sizes=(500, 400, 300, 200, 200, 200),
                activation='tanh',
                max_iter=10000,
                learning_rate = "adaptive",
                learning_rate_init=0.0001)
