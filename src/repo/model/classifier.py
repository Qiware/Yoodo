# encoding=utf-8
# 君子爱财 取之YOODO!

# 训练模型: 分类模型

from sklearn.neural_network import MLPClassifier

from model import *

class Classifier(Model):
    def __init__(self, days, is_build=False):
        ''' 初始化
            @Param days: 预测days的模型
            @Param is_build: 是否新建模型
        '''
        self.days = days
        if is_build:
            self.model = self._new()
        else:
            self.model = self._load()

    def _new(self):
        ''' 新建模型 '''
        return MLPClassifier(
                activation='tanh', # tanh, relu, logistic
                alpha=1e-4,
                hidden_layer_sizes=(1000, 1000, 1000, 1000, 1000),
                random_state=1,
                max_iter=100000,
                verbose=True,
                learning_rate="invscaling",
                learning_rate_init=0.0001)

    def _gen_model_fpath(self, days):
        ''' 生成分类预测模型的路径 '''
        return "../../../model/c-%ddays.mod" % (int(days))
