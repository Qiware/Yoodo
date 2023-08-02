# -*- coding:utf-8 -*-
# 君子爱财 取之有道

import sys
import joblib
import logging

import numpy as np
from sklearn.model_selection import train_test_split

sys.path.append("../../lib/repo/log")
from log import *

sys.path.append("../../lib/data")
from data import Data

sys.path.append("../../lib/model")
from model import Model

# 拉取训练交易数据条目
GET_TRANSACTION_MAX_NUM = 1000

# 单条训练样本拥有的交易数据条目
TRAIN_DATA_TRANSACTION_NUM = 30

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
        model = Model(days, is_rebuild)

        model.fit(feature_train, target_train) # 训练

        model.dump() # 固化模型

        # 模型评估
        predict_test = model.predict(feature_test)

        index = 0
        right_count = 0
        wrong_count = 0
        zero_count = 0
        while (index < len(target_test)):
            mul = predict_test[index] * target_test[index]
            if mul > 0:
                right_count += 1
            elif (mul == 0) and ((target_test[index] == 0) or ((predict_test[index] == 0) and (target_test[index] > 0))):
                zero_count += 1
            else:
                wrong_count += 1
            logging.debug("feature[%d] %s", index, feature_test[index])
            logging.debug("compare[%d] %s:%s", index, predict_test[index], target_test[index])
            index += 1

        sample_count = right_count + wrong_count + zero_count
        logging.debug("right_count:%d/%f wrong_count:%d/%f zero_count:%d/%f",
                      right_count, float(right_count)/sample_count,
                      wrong_count, float(wrong_count)/sample_count,
                      zero_count, float(zero_count)/sample_count)

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


if __name__ == "__main__":
    # 提取参数
    action = sys.argv[1]
    date = sys.argv[2]
    days = int(sys.argv[3])

    # 初始化日志
    log_init("../../log/trainer.log"+str(date)+"-"+str(days))

    trainer = Trainer()
    if action == "update":
        print("Update model. date:%s days:%s" % (date, days))
        trainer.update(date, days)
    elif action == "rebuild":
        print("Rebuild model. date:%s days:%s" % (date, days))
        trainer.rebuild(date, days)