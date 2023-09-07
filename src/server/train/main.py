# -*- coding:utf-8 -*-
# 君子爱财 取之有道

import sys
import joblib
import logging

import numpy as np
from sklearn.model_selection import train_test_split

sys.path.append("../../lib/repo/log")
from log import *
sys.path.append("../../server/train/logic")
from logic.train import Trainer

from train import *

def usage():
    ''' 展示帮助信息 '''
    print("python3 ./main.py [model] [action] [date] [days]")
    print("     - model: 训练模型(r:线性模型; c:分类模型)")
    print("     - action: 操作行为(update:更新模型; rebuild:重建模型)")
    print("     - date: 预测时间")
    print("     - days: 预测周期")

if __name__ == "__main__":
    if len(sys.argv) != 5:
        usage()
        exit(-1)

    # 提取参数
    model = sys.argv[1] # 训练模型(r:线性模型 c:分类模型)
    action = sys.argv[2] # 操作行为(update:增量更新 rebuild:模型重建)
    date = sys.argv[3] # 训练数据截止日期(格式: YYYYMMDD)
    days = int(sys.argv[4]) # 周期(天)

    # 初始化日志
    log_init("../../../log/trainer.log")

    trainer = Trainer(model, days, action=="rebuild")
    if action == "update":
        print("Update model. date:%s days:%s" % (date, days))
        trainer.update(date, days)
    elif action == "rebuild":
        print("Rebuild model. date:%s days:%s" % (date, days))
        trainer.rebuild(date, days)
