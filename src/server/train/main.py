# -*- coding:utf-8 -*-
# 君子爱财 取之有道

import sys
import joblib
import logging

import numpy as np
from sklearn.model_selection import train_test_split

sys.path.append("../../lib/repo/log")
from log import *

from train import *

if __name__ == "__main__":
    # 提取参数
    action = sys.argv[1] # 行为(update:增量更新 rebuild:模型重建)
    date = sys.argv[2] # 训练数据截止日期(格式: YYYYMMDD)
    days = int(sys.argv[3]) # 周期(天)

    # 初始化日志
    log_init("../../../log/trainer.log")

    trainer = Trainer()
    if action == "update":
        print("Update model. date:%s days:%s" % (date, days))
        trainer.update(date, days)
    elif action == "rebuild":
        print("Rebuild model. date:%s days:%s" % (date, days))
        trainer.rebuild(date, days)
