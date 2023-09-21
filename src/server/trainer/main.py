# -*- coding:utf-8 -*-
# 君子爱财 取之YOODO!

import sys

sys.path.append("../../repo/lib/log")
from log import *

sys.path.append("../../server/trainer/logic")
from logic.trainer import Trainer

from trainer import *


def usage():
    """ 展示帮助信息 """
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
    model = sys.argv[1]  # 训练模型(r:线性模型 c:分类模型)
    action = sys.argv[2]  # 操作行为(update:增量更新 rebuild:模型重建)
    date = sys.argv[3]  # 训练数据截止日期(格式: YYYYMMDD)
    days = int(sys.argv[4])  # 训练模型预测周期(时间不宜过长)

    # 初始化日志
    log_init("../../../log/trainer.log")

    trainer = Trainer(model, days, action == "build")
    if action == "build":
        print("Build model. date:%s days:%s" % (date, days))
        trainer.build(date, days)
