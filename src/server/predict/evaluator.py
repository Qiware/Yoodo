# -*- coding:utf-8 -*-
# 君子爱财 取之有道

# 功能描述: 用于评估模型的预测结果准确性

import sys
import logging

sys.path.append("../../lib/repo/log")
from log import *

from predict import *

if __name__ == "__main__":

    date = sys.argv[1]
    days = int(sys.argv[2])

    log_init("../../../log/evalutor.log" + str(date) + "-" + str(days))

    # 新建对象
    pred = Predicter()

    # 结果预测
    pred.evaluate(date, days)
