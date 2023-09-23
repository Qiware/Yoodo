# -*- coding:utf-8 -*-
# 君子爱财 取之YOODO!

# 功能描述: 用于评估模型的预测结果准确性

import sys

sys.path.append("../../lib/utils/log")
from log import log_init

from predict import *

if __name__ == "__main__":

    date = sys.argv[1]
    days = int(sys.argv[2])

    log_init("../../../log/evalutor.log")

    # 新建对象
    pred = Predicter()

    # 结果预测
    pred.evaluate(date, days)
