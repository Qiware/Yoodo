# -*- coding:utf-8 -*-
# 君子爱财 取之有道

import sys
import logging

sys.path.append("../../lib/repo/log")
from log import *

from predict import *

if __name__ == "__main__":

    date = int(sys.argv[1])
    days = int(sys.argv[2])

    log_init("../../../log/predicter.log" + str(date) + "-" + str(days))

    # 新建对象
    pred = Predicter()

    # 结果预测
    pred.predict(date, days)
