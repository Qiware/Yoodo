# -*- coding:utf-8 -*-
# 君子爱财 取之有道

import sys
import time
import logging

sys.path.append("../../lib/repo/log")
from log import *

sys.path.append("../../lib/repo/dtime")
from dtime import *

from predict import *

if __name__ == "__main__":

    date = int(sys.argv[1])
    days = int(sys.argv[2])

    # 验证参数合法性
    try:
        timestamp = date_to_timestamp(date)
        curr_timestamp = time.time()
        if curr_timestamp < timestamp:
            print("Date is out of range!")
            exit(-1)
    except Exception as e:
        print("Date is out of range! err:%s", e)
        exit(-1)

    log_init("../../../log/predicter.log")

    # 新建对象
    pred = Predicter()

    # 结果预测
    pred.predict(date, days)
