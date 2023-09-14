# -*- coding:utf-8 -*-
# 君子爱财 取之YOODO!

import sys
import time
import logging

sys.path.append("../../lib/repo/log")
from log import *

sys.path.append("../../lib/repo/dtime")
from dtime import *

from predict import *

def usage():
    ''' 展示帮助信息 '''
    print("python3 ./main.py [model] [date] [days]")
    print("     - model: 模型类型(r:线性模型; c:分类模型)")
    print("     - date: 预测时间")
    print("     - days: 预测周期")

def date_is_valid(date):
    ''' 判断DATE是否合法 '''
    timestamp = date_to_timestamp(date)
    if timestamp == 0:
        logging.error("Date is invalid! date:%s", date)
        return False

    curr_timestamp = time.time()
    if curr_timestamp < timestamp:
        logging.error("Date is out of lastest date! date:%s", date)
        return False

    return True

if __name__ == "__main__":
    # 校验参数个数
    if len(sys.argv) != 4:
        usage()
        exit(0)

    # 提取校验参数
    model = str(sys.argv[1])
    date = int(sys.argv[2])
    days = int(sys.argv[3])

    # 验证参数合法性
    is_valid = date_is_valid(date)
    if is_valid == False:
        print("Date is invalid! date:", date)
        exit(-1)

    log_init("../../../log/predicter.log")

    # 新建对象
    pred = Predicter()

    # 结果预测
    pred.predict(model, date, days)
