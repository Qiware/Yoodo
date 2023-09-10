# -*- coding:utf-8 -*-

# 计算股票交易指数

import sys
import logging

sys.path.append("./logic")
from index import *

sys.path.append("../../lib/repo/log")
from log import *

def usage():
    ''' 展示帮助信息 '''
    print("python3 ./main.py")
    print("     - help: 展示帮助信息")

if __name__ == "__main__":
    # 校验参数
    if len(sys.argv) < 2:
        usage() 
        exit(-1)

    start_stock_code = int(sys.argv[1])

    # 日志初始化
    log_init("../../../log/index.log")

    # 新建指数处理器
    index = Index()

    index.rebuild(start_stock_code)

