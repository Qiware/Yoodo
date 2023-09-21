# -*- coding:utf-8 -*-
# 君子爱财 取之YOODO!

# 计算股票交易指数

import sys

sys.path.append("./logic")
from analyzer import *

sys.path.append("../../repo/lib/log")
from log import *


def usage():
    """ 展示帮助信息 """
    print("python3 ./main.py")
    print("     - help: 展示帮助信息")


if __name__ == "__main__":
    # 校验参数
    if len(sys.argv) < 2:
        usage()
        exit(-1)

    start_code = int(sys.argv[1])  # 起始代码
    worker_num = int(sys.argv[2])  # 并发数量

    # 日志初始化
    log_init("../../../log/analyzer.log")

    # 新建数据分析
    analyzer = Analyzer(start_code, worker_num)

    analyzer.wait()
