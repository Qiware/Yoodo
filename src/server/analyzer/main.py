# -*- coding:utf-8 -*-
# 君子爱财 取之YOODO!

# 计算股票交易指数

import sys

sys.path.append("./logic")
from analyzer import Analyzer

sys.path.append("../../lib/utils/log")
from log import *


def usage():
    """ 展示帮助信息 """
    print("python3 ./main.py")
    print("     - help: 展示帮助信息")


if __name__ == "__main__":
    # 日志初始化
    log_init("../../../log/analyzer.log")

    # 新建数据分析
    analyzer = Analyzer()

    analyzer.load_index()
    analyzer.load_stock()
