# -*- coding:utf-8 -*-
# 君子爱财 取之YOODO!

# 获取潜力股列表

import json
import logging
import sys
import time

sys.path.append("../../lib/utils/log")
from log import *

sys.path.append("../../lib/data")
from data import Data
from label import Label

sys.path.append("../../repo/database")
from database import Database

sys.path.append("../../lib/utils/dtime")
from dtime import *

class Potential:
    def __init__(self):
        # 数据处理
        self.data = Data()
        self.database = Database()

        self.label = Label()

    def get_potential_stock(self, date):
        """ 获取潜力股 """
        return self.data.get_potential_stock(date)

    def analyze(self, date):
        """ 通过分析技术指标找出潜力股 """

        # 获取股票列表
        stock_list = self.data.get_good_stock()

        for stock in stock_list:
            stock_key = stock["stock_key"]

            # 查询交易指数
            tech_index = self.database.get_technical_index_list(stock_key, date)
            if tech_index is None:
                logging.error("Get technical index failed! stock_key:%s date:%s", stock_key, date)
                return None

            idx = 0
            for item in tech_index:
                prev_tech_data = None
                curr_tech_data = json.loads(item["data"])
                if idx > 0:
                    prev_tech_data = json.loads(tech_index[idx]["data"])

                labels = dict()

                labels["stock_key"] = stock_key
                labels["date"] = item["date"]
                labels["none"] = 0
                labels["positive"] = 0
                labels["negative"] = 0

                if "KDJ" in curr_tech_data.keys():
                    kdj = self.label.kdj2label(curr_tech_data["KDJ"])
                    labels["kdj"] = kdj
                    if kdj == 0:
                        labels["none"] += 1
                    elif kdj > 0:
                        labels["positive"] += 1
                    elif kdj < 0:
                        labels["negative"] += 1

                if "RSI" in curr_tech_data.keys():
                    rsi = self.label.rsi2label(curr_tech_data["RSI"])
                    labels["rsi"] = rsi
                    if rsi == 0:
                        labels["none"] += 1
                    elif rsi > 0:
                        labels["positive"] += 1
                    elif rsi < 0:
                        labels["negative"] += 1

                if ("CCI" in curr_tech_data.keys()) and ("CCI" in prev_tech_data.keys()):
                    cci = self.label.cci2label(curr_tech_data["CCI"], prev_tech_data["CCI"])
                    labels["cci"] = cci
                    if cci == 0:
                        labels["none"] += 1
                    elif cci > 0:
                        labels["positive"] += 1
                    elif cci < 0:
                        labels["negative"] += 1

                print(labels)
                idx += 1

if __name__ == "__main__":

    date = int(sys.argv[1])

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

    log_init("../../../log/potential.log")

    # 新建对象
    pot = Potential()

    # 获取潜力股
    #pot.get_potential_stock(date)
    pot.analyze(date)
