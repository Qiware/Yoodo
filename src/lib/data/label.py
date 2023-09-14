# -*- coding:utf-8 -*-
# 君子爱财 取之YOODO!

import sys
import time
import json
import joblib
import logging

sys.path.append("../../lib/model")
from model import *
sys.path.append("../../lib/repo/log")
from log import *
sys.path.append("../../lib/repo/database")
from database import *

# LABEL转换
class Label():
    def __init__(self):
        # 连接数据库
        self.database = Database()

        # 查询指数数据
        self.hsi_index_list = self.get_hsi_index() # 恒生指数
        self.hz2083_index_list = self.get_hz2083_index() # 恒生科技指数

    def ratio(self, base_val, val):
        ''' 波动比率
            @Param base_val: 基准值
            @Param val: 当前值
        '''
        diff = (val - base_val)
        if diff == 0:
            return 0
        if (base_val == 0):
            return 100
        return  diff / base_val * 100

    def gen_classify(self, price_ratio):
        ''' 生成分类
            @Param price_ratio: 涨价幅度
        '''
        if price_ratio < 0:
            price_ratio -= 5
        return int(price_ratio/5) * 5

    def kdj_label(self, kdj):
        ''' KDJ特征LABEL '''
        if int(kdj["K"]) > 90: # 超买
            return 1
        elif int(kdj["K"]) < 10: # 超卖
            return -1
        return 0

    def rsi_label(self, rsi):
        ''' RSI特征LABEL '''
        if rsi > 90: # 严重超买
            return 3
        elif rsi > 80: # 超买
            return 2
        elif rsi > 50: # 多头涨势
            return 1
        elif rsi < 10: # 验证超卖
            return -3
        elif rsi < 20: # 超卖
            return -2
        elif rsi < 50: # 空头跌势
            return -1
        # 处于50时买卖均衡
        return 0

    def cci_label(self, cci):
        ''' CCI特征LABEL '''
        if cci > 100: # 超买
            return 1
        elif cci < -100: # 超卖
            return -1
        # -100 ~ 100表示整盘区间
        return 0
