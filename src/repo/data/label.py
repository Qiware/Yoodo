# -*- coding:utf-8 -*-
# 君子爱财 取之YOODO!

import sys
import time
import json
import joblib
import logging

SIGNAL_ADD_PLUS = 3  # 信号: 强烈加仓
SIGNAL_ADD = 2  # 信号: 加仓
SIGNAL_POSITIVE = 1  # 信号: 正向(强势)
SIGNAL_NONE = 0  # 信号: 持平
SIGNAL_NEGATIVE = -1  # 信号: 负向(弱势)
SIGNAL_SUB = -2  # 信号: 减仓
SIGNAL_SUB_PLUS = -3  # 信号: 强烈减仓

# LABEL转换
class Label():
    def __init__(self):
        pass

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
        if int(kdj["K"]) > 90: # 超买: 减仓
            return SIGNAL_SUB
        elif int(kdj["K"]) < 10: # 超卖: 加仓
            return SIGNAL_ADD
        return SIGNAL_NONE

    def rsi_label(self, rsi):
        ''' RSI特征LABEL '''
        if rsi > 90: # 严重超买: 强烈减仓
            return SIGNAL_SUB_PLUS
        elif rsi > 80: # 超买: 减仓
            return SIGNAL_SUB
        elif rsi > 50: # 多头涨势
            return SIGNAL_POSITIVE
        elif rsi < 10: # 严重超卖: 强烈加仓
            return SIGNAL_ADD_PLUS
        elif rsi < 20: # 超卖: 加仓
            return SIGNAL_ADD
        elif rsi < 50: # 空头跌势
            return SIGNAL_NEGATIVE
        # 处于50时买卖均衡
        return SIGNAL_NONE

    def cci_label(self, cci):
        ''' CCI特征LABEL '''
        if cci > 100: # 超买: 减仓
            return SIGNAL_SUB
        elif cci < -100: # 超卖: 加仓
            return SIGNAL_ADD
        # -100 ~ 100表示整盘区间
        return SIGNAL_NONE

    def ad_label(self, curr_ad, prev_ad, curr_close_price, prev_close_price):
        ''' AD特征LABEL '''
        # 底背离: 价格下跌, 但资金在增加(看涨: 买入信号)
        if ((curr_close_price - prev_close_price) < 0) and ((curr_ad - prev_ad) > 0):
            return SIGNAL_ADD
        # 顶背离: 价格上涨, 但资金在减少(看跌: 卖出信号)
        if ((curr_close_price - prev_close_price) > 0) and ((curr_ad - prev_ad) < 0):
            return SIGNAL_SUB
        # 价格和资金量同步
        return SIGNAL_NONE

    def adosc_label(self, curr_ad, prev_ad):
        ''' ADOSC特征LABEL
            1.osc指标实际上是专门以0值为中线，若osc在零线之上，于是市场处于强势状态；
              若是osc的位置在零线之下，于是市场处于弱势状态。
            2.假如osc穿过零线上升，那此时就是市场走强，可视为购买信号。
              反之亦然，假设osc跌破零线继续下行，那么市场变弱，能够视做卖出信号。
            3.若是osc离零线不近，换言之就是价格远离平均线，这时应注意价格很可能向平均线回归。
        '''
        if curr_ad > 0:
            if prev_ad <= 0:
                return SIGNAL_ADD
            return SIGNAL_POSITIVE
        if curr_ad < 0:
            if prev_ad >= 0:
                return SIGNAL_SUB
            return SIGNAL_NEGATIVE
        return SIGNAL_NONE
