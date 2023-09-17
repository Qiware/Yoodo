# -*- coding:utf-8 -*-
# 君子爱财 取之YOODO!

import sys
import time
import json
import joblib
import logging
import xxhash

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
        val = 2
        if price_ratio < 0:
            price_ratio -= val
        return int(price_ratio/val) * val

    def str_label(self, s):
        ''' 将字符串转为数字LABEL '''
        return xxhash.xxh32(s, seed=0).intdigest()

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

    def cci_label(self, curr_cci, prev_cci):
        ''' CCI特征LABEL
            CCI指标非常敏感, 适合追踪暴涨暴跌行情
        '''
        # 情况1: 指标从下往上快速突破100, 是买入时间
        if curr_cci > 100: # 超买区域
            if prev_cci < 100:
                return SIGNAL_ADD_PLUS
            if curr_cci - prev_cci > 0:
                return SIGNAL_ADD
            return SIGNAL_SUB
        if curr_cci < -100: # 超卖区域
            if prev_cci > -100:
                return SIGNAL_SUB_PLUS
            if curr_cci - prev_cci < 0:
                return SIGNAL_SUB
            return SIGNAL_ADD
        # -100 ~ 100表示整盘区间
        return SIGNAL_NONE

    def ad_label(self, curr, prev):
        ''' AD特征LABEL '''
        # 底背离: 价格下跌, 但资金在增加(看涨: 买入信号)
        if ((curr["close_price"] - prev["close_price"]) < 0) and \
                ((curr["AD"] - prev["AD"]) > 0):
            return SIGNAL_ADD
        # 顶背离: 价格上涨, 但资金在减少(看跌: 卖出信号)
        if ((curr["close_price"] - prev["close_price"]) > 0) and ((curr["AD"] - prev["AD"]) < 0):
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

    def macd_label(self, curr, prev):
        ''' MACD特征转为LABEL
            1.DIF线(快线)由下往上穿越DEA线(慢线)，这种形态叫MACD金叉。金叉会出现
            在零轴之上，也会出现在零轴之下。在零轴下出现，表示股价止跌回涨，可短
            线买入；而金叉在零轴上出现时，则表明股价即将开始有较大幅度的反弹，适
            合长线投资。
            2.类似的，当DIF线由上往下穿越DEA线，形成MACD死叉。如果死叉在零轴之上
            出现，意味着股价短期内下跌调整开始，投资者应减仓；若死叉出现在零轴下，
            则表示股价已经见顶，后市很可能开始大幅下行，投资者应立即清仓。
        '''
        # 判断是否MACD金叉
        if (curr["DIFF"] > curr["DEA"]) and (prev["DIFF"] < prev["DEA"]):
            if curr["MACD"] > 0:
                return 2 # 将有大幅度的反弹, 适合长线投资
            return 1 # 股票止跌回涨, 可断线买入
        # 判断是否死叉
        if (curr["DIFF"] < curr["DEA"]) and (prev["DIFF"] > prev["DEA"]):
            if curr["MACD"] > 0:
                return -1 # 意味着股价短期内下跌调整开始，投资者应减仓
            return -2 # 表示股价已经见顶，后市很可能开始大幅下行，投资者应立即清仓
        return 0

    def sar_label(self, curr, prev):
        ''' SAR指标LABEL
            @Param sar: SAR指标
            @Param close_price: 收盘价
        '''
        if curr["SAR"] < curr["close_price"]:
            # 1、当股票股价从SAR曲线下方开始向上突破SAR曲线时，为买入信号，预示着股
            #    价一轮上升行情可能展开，投资者应迅速及时地买进股票。
            if prev["SAR"] >= prev["close_price"]:
                return SIGNAL_ADD_PLUS
            # 2、当股票股价向上突破SAR曲线后继续向上运动而SAR曲线也同时向上运动时，
            #    表明股价的上涨趋势已经形成，SAR曲线对股价构成强劲的支撑，投资者应坚
            #    决持股待涨或逢低加码买进股票。
            if prev["SAR"] < prev["close_price"]:
                return SIGNAL_POSITIVE
            return SIGNAL_NONE

        if curr["SAR"] > curr["close_price"]:
            # 3、当股票股价从SAR曲线上方开始向下突破SAR曲线时，为卖出信号，预示
            #    着股价一轮下跌行情可能展开，投资者应迅速及时地卖出股票。
            if prev["SAR"] <= prev["close_price"]:
                return SIGNAL_SUB_PLUS
            # 4、当股票股价向下突破SAR曲线后继续向下运动而SAR曲线也同时向下运动，
            #    表明股价的下跌趋势已经形成，SAR曲线对股价构成巨大的压力，投资者
            #    应坚决持币观望或逢高减磅。
            if prev["SAR"] > prev["close_price"]:
                return SIGNAL_NEGATIVE
            return SIGNAL_NONE
        return SIGNAL_NONE


if __name__ == "__main__":
    label = Label()

    name = "qifeng"
    print(label.str_label(name))
