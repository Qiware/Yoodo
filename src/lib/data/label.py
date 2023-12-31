# -*- coding:utf-8 -*-
# 君子爱财 取之YOODO!

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
        """ 波动比率
            @Param base_val: 基准值
            @Param val: 当前值
        """
        diff = (val - base_val)
        if diff == 0:
            return 0
        if base_val == 0:
            return 100
        return diff / base_val * 100

    def gen_classify(self, price_ratio):
        """ 生成分类
            @Param price_ratio: 涨价幅度
        """
        val = 1
        if price_ratio < 0:
            price_ratio -= val
        return int(price_ratio / val) * val

    def str2label(self, s):
        """ 将字符串转为数字LABEL """
        return xxhash.xxh32(s, seed=0).intdigest()

    def kdj2label(self, kdj):
        """ KDJ特征LABEL
            @基本概念:
              1.K线: 反映了最近一段时间内股票价格在波动中收盘价与最低价之间的相对位置;
              2.D线: 则是K线的平滑线;
              3.J线: 则是K线和D线的差值;
              K线和D线的活动范围在0-100, J线的活动范围没有具体的限制.
            @应用规则: https://zhuanlan.zhihu.com/p/419208966
        """

        k = int(kdj["K"])
        d = int(kdj["D"])
        j = int(kdj["J"])

        # 1.J大于100时，行情呈现超买现象。J小于0时，行情呈现超卖现象。
        if j > 100:  # 超买: 减仓
            return SIGNAL_SUB_PLUS
        elif j < 0:  # 超卖: 加仓
            return SIGNAL_ADD_PLUS

        # 2.K与D值永远介于0到100之间。K大于90时，行情呈现超买现象。K小于10时，
        #   行情呈现超卖现象。
        if k > 90:  # 超买: 减仓
            return SIGNAL_SUB_PLUS
        elif k < 10:  # 超卖: 加仓
            return SIGNAL_ADD_PLUS

        # 3.K与D值永远介于0到100之间。D大于80时，行情呈现超买现象。D小于20时，
        #   行情呈现超卖现象。
        if d > 80:  # 超买: 减仓
            return SIGNAL_SUB_PLUS
        elif d < 20:  # 超卖: 加仓
            return SIGNAL_ADD_PLUS

        # 4.上涨趋势中，K值大于D值，K线向上突破D线时，为买进信号。下跌趋势中，
        #   K值小于D值，K线向下跌破D线时，为卖出信号。
        if k > d:
            return SIGNAL_POSITIVE
        elif k < d:
            return SIGNAL_NEGATIVE

        return SIGNAL_NONE

    def rsi2label(self, rsi):
        """ RSI特征LABEL """
        if rsi > 90:  # 严重超买: 强烈减仓
            return SIGNAL_SUB_PLUS
        elif rsi > 80:  # 超买: 减仓
            return SIGNAL_SUB
        elif rsi > 50:  # 多头涨势
            return SIGNAL_POSITIVE
        elif rsi < 10:  # 严重超卖: 强烈加仓
            return SIGNAL_ADD_PLUS
        elif rsi < 20:  # 超卖: 加仓
            return SIGNAL_ADD
        elif rsi < 50:  # 空头跌势
            return SIGNAL_NEGATIVE
        # 处于50时买卖均衡
        return SIGNAL_NONE

    def cci2label(self, curr_cci, prev_cci):
        """ CCI特征LABEL
            @Desc CCI指标非常敏感, 适合追踪暴涨暴跌行情
            @Param curr_cci: 当前周期CCI指标
            @Param prev_cci: 前一周期CCI指标
        """
        # 情况1: 指标从下往上快速突破100, 是买入时间
        if curr_cci > 100:  # 超买区域
            if prev_cci < 100:
                return SIGNAL_ADD_PLUS
            if curr_cci - prev_cci > 0:
                return SIGNAL_ADD
            return SIGNAL_SUB
        if curr_cci < -100:  # 超卖区域
            if prev_cci > -100:
                return SIGNAL_SUB_PLUS
            if curr_cci - prev_cci < 0:
                return SIGNAL_SUB
            return SIGNAL_ADD
        # -100 ~ 100表示整盘区间
        return SIGNAL_NONE

    def ad2label(self, curr, prev):
        """ AD特征LABEL
            @应用规则: 当AD指标上升时，意味着资金在流入，股票价格有望上涨，此时是买入良机；
            当AD指标下降时，意味着资金在流出，股票价格可能下跌，此时是卖出时机。
        """
        return self.ratio(prev["AD"], curr["AD"])

    def adosc2label(self, curr_ad, prev_ad):
        """ ADOSC特征LABEL
            1.osc指标实际上是专门以0值为中线，若osc在零线之上，于是市场处于强势状态；
              若是osc的位置在零线之下，于是市场处于弱势状态。
            2.假如osc穿过零线上升，那此时就是市场走强，可视为购买信号。
              反之亦然，假设osc跌破零线继续下行，那么市场变弱，能够视做卖出信号。
            3.若是osc离零线不近，换言之就是价格远离平均线，这时应注意价格很可能向平均线回归。
        """
        if curr_ad > 0:
            if prev_ad <= 0:
                return SIGNAL_ADD
            return SIGNAL_POSITIVE
        if curr_ad < 0:
            if prev_ad >= 0:
                return SIGNAL_SUB
            return SIGNAL_NEGATIVE
        return SIGNAL_NONE

    def macd2label(self, curr, prev):
        """ MACD特征转为LABEL
            1.DIF线(快线)由下往上穿越DEA线(慢线)，这种形态叫MACD金叉。金叉会出现
            在零轴之上，也会出现在零轴之下。在零轴下出现，表示股价止跌回涨，可短
            线买入；而金叉在零轴上出现时，则表明股价即将开始有较大幅度的反弹，适
            合长线投资。
            2.类似的，当DIF线由上往下穿越DEA线，形成MACD死叉。如果死叉在零轴之上
            出现，意味着股价短期内下跌调整开始，投资者应减仓；若死叉出现在零轴下，
            则表示股价已经见顶，后市很可能开始大幅下行，投资者应立即清仓。
            3.暂不考虑顶背离和底背离的形态
        """
        # 判断是否MACD金叉
        if curr["DIFF"] > curr["DEA"]:
            if prev["DIFF"] < prev["DEA"]:
                # 出现MACD金叉形态
                if curr["DEA"] > 0:
                    # 金叉在零轴上出现时，则表明股价即将开始有较大幅度的反弹，适合长线投资。
                    return SIGNAL_ADD_PLUS
                # 在零轴下出现，表示股价止跌回涨，可短线买入
                return SIGNAL_ADD
            # 维持MACD金叉形态后的走势
            return SIGNAL_POSITIVE  # 继续看涨
        # 判断是否死叉
        if curr["DIFF"] < curr["DEA"]:
            if prev["DIFF"] > prev["DEA"]:
                # 出现MACD死叉形态
                if curr["DEA"] < 0:
                    # 死叉出现在零轴下，则表示股价已经见顶，后市很可能开始大幅下行，投资者应立即清仓。
                    return SIGNAL_SUB_PLUS  # 意味着股价短期内下跌调整开始，投资者应减仓
                # 死叉在零轴之上出现，意味着股价短期内下跌调整开始，投资者应减仓
                return SIGNAL_SUB  # 意味着股价短期内下跌调整开始，投资者应减仓
            # 维持MACD死叉形态后的走势
            return SIGNAL_NEGATIVE  # 继续看跌
        return SIGNAL_NONE

    def sar2label(self, curr, prev):
        """ SAR指标LABEL
            @Param sar: SAR指标
            @Param close_price: 收盘价
        """
        # 股价上涨时
        if curr["close_price"] > prev["close_price"]:
            if curr["close_price"] > curr["SAR"]:
                # 1、当股票股价从SAR曲线下方开始向上突破SAR曲线时，为买入信号，预示着股
                #    价一轮上升行情可能展开，投资者应迅速及时地买进股票。
                if prev["SAR"] >= prev["close_price"]:
                    return SIGNAL_ADD_PLUS
                # 2、当股票股价向上突破SAR曲线后继续向上运动而SAR曲线也同时向上运动时，
                #    表明股价的上涨趋势已经形成，SAR曲线对股价构成强劲的支撑，投资者应坚
                #    决持股待涨或逢低加码买进股票。
                if curr["SAR"] > prev["SAR"]:
                    return SIGNAL_POSITIVE
                return SIGNAL_NONE

        if curr["close_price"] < prev["close_price"]:
            if curr["SAR"] > curr["close_price"]:
                # 3、当股票股价从SAR曲线上方开始向下突破SAR曲线时，为卖出信号，预示
                #    着股价一轮下跌行情可能展开，投资者应迅速及时地卖出股票。
                if prev["SAR"] <= prev["close_price"]:
                    return SIGNAL_SUB_PLUS
                # 4、当股票股价向下突破SAR曲线后继续向下运动而SAR曲线也同时向下运动，
                #    表明股价的下跌趋势已经形成，SAR曲线对股价构成巨大的压力，投资者
                #    应坚决持币观望或逢高减磅。
                if prev["SAR"] > curr["SAR"]:
                    return SIGNAL_NEGATIVE
                return SIGNAL_NONE
        return SIGNAL_NONE

    def obv2label(self, curr, prev):
        """ 将OBV指标转为LABEL
            1、当股价上升而OBV线下降，表示买盘无力，股价可能会回跌。
            2、股价下降时而OBV线上升，表示买盘旺盛，逢低接手强股，股价可能会止跌回升。
            3、OBV线缓慢上升，表示买气逐渐加强，为买进信号。
            4、OBV线急速上升时，表示力量将用尽为卖出信号。
            5、OBV线对双重顶第二个高峰的确定有较为标准的显示，当股价自双重顶第一个高峰下跌又再次回升时，如果OBV线能够随股价趋势同步上升且
               价量配合，则可持续多头市场并出现更高峰。相反，当股价再次回升时OBV线未能同步配合，却见下降，则可能形成第二个顶峰，完成双重顶
               的形态，导致股价反转下跌。
            6、OBV线从正的累积数转为负数时，为下跌趋势，应该卖出持有股票。反之，OBV线从负的累积数转为正数时，应该买进股票。
            7、OBV线最大的用处，在于观察股市盘局整理后，何时会脱离盘局以及突破后的未来走势，OBV线变动方向是重要参考指数，其具体的数值并无
               实际意义。
            参考资料: https://baijiahao.baidu.com/s?id=1729552677456218979&wfr=spider&for=pc
        """
        diff = curr["OBV"] - prev["OBV"]

        # 1、当股价上升而OBV线下降，表示买盘无力，股价可能会回跌。
        if curr["close_price"] - prev["close_price"] > 0:
            if diff < 0:
                return SIGNAL_NEGATIVE

        # 2、股价下降时而OBV线上升，表示买盘旺盛，逢低接手强股，股价可能会止跌回升。
        if curr["close_price"] - prev["close_price"] < 0:
            if diff > 0:
                return SIGNAL_POSITIVE

        diff = curr["OBV"] - prev["OBV"]
        if diff > 0:
            ratio = diff / abs(prev["OBV"])
            if ratio >= 0.3:
                # 4、OBV线急速上升时，表示力量将用尽为卖出信号。
                return SIGNAL_SUB
            # 3、OBV线缓慢上升，表示买气逐渐加强，为买进信号。
            return SIGNAL_ADD

        # 5、OBV线对双重顶第二个高峰的确定有较为标准的显示，当股价自双重顶第一个高峰下跌又再次回升时，如果OBV线能够随股价趋势同步上升且
        #    价量配合，则可持续多头市场并出现更高峰。 (底背离)
        #    相反，当股价再次回升时OBV线未能同步配合，却见下降，则可能形成第二个顶峰，完成双重顶
        #    的形态，导致股价反转下跌。(顶背离)

        # 6、OBV线从正的累积数转为负数时，为下跌趋势，应该卖出持有股票。反之，OBV线从负的累积数转为正数时，应该买进股票。
        if (prev["OBV"] > 0) and (curr["OBV"] < 0):
            return SIGNAL_SUB
        elif (prev["OBV"] < 0) and (curr["OBV"] > 0):
            return SIGNAL_ADD

        # 7、OBV线最大的用处，在于观察股市盘局整理后，何时会脱离盘局以及突破后的未来走势，OBV线变动方向是重要参考指数，其具体的数值并无
        #    实际意义。
        return SIGNAL_NONE


if __name__ == "__main__":
    label = Label()

    name = "qifeng"
    print(label.str2label(name))
