# -*- coding:utf-8 -*-
# 君子爱财 取之YOODO!

import sys
import time
import json
import joblib
import logging

sys.path.append("../lib/model")
from model import *
sys.path.append("../lib/log")
from log import *
sys.path.append("../database")
from database import *

from label import Label

# 恒生指数KEY
STOCK_KEY_HSI = "hkex:hsi" # 恒生指数
STOCK_KEY_HZ2083 = "hkex:hz2083" # 恒生科技指数

# 单条训练样本拥有的交易数据条目
TRAIN_DATA_TRANSACTION_NUM = 20

# 查询所有交易数据
TRANSACTION_NUM_ALL = 999999

# 股票市值50亿
STOCK_MARKET_CAP_5B = 5000000000
# 股票市值200亿
STOCK_MARKET_CAP_20B = 20000000000

# 数据处理
class Data():
    def __init__(self):
        self.label = Label()

        # 连接数据库
        self.database = Database()

        # 查询指数数据
        self.hsi_index_list = self.get_hsi_index() # 恒生指数
        self.hz2083_index_list = self.get_hz2083_index() # 恒生科技指数

    def gen_train_data_fpath(self, date, days):
        ''' 生成训练数据的路径 '''
        return "../../../data/train/%s-%ddays.dat" % (str(date), int(days))

    def fill_transaction_data(self, stock_key, date, transaction_list):
        ''' 填充交易数据 '''

        # 查询交易指数
        index_dict = self.database.get_transaction_index_list(stock_key, date)
        if index_dict is None:
            logging.error("Get transaction index failed! stock_key:%s date:%s", stock_key, date)
            return None

        # 填充交易数据
        for item in transaction_list:
            curr_date = item["date"]

            # 填充恒生指数
            item["hsi_open_price"] = self.hsi_index_list[curr_date]["open_price"] # 开盘价(取第一天开盘价)
            item["hsi_close_price"] = self.hsi_index_list[curr_date]["close_price"] # 收盘价(取最后一天收盘价)
            item["hsi_top_price"] = self.hsi_index_list[curr_date]["top_price"] # 最高价
            item["hsi_bottom_price"] = self.hsi_index_list[curr_date]["bottom_price"] # 最低价
            item["hsi_turnover"] = self.hsi_index_list[curr_date]["turnover"] # 交易额

            # 填充交易指数
            item["index"] = json.loads(index_dict[curr_date]["data"])

        return transaction_list

    def gen_train_data_by_days(self, model_type, date, days, num):
        ''' 按days天聚合训练数据
            @Param date: 结束日期
            @Param days: 以days为间隔进行分组
            @Param num: 交易数据条目
        '''

        logging.debug("Call gen_train_data_by_days(). date:%s days:%s num:%s", date, days, num)

        fp = open(self.gen_train_data_fpath(date, days), "w")

        # 获取股票列表
        stock_list = self.database.get_good_stock()
        for stock in stock_list:
            stock_key = stock["stock_key"]

            logging.debug("Generate train data. stock_key:%s date:%s num:%s", stock_key, date, num)

            # 拉取交易数据
            transaction_list = self.database.get_transaction_list(stock_key, date, num)

            # 填充交易数据
            transaction_list = self.fill_transaction_data(stock_key, date, transaction_list)

            # 生成训练样本
            if model_type == MODEL_REGRESSOR:
                self.gen_train_data_by_transaction_list(stock_key, transaction_list, fp)
            elif model_type == MODEL_CLASSIFIER:
                self.gen_classify_train_data_by_transaction_list(stock_key, transaction_list, fp)

        fp.close()

        return None

    def gen_train_data_by_transaction_list(self, stock_key, transaction_list, fp):
        ''' 通过交易列表生成训练数据
            @Param stock_key: 股票KEY
            @Param transaction_list: 交易数据
            @Param fp: 文件指针
            @注意: 收盘价/最高价/最低价的涨跌比例不与当天开盘价比较, 而是与前一天的收盘价比较.
        '''

        logging.debug("Call gen_train_data_by_transaction_list(). stock_key:%s", stock_key)

        train_data_list = list()

        # 判断参数合法性
        if len(transaction_list) < TRAIN_DATA_TRANSACTION_NUM+1:
            logging.error("Transaction data not enough! stock_key:%s", stock_key)
            return None

        # 注意: 收盘价/最高价/最低价不与当前的开盘价比较, 而是与前一天的收盘价比较. 
        #       预留最后一个作为起始基准.
        offset = len(transaction_list) - (TRAIN_DATA_TRANSACTION_NUM + 1)

        while (offset > 0):
            train_data = ""

            # 生成训练数据
            features = self.gen_feature_by_transaction_list(
                    stock_key,
                    transaction_list[offset:offset+TRAIN_DATA_TRANSACTION_NUM+1])
            if features is None:
                offset -= 1
                continue

            for feature in features:
                train_data += "%f," % (feature)

            # 设置预测结果(往前一天的收盘价 与 往后一天的收盘价做对比)
            price_ratio = self.label.ratio(
                    transaction_list[offset]["close_price"],
                    transaction_list[offset-1]["close_price"])

            train_data += "%f\n" % (price_ratio)

            train_data_list.append(train_data)
            offset -= 1

        # 将结果输出至文件
        fp.writelines(train_data_list)

        return None

    def gen_classify_train_data_by_transaction_list(self, stock_key, transaction_list, fp):
        ''' 通过交易列表生成训练数据(分类)
            @Param stock_key: 股票KEY
            @Param transaction_list: 交易数据
            @Param fp: 文件指针
            @注意: 收盘价/最高价/最低价的涨跌比例不与当天开盘价比较, 而是与前一天的收盘价比较.
        '''

        logging.debug("Call gen_class_train_data_by_transaction_list(). stock_key:%s",
                      stock_key)

        train_data_list = list()

        # 判断参数合法性
        if len(transaction_list) < TRAIN_DATA_TRANSACTION_NUM+1:
            logging.error("Transaction data not enough! stock_key:%s", stock_key)
            return None

        # 注意: 收盘价/最高价/最低价不与当前的开盘价比较, 而是与前一天的收盘价比较. 
        #       预留最后一个作为起始基准.
        offset = len(transaction_list) - (TRAIN_DATA_TRANSACTION_NUM + 1)

        while (offset > 0):
            train_data = ""

            # 生成训练数据
            features = self.gen_feature_by_transaction_list(
                    stock_key,
                    transaction_list[offset:offset+TRAIN_DATA_TRANSACTION_NUM+1])
            if features is None:
                logging.error("Generate feature by transactionn list failed! stock_key:%s offset:%s",
                              stock_key, offset)
                offset -= 1
                continue
            for feature in features:
                train_data += "%f," % (feature)

            # 设置预测结果(往前一天的收盘价 与 往后一天的收盘价做对比)
            price_ratio = self.label.ratio(
                    transaction_list[offset]["close_price"],
                    transaction_list[offset-1]["close_price"])
            classify = self.label.gen_classify(price_ratio)

            train_data += "%d\n" % (classify)

            train_data_list.append(train_data)
            offset -= 1

        # 将结果输出至文件
        fp.writelines(train_data_list)

        return None

    def load_train_data(self, date, days):
        ''' 加载训练数据, 并返回特征数据和目标数据 '''
        # 加载训练数据
        fp = open(self.gen_train_data_fpath(date, days))
        lines = fp.readlines()
        fp.close()

        # 处理训练数据
        target_list = list()
        feature_list = list()

        index = 0
        for line in lines:
            index += 1
            logging.debug("Load train data. line:%d", index)
            line = line.strip()
            data = line.split(",")

            feature = list()
            target = float(data[-1])

            idx = 0
            while (idx < len(data)-1):
                feature.append(float(data[idx]))
                idx += 1
            feature_list.append(feature)
            target_list.append(target)

        return feature_list, target_list

    def gen_feature_by_transaction_list(self, stock_key, transaction_list):
        ''' 通过交易列表生成特征数据. 返回格式: [x1, x2, x3, ...]
            @Param transaction_list: 交易数据
            @Param fname: 训练数据输出文件
            @Return: 特征数据列表
        '''
        # 判断参数合法性
        if len(transaction_list) < (TRAIN_DATA_TRANSACTION_NUM+1):
            logging.error("Transaction data not enough! stock_key:%s", stock_key)
            return None

        feature = list()
        label = Label()

        # 生成特征数据(注意: 最后一个作为起始基准)
        index = TRAIN_DATA_TRANSACTION_NUM - 1
        while (index >= 0):
            try:
                curr = transaction_list[index]
                prev = transaction_list[index+1]

                curr_index = curr["index"] # 当前周期交易指数
                prev_index = prev["index"] # 前一周期交易指数

                # 与前周期的比较
                feature.append(self.label.ratio(prev["close_price"], curr["open_price"]))
                feature.append(self.label.ratio(prev["close_price"], curr["close_price"]))
                feature.append(self.label.ratio(prev["close_price"], curr["top_price"]))
                feature.append(self.label.ratio(prev["close_price"], curr["bottom_price"]))

                feature.append(self.label.ratio(prev["volume"], curr["volume"]))
                feature.append(self.label.ratio(prev["turnover"], curr["turnover"]))
                feature.append(self.label.ratio(prev["turnover_ratio"], curr["turnover_ratio"]))

                feature.append(self.label.ratio(prev["hsi_close_price"], curr["hsi_open_price"]))
                feature.append(self.label.ratio(prev["hsi_close_price"], curr["hsi_close_price"]))
                feature.append(self.label.ratio(prev["hsi_close_price"], curr["hsi_top_price"]))
                feature.append(self.label.ratio(prev["hsi_close_price"], curr["hsi_bottom_price"]))

                # 与本周期的开盘价比较
                feature.append(self.label.ratio(curr["open_price"], curr["close_price"]))
                feature.append(self.label.ratio(curr["open_price"], curr["top_price"]))
                feature.append(self.label.ratio(curr["open_price"], curr["bottom_price"]))

                feature.append(self.label.ratio(curr["hsi_open_price"], curr["hsi_close_price"]))
                feature.append(self.label.ratio(curr["hsi_open_price"], curr["hsi_top_price"]))
                feature.append(self.label.ratio(curr["hsi_open_price"], curr["hsi_bottom_price"]))

                # 与本周期的收盘价比较
                feature.append(self.label.ratio(curr["close_price"], curr["top_price"]))
                feature.append(self.label.ratio(curr["close_price"], curr["bottom_price"]))

                feature.append(self.label.ratio(curr["hsi_close_price"], curr["hsi_top_price"]))
                feature.append(self.label.ratio(curr["hsi_close_price"], curr["hsi_bottom_price"]))

                # 换手率
                feature.append(curr["turnover_ratio"])

                # 与本周期的交易指数比较
                feature.append(self.label.ratio(curr["close_price"], curr_index["MA5PRC"]))
                feature.append(self.label.ratio(curr["close_price"], curr_index["MA10PRC"]))
                feature.append(self.label.ratio(curr["close_price"], curr_index["MA20PRC"]))

                feature.append(self.label.ratio(curr["close_price"], curr_index["BOLL"]["UPPER"]))
                feature.append(self.label.ratio(curr["close_price"], curr_index["BOLL"]["MIDDLE"]))
                feature.append(self.label.ratio(curr["close_price"], curr_index["BOLL"]["LOWER"]))

                feature.append(self.label.ratio(curr["volume"], curr_index["MA5VOL"]))
                feature.append(self.label.ratio(curr["volume"], curr_index["MA10VOL"]))
                feature.append(self.label.ratio(curr["volume"], curr_index["MA20VOL"]))

                # 交易指数
                feature.append(self.label.kdj_label(curr_index["KDJ"]))
                feature.append(self.label.rsi_label(int(curr_index["RSI"])))

                feature.append(self.label.cci_label(int(curr_index["CCI"])))
                feature.append(self.label.ratio(curr_index["CCI"], prev_index["CCI"]))

                feature.append(curr_index["MACD"]["MACD"])
                feature.append(curr_index["MACD"]["DIFF"])
                feature.append(curr_index["MACD"]["DEA"])
                feature.append(self.label.ratio(curr_index["MACD"]["DIFF"], curr_index["MACD"]["DEA"]))

                feature.append(self.label.ad_label(curr_index["AD"], prev_index["AD"], curr["close_price"], prev["close_price"])
                feature.append(self.label.ratio(curr_index["AD"], prev_index["AD"]))

                index -= 1
            except Exception as e:
                logging.error("Generate feature by transaction list failed! stock_key:%s error:%s",
                              stock_key, e)
                return None

        logging.info("Generate feature by transaction list success. transaction_list:%d feature:%d",
                     len(transaction_list), len(feature))

        return feature

    def get_transaction_list(self, stock_key, date, num):
        ''' 获取交易列表 '''
        return self.database.get_transaction_list(stock_key, date, num)

    def load_feature(self, stock_key, date, days):
        ''' 加载特征数据 '''
        feature = list()

        # 查询所需数据
        transaction_list = self.database.get_transaction_list(
                stock_key, date, TRAIN_DATA_TRANSACTION_NUM*(days+1))
        if (transaction_list is None) or (len(transaction_list) == 0):
            logging.error("Get transaction list failed! stock_key:%s date:%s days:%s",
                          stock_key, date, days)
            return date, None

        lastest = transaction_list[0]

        # 填充交易数据
        transaction_list = self.fill_transaction_data(stock_key, date, transaction_list)

        # 生成训练样本
        item = self.gen_feature_by_transaction_list(stock_key, transaction_list)
        if item is None:
            logging.error("Generate feature by transaction list failed! stock_key:%s transaction_list:%d",
                          stock_key, len(transaction_list))
            return date, None

        feature.append(item)

        return lastest["date"], feature

    def get_all_stock(self):
        ''' 获取所有股票  '''
        return self.database.get_all_stock()

    def get_good_stock(self):
        ''' 获取优质股票  '''
        return self.database.get_good_stock()

    def update_predict(self, stock_key, date, days, base_date, ratio):
        ''' 更新预测数据
            @Param date: 评估时间
            @Param days: 评估周期
            @Param base_date: 基于哪天的数据
            @Param ratio: 预估涨幅
        '''

        # 获取基准价格
        transaction_list = self.database.get_transaction_list(stock_key, date, 1)
        if len(transaction_list) == 0:
            logging.error("Get transaction list failed! stock_key:%s date:%s",
                          stock_key, date)
            return
        if base_date != transaction_list[0]["date"]:
            logging.error("Data was updated! stock_key:%s date:%s",
                          stock_key, date)
            return

        base_price = transaction_list[0]["close_price"]

        # 准备数据
        data = dict()
        data["stock_key"] = stock_key
        data["date"] = date
        data["days"] = int(days)
        data["base_date"] = base_date
        data["base_price"] = base_price
        data["pred_price"] = base_price + base_price*(ratio/100)
        data["pred_ratio"] = ratio

        curr_timestamp = int(time.time())
        data["create_time"] = time.localtime(curr_timestamp)
        data["update_time"] = time.localtime(curr_timestamp)

        # 更新预测结果
        self.database.set_predict(data)

    def update_predict_real(self, stock_key, base_date, days, real_price, real_ratio):
        ''' 更新预测数据中的真实数据 '''

        # 准备数据
        data = dict()
        data["stock_key"] = stock_key
        data["base_date"] = base_date
        data["days"] = int(days)
        data["real_price"] = real_price
        data["real_ratio"] = real_ratio

        curr_timestamp = int(time.time())
        data["update_time"] = time.localtime(curr_timestamp)

        # 更新预测结果
        self.database.update_predict_real(data)

    def get_potential_stock(self, date):
        ''' 获取潜力股(隔夜持股法)
            @Desc: 1.涨幅在3%~5%之间
                   2.量比排名: 剔除量比小于1的股票
                   3.换手率排名: 剔除换手率>10%和<5%的股票
                   4.按市值排名: 剔除市值大于200亿和小于50亿的股票
                   5.成交量忽高忽低的删掉, 只保留成交量持续放大的股票
                   6.K线形态: 高位票或没有支撑的删掉
                   7.看分时图: 全天运行的分时均价上方必须强于大盘
                   8.两点半左右创出当日的新高, 回踩均线不跌破, 就是买点.
        '''
        # 拉取所有股票当天交易数据
        potential_stock_list = list() # 潜力股列表

        stock_list = self.database.get_all_stock()
        for stock in stock_list:
            stock_key = stock["stock_key"]

            # 拉取当天交易数据
            transaction_list = self.database.get_transaction_list(stock_key, date, 3)
            if len(transaction_list) < 5:
                logging.info("Get transaction list failed! stock_key:%s date:%s",
                             stock_key, date)
                continue
            if date - int(transaction_list[0]["date"]) > 7:
                logging.info("Long time not exchange! stock_key:%s date:%s",
                             stock_key, transaction_list[0]["date"])
                continue

            # 判断是否为潜力股
            if self.is_potential_stock(stock, transaction_list):
                potential_stock_list.append(stock_key)

        return potential_stock_list

    def is_potential_stock(self, stock, transaction_list):
        ''' 是否是潜力股
            @Desc: 1.涨幅在3%~5%之间
                   2.量比排名: 剔除量比小于1的股票
                   3.换手率排名: 剔除换手率>10%和<5%的股票
                   4.按市值排名: 剔除市值大于200亿和小于50亿的股票
                   5.成交量忽高忽低的删掉, 只保留成交量持续放大的股票
                   6.K线形态: 高位票或没有支撑的删掉
                   7.看分时图: 全天运行的分时均价上方必须强于大盘
                   8.两点半左右创出当日的新高, 回踩均线不跌破, 就是买点.
        '''
        curr = transaction_list[0] # 今天交易数据

        # 1.当天涨幅在2% ~ 5%之间
        ratio = self.label.ratio(curr["open_price"], curr["close_price"])
        if (ratio < 2) or (ratio > 5):
            logging.info("Price ratio out of [2, 5]. stock_key:%s ratio:%s",
                         stock["stock_key"], ratio)
            return False

        # 2.量比排名: 剔除量比小于1的股票
        #ratio = self.label.ratio(curr["ma5_volume"], 5 * curr["volume"])
        #if ratio < 1:
        #    logging.info("Volume ratio out of [1, ~). stock_key:%s ratio:%s",
        #                 stock["stock_key"], ratio)
        #    return False

        # 3.换手率排名: 剔除换手率>10%和<4%的股票
        if (curr["turnover_ratio"] > 10) or (curr["turnover_ratio"] < 4):
            logging.info("Turnover ratio out of [4, 10]. stock_key:%s turnover_ratio:%s",
                         stock["stock_key"], curr["turnover_ratio"])
            return False

        # 4.按市值排名: 剔除市值大于200亿和小于50亿的股票
        if (stock["market_cap"] > STOCK_MARKET_CAP_20B) \
                or (stock["market_cap"] < STOCK_MARKET_CAP_5B):
            logging.info("Market cap out of [5B, 20B]. stock_key:%s market_cap:%s",
                         stock["stock_key"], stock["market_cap"])
            return False

        # 5.成交量忽高忽低的删掉, 只保留成交量持续放大的股票
        index = 0
        max_index = len(transaction_list)-1
        while(index < max_index):
            if transaction_list[index]["volume"] <= transaction_list[index+1]["volume"]:
                logging.info("Volume not add. stock_key:%s", stock["stock_key"])
                return False
            index += 1 

        # 6.K线形态: 高位票或没有支撑的删掉

        # 7.看分时图: 全天运行的分时均价上方必须强于大盘

        # 8.两点半左右创出当日的新高, 回踩均线不跌破, 就是买点.

        return True

    def get_hsi_index(self):
        ''' 获取恒生指数数据 '''
        transaction_list = self.database.get_all_transaction_list_by_stock_key(STOCK_KEY_HSI)
        if (transaction_list is None) or (len(transaction_list) == 0):
            logging.error("Get hsi index transaction list failed! stock_key:%s",
                          STOCK_KEY_HSI)
            return None

        hsi_index_list = dict()

        for item in transaction_list:
            hsi_index_list[item["date"]] = item

        return hsi_index_list

    def get_hz2083_index(self):
        ''' 获取'恒生科技指数'数据 '''
        transaction_list = self.database.get_all_transaction_list_by_stock_key(STOCK_KEY_HZ2083)
        if (transaction_list is None) or (len(transaction_list) == 0):
            logging.error("Get hz2083 index transaction list failed! stock_key:%s",
                          STOCK_KEY_HZ2083)
            return None

        hz2083_index_list = dict()

        for item in transaction_list:
            hz2083_index_list[item["date"]] = item

        return hz2083_index_list

    def set_transaction_index(self, data):
        ''' 更新股票指标数据 '''

        curr_timestamp = int(time.time())
        data["create_time"] = time.localtime(curr_timestamp)
        data["update_time"] = time.localtime(curr_timestamp)

        # 更新预测结果
        self.database.set_transaction_index(data)


