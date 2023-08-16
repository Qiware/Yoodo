# -*- coding:utf-8 -*-
# 君子爱财 取之有道

import sys
import time
import joblib
import logging

sys.path.append("../../lib/repo/log")
from log import *
sys.path.append("../../lib/repo/database")
from database import *

# 单条训练样本拥有的交易数据条目
TRAIN_DATA_TRANSACTION_NUM = 20

# 数据处理
class Data():
    def __init__(self):
        # 连接数据库
        self.database = Database()
        pass

    def ratio(self, start_val, end_val):
        ''' 波动比率
            @Param start_val: 开始值
            @Param end_val: 结束值
        '''
        diff = (end_val - start_val)
        if diff == 0:
            return 0
        if (start_val == 0):
            return 100
        return  diff / start_val * 100

    def gen_train_data_fpath(self, date, days):
        ''' 生成训练数据的路径 '''
        return "../../../data/train/%s-%ddays.dat" % (str(date), int(days))

    def group_transaction_by_days(self, transaction_list, days):
        ''' 交易数据间隔days分组 '''

        transaction_group = list()
        group_num = int(len(transaction_list) / days)

        num = int(0)
        while (num < group_num):
            index = num * days

            item = dict()

            item["stock_key"] = transaction_list[index]["stock_key"] # 股票KEY
            item["open_price"] = transaction_list[index]["open_price"] # 开盘价(取第一天开盘价)
            item["close_price"] = transaction_list[index+days-1]["close_price"] # 收盘价(取最后一天收盘价)

            item["top_price"] = transaction_list[index]["top_price"] # 最高价
            item["bottom_price"] = transaction_list[index]["bottom_price"] # 最低价
            item["volume"] = transaction_list[index]["volume"] # 交易量
            item["turnover"] = transaction_list[index]["turnover"] # 交易额
            item["turnover_ratio"] = transaction_list[index]["turnover_ratio"] # 换手率

            item["ma5_avg_price"] = transaction_list[index]["ma5_avg_price"] # MA5平均价格
            item["ma10_avg_price"] = transaction_list[index]["ma10_avg_price"] # MA10平均价格
            item["ma20_avg_price"] = transaction_list[index]["ma20_avg_price"] # MA20平均价格

            item["ma5_volume"] = transaction_list[index]["ma5_volume"] # MA5交易量
            item["ma10_volume"] = transaction_list[index]["ma10_volume"] # MA10交易量
            item["ma20_volume"] = transaction_list[index]["ma20_volume"] # MA20交易量

            index += 1
            while (index < (num+1)*days):
                item["top_price"] = max(item["top_price"], transaction_list[index]["top_price"])
                item["bottom_price"] = min(item["bottom_price"], transaction_list[index]["bottom_price"])

                item["volume"] += transaction_list[index]["volume"]
                item["turnover"] += transaction_list[index]["turnover"]
                item["turnover_ratio"] += transaction_list[index]["turnover_ratio"]

                item["ma5_avg_price"] += transaction_list[index]["ma5_avg_price"]
                item["ma10_avg_price"] += transaction_list[index]["ma10_avg_price"]
                item["ma20_avg_price"] += transaction_list[index]["ma20_avg_price"]

                item["ma5_volume"] += transaction_list[index]["ma5_volume"]
                item["ma10_volume"] += transaction_list[index]["ma10_volume"]
                item["ma20_volume"] += transaction_list[index]["ma20_volume"]

                index += 1
            item["ma5_avg_price"] /= days
            item["ma10_avg_price"] /= days
            item["ma20_avg_price"] /= days
            logging.debug("Transaction group data:%s", item)
            transaction_group.append(item)
            num += 1

        logging.info("transaction_list:%d transaction_group:%d",
                     len(transaction_list), len(transaction_group))

        return transaction_group

    def gen_train_data_by_days(self, date, days, num):
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

            # 拉取交易数据
            transaction_list = self.database.get_transaction_list(stock_key, date, num)

            # 交易数据聚合分组
            transaction_group = self.group_transaction_by_days(transaction_list, days)

            # 生成训练样本
            self.gen_train_data_by_transaction_list(stock_key, transaction_group, fp)
            #self.gen_class_train_data_by_transaction_list(stock_key, transaction_group, fp)

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
            index = offset + TRAIN_DATA_TRANSACTION_NUM - 1
            while (index >= offset):
                curr = transaction_list[index]
                prev = transaction_list[index+1]
                if (index - TRAIN_DATA_TRANSACTION_NUM + 1) != offset:
                    train_data += ","
                # 开盘价波动率(与'前周期'比较)
                train_data += "%f" % (self.ratio(prev["close_price"], curr["open_price"]))
                # 收盘价波动率(与'前周期'比较)
                train_data += ",%f" % (self.ratio(prev["close_price"], curr["close_price"]))
                # 最高价波动率(与'前周期'比较)
                train_data += ",%f" % (self.ratio(prev["close_price"], curr["top_price"]))
                # 最低价波动率(与'前周期'比较)
                train_data += ",%f" % (self.ratio(prev["close_price"], curr["bottom_price"]))
                # 交易量波动率(与'前周期'比较)
                train_data += ",%f" % (self.ratio(prev["volume"], curr["volume"]))
                # 交易额波动率(与'前周期'比较)
                train_data += ",%f" % (self.ratio(prev["turnover"], curr["turnover"]))
                # 换手率波动率(与'前周期'比较)
                train_data += ",%f" % (self.ratio(prev["turnover_ratio"], curr["turnover_ratio"]))

                # 收盘价波动率(与'本周期'开盘价比较)
                train_data += ",%f" % (self.ratio(curr["open_price"], curr["close_price"]))
                # 最高价波动率(与'本周期'开盘价比较)
                train_data += ",%f" % (self.ratio(curr["open_price"], curr["top_price"]))
                # 最低价波动率(与'本周期'开盘价比较)
                train_data += ",%f" % (self.ratio(curr["open_price"], curr["bottom_price"]))

                # 最高价和收盘价比较(与'本周期'收盘价比较)
                train_data += ",%f" % (self.ratio(curr["close_price"], curr["top_price"]))
                # 最低价和收盘价比较(与'本周期'收盘价比较)
                train_data += ",%f" % (self.ratio(curr["close_price"], curr["bottom_price"]))

                # 换手率
                train_data += ",%f" % (curr["turnover_ratio"])

                # 收盘价和平均价格的比值
                train_data += ",%f" % (self.ratio(curr["ma5_avg_price"], curr["close_price"]))
                train_data += ",%f" % (self.ratio(curr["ma10_avg_price"], curr["close_price"]))
                train_data += ",%f" % (self.ratio(curr["ma20_avg_price"], curr["close_price"]))

                # 交易量和平均交易量的比值
                train_data += ",%f" % (self.ratio(curr["ma5_volume"], curr["volume"]))
                train_data += ",%f" % (self.ratio(curr["ma10_volume"], curr["volume"]))
                train_data += ",%f" % (self.ratio(curr["ma20_volume"], curr["volume"]))

                index -= 1
            # 设置预测结果(往前一天的收盘价 与 往后一天的收盘价做对比)
            price_ratio = self.ratio(
                    transaction_list[offset]["close_price"],
                    transaction_list[offset-1]["close_price"])
            train_data += ",%f\n" % (price_ratio)

            train_data_list.append(train_data)
            offset -= 1

        # 将结果输出至文件
        fp.writelines(train_data_list)

        return None

    def gen_class_train_data_by_transaction_list(self, stock_key, transaction_list, fp):
        ''' 通过交易列表生成训练数据(分类)
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
            index = offset + TRAIN_DATA_TRANSACTION_NUM - 1
            while (index >= offset):
                curr = transaction_list[index]
                prev = transaction_list[index+1]
                if (index - TRAIN_DATA_TRANSACTION_NUM + 1) != offset:
                    train_data += ","
                # 开盘价波动率(与'前周期'比较)
                train_data += "%f" % (self.ratio(prev["close_price"], curr["open_price"]))
                # 收盘价波动率(与'前周期'比较)
                train_data += ",%f" % (self.ratio(prev["close_price"], curr["close_price"]))
                # 最高价波动率(与'前周期'比较)
                train_data += ",%f" % (self.ratio(prev["close_price"], curr["top_price"]))
                # 最低价波动率(与'前周期'比较)
                train_data += ",%f" % (self.ratio(prev["close_price"], curr["bottom_price"]))
                # 交易量波动率(与'前周期'比较)
                train_data += ",%f" % (self.ratio(prev["volume"], curr["volume"]))
                # 交易额波动率(与'前周期'比较)
                train_data += ",%f" % (self.ratio(prev["turnover"], curr["turnover"]))
                # 换手率波动率(与'前周期'比较)
                train_data += ",%f" % (self.ratio(prev["turnover_ratio"], curr["turnover_ratio"]))

                # 收盘价波动率(与'本周期'开盘价比较)
                train_data += ",%f" % (self.ratio(curr["open_price"], curr["close_price"]))
                # 最高价波动率(与'本周期'开盘价比较)
                train_data += ",%f" % (self.ratio(curr["open_price"], curr["top_price"]))
                # 最低价波动率(与'本周期'开盘价比较)
                train_data += ",%f" % (self.ratio(curr["open_price"], curr["bottom_price"]))

                # 最高价和收盘价比较(与'本周期'收盘价比较)
                train_data += ",%f" % (self.ratio(curr["close_price"], curr["top_price"]))
                # 最低价和收盘价比较(与'本周期'收盘价比较)
                train_data += ",%f" % (self.ratio(curr["close_price"], curr["bottom_price"]))

                # 换手率
                train_data += ",%f" % (curr["turnover_ratio"])

                # 收盘价和平均价格的比值
                train_data += ",%f" % (self.ratio(curr["ma5_avg_price"], curr["close_price"]))
                train_data += ",%f" % (self.ratio(curr["ma10_avg_price"], curr["close_price"]))
                train_data += ",%f" % (self.ratio(curr["ma20_avg_price"], curr["close_price"]))

                # 交易量和平均交易量的比值
                train_data += ",%f" % (self.ratio(curr["ma5_volume"], curr["volume"]))
                train_data += ",%f" % (self.ratio(curr["ma10_volume"], curr["volume"]))
                train_data += ",%f" % (self.ratio(curr["ma20_volume"], curr["volume"]))

                index -= 1
            # 设置预测结果(往前一天的收盘价 与 往后一天的收盘价做对比)
            price_ratio = self.ratio(
                    transaction_list[offset]["close_price"],
                    transaction_list[offset-1]["close_price"])
            train_data += ",%d\n" % (int(price_ratio/5))

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
            @Param stock_key: 股票KEY
            @Param transaction_list: 交易数据
            @Param fname: 训练数据输出文件
        '''
        # 判断参数合法性
        if len(transaction_list) < (TRAIN_DATA_TRANSACTION_NUM+1):
            logging.error("Transaction data not enough! stock_key:%s", stock_key)
            return None

        feature = list()

        # 生成特征数据(注意: 最有一个作为起始基准)
        index = TRAIN_DATA_TRANSACTION_NUM - 1
        while (index >= 0):
            curr = transaction_list[index]
            prev = transaction_list[index+1]
            # 与前周期的比较
            feature.append(self.ratio(prev["close_price"], curr["open_price"]))
            feature.append(self.ratio(prev["close_price"], curr["close_price"]))
            feature.append(self.ratio(prev["close_price"], curr["top_price"]))
            feature.append(self.ratio(prev["close_price"], curr["bottom_price"]))
            feature.append(self.ratio(prev["volume"], curr["volume"]))
            feature.append(self.ratio(prev["turnover"], curr["turnover"]))
            feature.append(self.ratio(prev["turnover_ratio"], curr["turnover_ratio"]))
            # 与本周期的开盘价比较
            feature.append(self.ratio(curr["open_price"], curr["close_price"]))
            feature.append(self.ratio(curr["open_price"], curr["top_price"]))
            feature.append(self.ratio(curr["open_price"], curr["bottom_price"]))
            # 与本周期的收盘价比较
            feature.append(self.ratio(curr["close_price"], curr["top_price"]))
            feature.append(self.ratio(curr["close_price"], curr["bottom_price"]))
            # 换手率
            feature.append(curr["turnover_ratio"])
            # 收盘价和平均价格的比值
            feature.append(self.ratio(curr["ma5_avg_price"], curr["close_price"]))
            feature.append(self.ratio(curr["ma10_avg_price"], curr["close_price"]))
            feature.append(self.ratio(curr["ma20_avg_price"], curr["close_price"]))
            # 交易量和平均交易量的比值
            feature.append(self.ratio(curr["ma5_volume"], curr["volume"]))
            feature.append(self.ratio(curr["ma10_volume"], curr["volume"]))
            feature.append(self.ratio(curr["ma20_volume"], curr["volume"]))
            index -= 1

        logging.info("Generate feature by transaction list success. stock_key:%s transaction_list:%d feature:%d",
                     stock_key, len(transaction_list), len(feature))

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

        # 交易数据聚合分组
        transaction_group = self.group_transaction_by_days(transaction_list, days)

        # 生成训练样本
        item = self.gen_feature_by_transaction_list(stock_key, transaction_group)
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
