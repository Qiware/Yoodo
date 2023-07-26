# -*- coding:utf-8 -*-
# 君子爱财 取之有道

import sys
import logging

import numpy as np
import matplotlib.pyplot as plt
from sklearn.preprocessing import PolynomialFeatures
from sklearn.linear_model import LinearRegression

sys.path.append("../lib/database")
from database import *

# 拉取训练交易数据条目
GET_TRANSACTION_NUM = 1000

# 单条训练样本拥有的交易数据条目
TRAIN_DATA_TRANSACTION_NUM = 30

# 股票训练数据占比
TRAIN_STOCK_RATE = 0.5

# 股票预测
class Predicter():
    def __init__(self):
        # 连接数据库
        self.database = Database()
        pass

    def price_rate(self, open_price, close_price):
        ''' 计算价格上浮率 '''
        return (close_price - open_price) / open_price * 100

    def gen_train_data(self, date):
        ''' 生成训练数据 '''
        fp = open(str(date)+".csv", "w")

        # 获取股票列表
        stock_list = self.database.get_all_stock()

        count = 0
        for stock in stock_list:
            stock_key = stock["key"]

            # 拉取交易数据
            transaction_list = self.database.get_transaction(stock_key, date, GET_TRANSACTION_NUM)

            # 生成训练样本
            self.gen_train_data_by_transaction_list(stock_key, transaction_list, fp)

            # 预留一定比例做实验
            count += 1
            if (float(count) / len(stock_list)) > TRAIN_STOCK_RATE:
                break

        fp.close()

        return None

    def gen_train_data_by_transaction_list(self, stock_key, transaction_list, fp):
        ''' 通过交易列表生成训练数据
            @Param stock_key: 股票KEY
            @Param transaction_list: 交易数据
            @Param fname: 训练数据输出文件
        '''
        train_data_list = list()

        # 判断参数合法性
        if len(transaction_list) < TRAIN_DATA_TRANSACTION_NUM:
            logging.error("Transaction data not enough! stock_key:%s", stock_key)
            return None

        offset = len(transaction_list) - TRAIN_DATA_TRANSACTION_NUM
        while (offset > 0):
            train_data = ""
            # 生成训练数据
            index = offset + TRAIN_DATA_TRANSACTION_NUM - 1
            while (index >= offset):
                if (index - TRAIN_DATA_TRANSACTION_NUM + 1) == offset:
                    train_data += "%f,%f,%f,%f,%d,%f,%f" % (transaction_list[index]["open_price"],
                                                            transaction_list[index]["close_price"],
                                                            transaction_list[index]["top_price"],
                                                            transaction_list[index]["bottom_price"],
                                                            transaction_list[index]["volume"],
                                                            transaction_list[index]["turnover"],
                                                            self.price_rate(transaction_list[index]["open_price"], transaction_list[index]["close_price"]))
                else:
                    train_data += ",%f,%f,%f,%f,%d,%d,%f" % (transaction_list[index]["open_price"],
                                                            transaction_list[index]["close_price"],
                                                            transaction_list[index]["top_price"],
                                                            transaction_list[index]["bottom_price"],
                                                            transaction_list[index]["volume"],
                                                            transaction_list[index]["turnover"],
                                                            self.price_rate(transaction_list[index]["open_price"], transaction_list[index]["close_price"]))
                index -= 1
            # 设置预测结果(往前一天的收盘价 与 往后一天的收盘价做对比)
            price_rate = self.price_rate(transaction_list[offset]["close_price"], transaction_list[offset-1]["close_price"])
            train_data += ",%f\n" % (price_rate)

            train_data_list.append(train_data)
            offset -= 1

        # 将结果输出至文件
        fp.writelines(train_data_list)

        return None

    def train_model(self, date):
        ''' 模型训练 '''
        fpath = str(date) + ".csv"

        # 读取数据
        data = np.genfromtxt(fpath, delimiter=',')
        x_data = data[1:, 1]
        y_data = data[1:, 2]
        print("data: ", data)
        print("x_data: ", x_data)
        print("y_data: ", y_data)

        # 一维数据通过增加维度转为二维数据
        x_2data = x_data[:, np.newaxis]
        y_2data = data[1:, 2, np.newaxis]

        print("x_2data: ", x_2data)
        print("y_2data: ", y_2data)

        # 训练一元线性模型
        model = LinearRegression()
        model.fit(x_2data, y_2data)

        plt.plot(x_2data, y_2data, 'b.')
        plt.plot(x_2data, model.predict(x_2data), 'r')

        # 定义多项式回归：其本质是将变量x，根据degree的值转换为相应的多项式（非线性回归），eg: degree=3,则回归模型
        # 变为 y = theta0 + theta1 * x + theta2 * x^2 + theta3 * x^3
        #poly_reg = PolynomialFeatures(degree=3)
        poly_reg = PolynomialFeatures(degree=63)
        # 特征处理
        x_ploy = poly_reg.fit_transform(x_2data)  # 这个方法实质是把非线性的模型转为线性模型进行处理，
        # 处理方法就是把多项式每项的样本数据根据幂次数计算出相应的样本值(详细理解可以参考我的博文：https://blog.csdn.net/qq_34720818/article/details/103349452)

        # 训练线性模型（其本质是非线性模型，是由非线性模型转换而来）
        lin_reg_model = LinearRegression()
        lin_reg_model.fit(x_ploy, y_2data)

        plt.plot(x_2data, y_2data, 'b.')
        plt.plot(x_2data, lin_reg_model.predict(x_ploy), 'r')

        plt.show()

        return None

    def train(self, date):
        ''' 训练数据
            @Param date: 从哪天开始训练(YYYYMMDD)
        '''
        # 生成训练数据
        #self.gen_train_data(date)

        # 进行模型训练
        self.train_model(date)

if __name__ == "__main__":
    predict = Predicter()

    predict.train("20230726")
