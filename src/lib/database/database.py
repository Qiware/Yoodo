# -*- coding:utf-8 -*-

# 数据库的查询和更新操作

import pymysql

# 数据库操作
class Database():
    def __init__(self):
        ''' 初始化 '''
        self.mysql = pymysql.connect(host="localhost", port=3306, user="root", password="", db="exchange", charset="utf8")

    def set_stock(self, data):
        ''' 更新股票基础信息: 股票代码、企业名称等
            @Param data: 股票基础信息(dict类型)
        '''

        cursor = self.mysql.cursor()

        sql = f'INSERT INTO t_stock(`key`, name, create_time, update_time) VALUES(%s, %s, %s, %s)'

        cursor.execute(sql, (data["key"], data["name"], data["create_time"], data["update_time"]))

        self.mysql.commit()

        cursor.close()

        return None

    def set_transaction(self, data):
        ''' 更新交易数据: 开盘价、最高价、最低价、收盘价、交易量、交易额等
            @Param data: 股票交易信息(dict类型)
        '''
        cursor = self.mysql.cursor()

        sql = f'INSERT INTO t_transaction(stock_key, date, open_price, close_price, top_price, bottom_price, volume, turnover, create_time, update_time) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'

        execute = cursor.execute(sql, (data["stock_key"], data["date"], data["open_price"], data["close_price"], data["top_price"], data["bottom_price"], data["volume"], data["turnover"], data["create_time"], data["update_time"]))

        self.mysql.commit()

        cursor.close()

        return None

    def get_all_stock(self):
        ''' 获取所有股票列表 '''

        # 查询股票列表
        cursor = self.mysql.cursor()

        sql = f'SELECT `key`, name FROM t_stock'

        cursor.execute(sql)

        items = cursor.fetchall()

        cursor.close()

        # 数据整合处理
        result = list()

        for item in items:
            data = dict()
            data["key"] = item[0]
            data["name"] = item[1]
            result.append(data)
        return result

    def get_transaction(self, stock_key, date, num):
        ''' 获取指定日期往前的num条交易数据
            @Param stock_key: 股票KEY
            @Param date: 交易日期(格式: YYYYMMDD)
            @Param num: 交易条数
        '''

        # 查询交易数据
        cursor = self.mysql.cursor()

        sql = f'SELECT stock_key, date, open_price, close_price, top_price, bottom_price, volume, turnover FROM t_transaction WHERE stock_key=%s AND date<=%s ORDER BY date DESC LIMIT %s'

        cursor.execute(sql, (stock_key, date, num))

        items = cursor.fetchall()

        cursor.close()


        # 数据整合处理
        result = list()

        for item in items:
            data = dict()
            data["stock_key"] = item[0]
            data["date"] = item[1]
            data["open_price"] = item[2]
            data["close_price"] = item[3]
            data["top_price"] = item[4]
            data["bottom_price"] = item[5]
            data["volume"] = item[6]
            data["turnover"] = item[7]
            result.append(data)
        return result

if __name__ == "__main__":
    db = Database()

    result = db.get_transaction("hkex:0700", "20211231", 30)

    for item in result:
        print(item)
