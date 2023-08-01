# -*- coding:utf-8 -*-

# 数据库的查询和更新操作

import logging
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

        sql = f'INSERT INTO t_stock(`key`, name, create_time, update_time) \
                    VALUES(%s, %s, %s, %s)'

        cursor.execute(sql, (data["key"], data["name"], data["create_time"], data["update_time"]))

        self.mysql.commit()

        cursor.close()

        return None

    def _add_transaction(self, data):
        ''' 新增交易数据 '''

        logging.debug("Call _add_transaction(). data:%s", data)

        cursor = self.mysql.cursor()

        sql = f'INSERT INTO t_transaction(stock_key, date, \
                        open_price, close_price, top_price, \
                        bottom_price, volume, turnover, turnover_ratio, \
                        create_time, update_time) \
                    VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'

        execute = cursor.execute(sql, (data["stock_key"],
                                       data["date"],
                                       data["open_price"],
                                       data["close_price"],
                                       data["top_price"],
                                       data["bottom_price"],
                                       data["volume"],
                                       data["turnover"],
                                       data["turnover_ratio"],
                                       data["create_time"],
                                       data["update_time"]))

        self.mysql.commit()

        cursor.close()

        return None

    def _update_transaction(self, data):
        ''' 更新交易数据 '''

        logging.debug("Call _update_transaction(). data:%s", data)

        cursor = self.mysql.cursor()

        sql = f'UPDATE t_transaction SET open_price=%s, \
                        close_price=%s, \
                        top_price=%s, \
                        bottom_price=%s, \
                        volume=%s, \
                        turnover=%s, \
                        turnover_ratio=%s, \
                        update_time=%s \
                    WHERE stock_key=%s AND date=%s'

        cursor.execute(sql, (data["open_price"],
                             data["close_price"],
                             data["top_price"],
                             data["bottom_price"],
                             data["volume"],
                             data["turnover"],
                             data["turnover_ratio"],
                             data["update_time"],
                             data["stock_key"], data["date"]))

        self.mysql.commit()

        cursor.close()

    def set_transaction(self, data):
        ''' 更新交易数据: 开盘价、最高价、最低价、收盘价、交易量、交易额、换手率等
            @Param data: 股票交易信息(dict类型)
        '''
        old_data = self.get_transaction(data["stock_key"], data["date"])
        if old_data is None:
            return self._add_transaction(data)
        return self._update_transaction(data)

    def get_transaction(self, stock_key, date):
        ''' 获取指定交易数据
            @Param stock_key: 股票KEY
            @Param date: 交易日期(格式: YYYYMMDD)
        '''

        # 查询交易数据
        cursor = self.mysql.cursor()

        sql = f'SELECT stock_key, date, \
                    open_price, close_price, \
                    top_price, bottom_price, \
                    volume, turnover \
                FROM t_transaction \
                WHERE stock_key=%s AND date=%s'

        cursor.execute(sql, (stock_key, date))

        item = cursor.fetchone()

        cursor.close()

        if item is None:
            logging.debug("Select data from database failed. stock_key:%s date:%s",
                          stock_key, date)
            return None

        # 数据整合处理
        data = dict()

        data["stock_key"] = str(item[0])
        data["date"] = int(item[1])
        data["open_price"] = float(item[2])
        data["close_price"] = float(item[3])
        data["top_price"] = float(item[4])
        data["bottom_price"] = float(item[5])
        data["volume"] = float(item[6])
        data["turnover"] = float(item[7])

        return data

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

    def get_transaction_list(self, stock_key, date, num):
        ''' 获取指定日期往前的num条交易数据
            @Param stock_key: 股票KEY
            @Param date: 交易日期(格式: YYYYMMDD)
            @Param num: 交易条数
        '''

        # 查询交易数据
        cursor = self.mysql.cursor()

        sql = f'SELECT stock_key, date, open_price, \
                        close_price, top_price, bottom_price, \
                        volume, turnover, turnover_ratio \
                    FROM t_transaction \
                    WHERE stock_key=%s AND date<=%s ORDER BY date DESC LIMIT %s'

        cursor.execute(sql, (stock_key, date, num))

        items = cursor.fetchall()

        cursor.close()


        # 数据整合处理
        result = list()

        for item in items:
            data = dict()
            data["stock_key"] = str(item[0])
            data["date"] = int(item[1])
            data["open_price"] = float(item[2])
            data["close_price"] = float(item[3])
            data["top_price"] = float(item[4])
            data["bottom_price"] = float(item[5])
            data["volume"] = int(item[6])
            data["turnover"] = float(item[7])
            data["turnover_ratio"] = float(item[8])
            result.append(data)
        return result

    def _add_predict(self, data):
        ''' 新增预测数据 '''

        logging.debug("Call _add_predict(). data:%s", data)

        cursor = self.mysql.cursor()

        sql = f'INSERT INTO t_predict(stock_key, date, days, \
                        base_price, pred_price, pred_ratio, create_time, update_time) \
                    VALUES(%s,%s,%s,%s,%s,%s,%s,%s)'

        cursor.execute(sql, (data["stock_key"], data["date"],
                             data["days"], data["base_price"],
                             data["pred_price"], data["pred_ratio"],
                             data["create_time"], data["update_time"]))

        self.mysql.commit()

        cursor.close()

    def _update_predict(self, data):
        ''' 更新预测数据 '''

        logging.debug("Call _update_predict(). data:%s", data)

        cursor = self.mysql.cursor()

        sql = f'UPDATE t_predict SET base_price=%s, \
                        pred_price=%s,pred_ratio=%s,update_time=%s \
                    WHERE stock_key=%s AND date=%s AND days=%s'

        cursor.execute(sql, (data["base_price"], data["pred_price"],
                             data["pred_ratio"], data["update_time"],
                             data["stock_key"], data["date"], data["days"]))

        self.mysql.commit()

        cursor.close()

    def update_predict_real(self, data):
        ''' 更新预测数据中的真实数据 '''

        logging.debug("Call _update_predict_real(). data:%s", data)

        cursor = self.mysql.cursor()

        sql = f'UPDATE t_predict SET real_price=%s, \
                        real_ratio=%s,update_time=%s \
                    WHERE stock_key=%s AND date=%s AND days=%s'

        cursor.execute(sql, (data["real_price"],
                             data["real_ratio"], data["update_time"],
                             data["stock_key"], data["date"], data["days"]))

        self.mysql.commit()

        cursor.close()



    def set_predict(self, data):
        ''' 设置预测数据
            @Param data: 预测信息
        '''

        logging.debug("Call set_predict(). data:%s", data)

        old_data = self.get_predict(data["stock_key"], data["date"], data["days"])
        if old_data is None:
            return self._add_predict(data)
        return self._update_predict(data)

    def get_predict(self, stock_key, date, days):
        ''' 获取指定日期指定周期的预测结果
            @Param stock_key: 股票KEY
            @Param date: 交易日期(格式: YYYYMMDD)
            @Param days: 预测周期(天数)
        '''

        # 查询交易数据
        cursor = self.mysql.cursor()

        sql = f'SELECT stock_key, date, days, \
                        base_price, pred_price, pred_ratio \
                    FROM t_predict \
                    WHERE stock_key=%s AND date=%s AND days=%s'

        cursor.execute(sql, (stock_key, date, days))

        item = cursor.fetchone()

        cursor.close()

        if item is None:
            logging.debug("Select data from database failed. stock_key:%s date:%s days:%s",
                          stock_key, date, days)
            return None

        # 数据整合处理
        data = dict()

        data["stock_key"] = str(item[0])
        data["date"] = int(item[1])
        data["days"] = float(item[2])
        data["base_price"] = float(item[3])
        data["pred_price"] = float(item[4])
        data["pred_ratio"] = float(item[5])

        return data

if __name__ == "__main__":
    db = Database()

    result = db.get_transaction_list("hkex:0700", "20211231", 30)

    for item in result:
        print(item)
