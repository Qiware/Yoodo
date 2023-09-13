# -*- coding:utf-8 -*-

# 数据库的查询和更新操作

import logging
import pymysql

# 优质股票市值 >= 100亿
STOCK_GOOD_MARKET_CAP = 10000000000

# 数据库操作
class Database():
    def __init__(self):
        ''' 初始化 '''
        self.mysql = pymysql.connect(host="localhost", port=3306, user="root", password="", db="exchange", charset="utf8")

    def gen_insert_sql(self, table, data):
        ''' 生成SQL INSERT语句 '''

        sql = "INSERT INTO " + table + "("

        conditions = list()

        index = 0
        for key in data.keys():
            if index != 0:
                sql += ","
            sql += key
            conditions.append(data[key])
            index += 1
        sql += ") VALUES("

        index = 0
        for key in data.keys():
            if index != 0:
                sql += ","
            sql += "%s"
            index += 1
        sql += ")"

        return sql, tuple(conditions)

    def set_stock(self, data):
        ''' 更新股票基础信息: 股票代码、企业名称等
            @Param data: 股票基础信息(dict类型)
        '''

        logging.debug("Set stock. data:%s", data)

        old_data = self.get_stock(data["stock_key"])
        if old_data is None:
            return self._add_stock(data)
        return self._update_stock(data)

    def _add_stock(self, data):
        ''' 新增股票数据 '''

        logging.debug("Call _add_stock(). data:%s", data)

        # 生成SQL语句
        sql, conditions = self.gen_insert_sql("t_stock", data)

        logging.debug("sql:%s conditions:%s", sql, conditions)

        # 执行SQL语句
        cursor = self.mysql.cursor()

        execute = cursor.execute(sql, conditions)

        self.mysql.commit()

        cursor.close()

        return None

    def _update_stock(self, data):
        ''' 更新股票数据 '''

        logging.debug("Call _update_stock(). data:%s", data)

        # 生成SQL语句
        sql = f'UPDATE t_stock SET '

        index = 0
        conditions = list()

        for key in data.keys():
            if (key == "stock_key"):
                continue
            if index != 0:
                sql += ","
            sql += key+"=%s"
            conditions.append(data[key])
            index += 1
        sql += " WHERE stock_key=%s"

        logging.debug("sql: %s", sql)

        conditions.append(data["stock_key"])

        # 执行SQL语句
        cursor = self.mysql.cursor()

        cursor.execute(sql, tuple(conditions))

        self.mysql.commit()

        cursor.close()


    def _add_transaction(self, data):
        ''' 新增交易数据 '''

        logging.debug("Call _add_transaction(). data:%s", data)

        # 生成SQL语句
        sql, conditions = self.gen_insert_sql("t_transaction", data)

        logging.debug("sql: %s", sql)

        # 执行SQL语句
        cursor = self.mysql.cursor()

        execute = cursor.execute(sql, conditions)

        self.mysql.commit()

        cursor.close()

        return None

    def _update_transaction(self, data):
        ''' 更新交易数据 '''

        logging.debug("Call _update_transaction(). data:%s", data)

        # 生成SQL语句
        sql = f'UPDATE t_transaction SET '

        index = 0
        conditions = list()

        for key in data.keys():
            if (key == "stock_key") or (key == "date"):
                continue
            if index != 0:
                sql += ","
            sql += key+"=%s"
            conditions.append(data[key])
            index += 1
        sql += " WHERE stock_key=%s AND date=%s"

        logging.debug("sql: %s", sql)

        conditions.append(data["stock_key"])
        conditions.append(data["date"])

        # 执行SQL语句
        cursor = self.mysql.cursor()

        cursor.execute(sql, tuple(conditions))

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
                    volume, turnover, turnover_ratio \
                FROM t_transaction \
                WHERE stock_key=%s AND date=%s'

        cursor.execute(sql, (stock_key, date))

        item = cursor.fetchone()

        cursor.close()

        if item is None:
            logging.debug("No found. stock_key:%s date:%s", stock_key, date)
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
        data["turnover_ratio"] = float(item[8])

        return data

    def get_stock(self, stock_key):
        ''' 获取指定股票信息
            @Param stock_key: 股票KEY
        '''

        # 查询股票列表
        cursor = self.mysql.cursor()

        sql = f'SELECT stock_key, name, total, market_cap, disable \
                FROM t_stock \
                WHERE stock_key=%s'

        cursor.execute(sql, (stock_key))

        item = cursor.fetchone()

        cursor.close()

        if item is None:
            logging.error("Get stock data failed! stock_key:%s", stock_key)
            return None

        # 数据整合处理
        data = dict()
        data["stock_key"] = item[0]
        data["name"] = item[1]
        data["total"] = int(item[2])
        data["market_cap"] = float(item[3])
        data["disable"] = int(item[4])

        return data

    def get_all_stock(self):
        ''' 获取所有股票列表 '''

        # 查询股票列表
        cursor = self.mysql.cursor()

        sql = f'SELECT stock_key, name, total, market_cap, disable \
                FROM t_stock \
                WHERE disable=0'

        cursor.execute(sql)

        items = cursor.fetchall()

        cursor.close()

        # 数据整合处理
        result = list()

        for item in items:
            data = dict()
            data["stock_key"] = str(item[0]) # 股票KEY
            data["name"] = str(item[1]) # 企业名称
            data["total"] = int(item[2]) # 总股本数
            data["market_cap"] = float(item[3]) # 总市值
            data["disable"] = int(item[4]) # 是否禁用
            result.append(data)
        return result

    def get_good_stock(self):
        ''' 获取优质股票列表
            @Note: 市值超过100亿的股票, 则认为是优质股票.
        '''

        # 查询股票列表
        cursor = self.mysql.cursor()

        sql = f'SELECT stock_key, name, total, market_cap, disable \
                FROM t_stock \
                WHERE market_cap>=%s AND disable=0 AND second_classification LIKE %s'

        cursor.execute(sql, (STOCK_GOOD_MARKET_CAP, "%互联网%"))

        items = cursor.fetchall()

        cursor.close()

        # 数据整合处理
        result = list()

        for item in items:
            data = dict()
            data["stock_key"] = item[0] # 股票KEY
            data["name"] = item[1] # 企业名称
            data["total"] = int(item[2]) # 总股本数
            data["market_cap"] = float(item[3]) # 总市值
            data["disable"] = int(item[4]) # 是否禁用
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

    def get_all_transaction_list_by_stock_key(self, stock_key):
        ''' 获取指定日期往前的num条交易数据
            @Param stock_key: 股票KEY
        '''

        # 查询交易数据
        cursor = self.mysql.cursor()

        sql = f'SELECT stock_key, date, open_price, \
                        close_price, top_price, bottom_price, \
                        volume, turnover, turnover_ratio \
                    FROM t_transaction \
                    WHERE stock_key=%s ORDER BY date DESC'

        cursor.execute(sql, (stock_key))

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

        # 生成SQL语句
        sql, conditions = self.gen_insert_sql("t_predict", data)

        logging.debug("sql:%s conditions:%s", sql, conditions)

        # 执行SQL语句
        cursor = self.mysql.cursor()

        execute = cursor.execute(sql, conditions)

        self.mysql.commit()

        cursor.close()

        return None

    def _update_predict(self, data):
        ''' 更新预测数据 '''

        logging.debug("Call _update_predict(). data:%s", data)

        cursor = self.mysql.cursor()

        sql = f'UPDATE t_predict SET base_date=%s, base_price=%s, \
                    pred_price=%s,pred_ratio=%s,update_time=%s \
                WHERE stock_key=%s AND date=%s AND days=%s'

        cursor.execute(sql, (data["base_date"], data["base_price"],
                             data["pred_price"], data["pred_ratio"],
                             data["update_time"], data["stock_key"],
                             data["date"], data["days"]))

        self.mysql.commit()

        cursor.close()

    def update_predict_real(self, data):
        ''' 更新预测数据中的真实数据 '''

        logging.debug("Call _update_predict_real(). data:%s", data)

        cursor = self.mysql.cursor()

        sql = f'UPDATE t_predict SET real_price=%s, \
                        real_ratio=%s,update_time=%s \
                    WHERE stock_key=%s AND base_date=%s AND days=%s'

        cursor.execute(sql, (data["real_price"],
                             data["real_ratio"], data["update_time"],
                             data["stock_key"], data["base_date"], data["days"]))

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

    def set_transaction_index(self, data):
        ''' 设置交易指数
            @Param data: 预测信息
        '''

        logging.debug("Call set_transaction_index(). data:%s", data)

        old_data = self.get_transaction_index(data["stock_key"], data["date"], data["name"])
        if old_data is None:
            return self._add_transaction_index(data)
        return self._update_transaction_index(data)

    def _add_transaction_index(self, data):
        ''' 新增交易指数 '''

        logging.debug("Call _add_transaction_index(). data:%s", data)

        # 生成SQL语句
        sql, conditions = self.gen_insert_sql("t_transaction_index", data)

        logging.debug("sql: %s", sql)

        # 执行SQL语句
        cursor = self.mysql.cursor()

        execute = cursor.execute(sql, conditions)

        self.mysql.commit()

        cursor.close()

        return None

    def _update_transaction_index(self, data):
        ''' 更新交易指数 '''

        logging.debug("Call _update_transaction_index(). data:%s", data)

        # 生成SQL语句
        sql = f'UPDATE t_transaction_index SET '

        index = 0
        conditions = list()

        for key in data.keys():
            if (key == "stock_key") or (key == "date") or (key == "name"):
                continue
            if index != 0:
                sql += ","
            sql += key+"=%s"
            conditions.append(data[key])
            index += 1
        sql += " WHERE stock_key=%s AND date=%s AND name=%s"

        logging.debug("sql: %s", sql)

        conditions.append(data["stock_key"])
        conditions.append(data["date"])
        conditions.append(data["name"])

        # 执行SQL语句
        cursor = self.mysql.cursor()

        cursor.execute(sql, tuple(conditions))

        self.mysql.commit()

        cursor.close()

    def get_transaction_index(self, stock_key, date, name):
        ''' 获取指定交易数据
            @Param stock_key: 股票KEY
            @Param date: 交易日期(格式: YYYYMMDD)
            @Param name: 指标名称(如: MACD)
        '''

        # 查询交易数据
        cursor = self.mysql.cursor()

        sql = f'SELECT stock_key, date, name, value \
                FROM t_transaction_index \
                WHERE stock_key=%s AND date=%s AND name=%s'

        cursor.execute(sql, (stock_key, date, name))

        item = cursor.fetchone()

        cursor.close()

        if item is None:
            logging.debug("No found. stock_key:%s date:%s name:%s",
                          stock_key, date, name)
            return None

        # 数据整合处理
        data = dict()

        data["stock_key"] = str(item[0])
        data["date"] = int(item[1])
        data["name"] = str(item[2])
        data["value"] = item[3]

        return data

    def get_transaction_index_list(self, stock_key, name, lastest_date):
        ''' 获取指定交易数据
            @Param stock_key: 股票KEY
            @Param name: 指标名称(如: MACD)
            @Param lastest_date: 最新交易日期(格式: YYYYMMDD)
        '''

        # 查询交易数据
        cursor = self.mysql.cursor()

        sql = f'SELECT stock_key, date, name, value \
                FROM t_transaction_index \
                WHERE stock_key=%s AND date<=%s AND name=%s'

        cursor.execute(sql, (stock_key, lastest_date, name))

        items = cursor.fetchall()

        cursor.close()

        if item is None:
            logging.debug("No found. stock_key:%s date:%s name:%s",
                          stock_key, date, name)
            return None

        # 数据整合处理
        data = dict()

        for item in items:
            date = int(item["date"])

            data[date] = dict()

            data[date]["stock_key"] = str(item[0])
            data[date]["date"] = int(item[1])
            data[date]["name"] = str(item[2])
            data[date]["value"] = item[3]

        return data



if __name__ == "__main__":
    db = Database()

    result = db.get_transaction_list("hkex:0700", "20211231", 30)

    for item in result:
        print(item)
