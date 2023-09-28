# -*- coding:utf-8 -*-

# 数据库的查询和更新操作

import logging
import sys

sys.path.append("../../repo/mysql")
from mysql import MySQLPool

# 优质股票市值 >= 100亿
GOOD_STOCK_MIN_MARKET_CAP = 10000000000


# 数据库操作
def gen_insert_sql(table, data):
    """ 生成SQL INSERT语句 """

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


class Database:
    def __init__(self):
        """ 初始化 """
        self.mysql = MySQLPool(
            host="localhost",
            port=3306,
            user="root",
            password="",
            database="exchange")

    def set_stock(self, data):
        """ 更新股票基础信息: 股票代码、企业名称等
            @Param data: 股票基础信息(dict类型)
        """

        # logging.debug("Set stock. data:%s", data)

        old_data = self.get_stock(data["stock_key"])
        if old_data is None:
            return self._add_stock(data)
        return self._update_stock(data)

    def _add_stock(self, data):
        """ 新增股票数据 """

        # 生成SQL语句
        sql, conditions = gen_insert_sql("t_stock", data)

        # logging.debug("sql:%s conditions:%s", sql, conditions)

        # 执行SQL语句
        conn, cursor = self.msyql.open()
        execute = cursor.execute(sql, conditions)
        conn.commit()
        self.mysql.close(conn, cursor)

        return None

    def _update_stock(self, data):
        """ 更新股票数据 """

        # 生成SQL语句
        sql = f'UPDATE t_stock SET '

        index = 0
        conditions = list()

        for key in data.keys():
            if (key == "stock_key"):
                continue
            if index != 0:
                sql += ","
            sql += key + "=%s"
            conditions.append(data[key])
            index += 1
        sql += " WHERE stock_key=%s"

        # logging.debug("sql: %s", sql)

        conditions.append(data["stock_key"])

        # 执行SQL语句
        conn, cursor = self.mysql.open()
        cursor.execute(sql, tuple(conditions))
        conn.commit()
        self.mysql.close(conn, cursor)

    def _add_transaction(self, data):
        """ 新增交易数据 """

        # 生成SQL语句
        sql, conditions = gen_insert_sql("t_transaction", data)

        # logging.debug("sql: %s", sql)

        # 执行SQL语句
        conn, cursor = self.mysql.open()
        execute = cursor.execute(sql, conditions)
        conn.commit()
        self.mysql.close(conn, cursor)

        return None

    def _update_transaction(self, data):
        """ 更新交易数据 """

        # 生成SQL语句
        sql = f'UPDATE t_transaction SET '

        index = 0
        conditions = list()

        for key in data.keys():
            if (key == "stock_key") or (key == "date"):
                continue
            if index != 0:
                sql += ","
            sql += key + "=%s"
            conditions.append(data[key])
            index += 1
        sql += " WHERE stock_key=%s AND date=%s"

        # logging.debug("sql: %s", sql)

        conditions.append(data["stock_key"])
        conditions.append(data["date"])

        # 执行SQL语句
        conn, cursor = self.mysql.open()
        cursor.execute(sql, tuple(conditions))
        conn.commit()
        self.mysql.close(conn, cursor)

    def set_transaction(self, data):
        """ 更新交易数据: 开盘价、最高价、最低价、收盘价、交易量、交易额、换手率等
            @Param data: 股票交易信息(dict类型)
        """
        old_data = self.get_transaction(data["stock_key"], data["date"])
        if old_data is None:
            return self._add_transaction(data)
        return self._update_transaction(data)

    def get_transaction(self, stock_key, date):
        """ 获取指定交易数据
            @Param stock_key: 股票KEY
            @Param date: 交易日期(格式: YYYYMMDD)
        """

        # 查询交易数据
        sql = f'SELECT stock_key, date, \
                    open_price, close_price, \
                    top_price, bottom_price, \
                    volume, turnover, turnover_ratio \
                FROM t_transaction \
                WHERE stock_key=%s AND date=%s'

        conn, cursor = self.mysql.open()
        cursor.execute(sql, (stock_key, date))
        item = cursor.fetchone()
        self.mysql.close(conn, cursor)

        return item

    def get_stock(self, stock_key):
        """ 获取指定股票信息
            @Param stock_key: 股票KEY
        """

        # 查询股票列表
        sql = f'SELECT stock_key, name, total, market_cap, \
                first_classification, second_classification, disable \
                FROM t_stock \
                WHERE stock_key=%s'

        conn, cursor = self.mysql.open()
        cursor.execute(sql, (stock_key))
        item = cursor.fetchone()
        self.mysql.close(conn, cursor)

        return item

    def get_all_stock(self):
        """ 获取所有股票列表 """

        # 查询股票列表
        sql = f'SELECT stock_key, name, total, market_cap, disable \
                FROM t_stock \
                WHERE disable=0 ORDER BY stock_key'

        conn, cursor = self.mysql.open()
        cursor.execute(sql)
        items = cursor.fetchall()
        self.mysql.close(conn, cursor)

        return items

    def get_all_index(self):
        """ 获取所有指数列表 """

        # 查询指数列表
        sql = f'SELECT index_key, name FROM t_index'

        conn, cursor = self.mysql.open()
        cursor.execute(sql)
        items = cursor.fetchall()
        self.mysql.close(conn, cursor)

        return items

    def get_good_stock(self):
        """ 获取优质股票列表
            @Note: 市值超过100亿的股票, 则认为是优质股票.
        """

        # 查询股票列表
        sql = f'SELECT stock_key, name, total, market_cap, \
                first_classification, second_classification, disable \
                FROM t_stock \
                WHERE market_cap>=%s AND disable=0 ORDER BY stock_key'

        conn, cursor = self.mysql.open()
        cursor.execute(sql, GOOD_STOCK_MIN_MARKET_CAP)
        items = cursor.fetchall()
        self.mysql.close(conn, cursor)

        return items

    def get_transaction_list(self, stock_key, date, num):
        """ 获取指定日期往前的num条交易数据(时间倒序)
            @Param stock_key: 股票KEY
            @Param date: 交易日期(格式: YYYYMMDD)
            @Param num: 交易条数
        """

        # 查询交易数据
        sql = f'SELECT stock_key, date, open_price, \
                        close_price, top_price, bottom_price, \
                        volume, turnover, turnover_ratio \
                    FROM t_transaction \
                    WHERE stock_key=%s AND date<=%s ORDER BY date DESC LIMIT %s'

        conn, cursor = self.mysql.open()
        cursor.execute(sql, (stock_key, date, num))
        items = cursor.fetchall()
        self.mysql.close(conn, cursor)

        # 数据整合处理
        return items

    def get_all_transaction_list_by_stock_key(self, stock_key):
        """ 获取指定日期往前的num条交易数据
            @Param stock_key: 股票KEY
        """

        # 查询交易数据
        sql = f'SELECT stock_key, date, open_price, \
                        close_price, top_price, bottom_price, \
                        volume, turnover, turnover_ratio \
                    FROM t_transaction \
                    WHERE stock_key=%s ORDER BY date DESC'

        conn, cursor = self.mysql.open()
        cursor.execute(sql, (stock_key))
        items = cursor.fetchall()
        self.mysql.close(conn, cursor)

        return items

    def _add_predict(self, data):
        """ 新增预测数据 """

        # 生成SQL语句
        sql, conditions = gen_insert_sql("t_predict", data)

        # logging.debug("sql:%s conditions:%s", sql, conditions)

        # 执行SQL语句
        conn, cursor = self.mysql.open()
        execute = cursor.execute(sql, conditions)
        conn.commit()
        self.mysql.close(conn, cursor)

        return None

    def _update_predict(self, data):
        """ 更新预测数据 """

        sql = f'UPDATE t_predict SET base_date=%s, base_price=%s, \
                    pred_price=%s,pred_ratio=%s,update_time=%s \
                WHERE stock_key=%s AND date=%s AND days=%s'

        conn, cursor = self.mysql.open()
        cursor.execute(sql, (data["base_date"], data["base_price"],
                             data["pred_price"], data["pred_ratio"],
                             data["update_time"], data["stock_key"],
                             data["date"], data["days"]))
        conn.commit()
        self.mysql.close(conn, cursor)

    def update_predict_real(self, data):
        """ 更新预测数据中的真实数据 """

        sql = f'UPDATE t_predict SET real_price=%s, \
                        real_ratio=%s,update_time=%s \
                    WHERE stock_key=%s AND base_date=%s AND days=%s'

        conn, cursor = self.mysql.open()
        cursor.execute(sql, (data["real_price"],
                             data["real_ratio"], data["update_time"],
                             data["stock_key"], data["base_date"], data["days"]))
        conn.commit()
        self.mysql.close(conn, cursor)

    def set_predict(self, data):
        """ 设置预测数据
            @Param data: 预测信息
        """

        old_data = self.get_predict(data["stock_key"], data["date"], data["days"])
        if old_data is None:
            return self._add_predict(data)
        return self._update_predict(data)

    def get_predict(self, stock_key, date, days):
        """ 获取指定日期指定周期的预测结果
            @Param stock_key: 股票KEY
            @Param date: 交易日期(格式: YYYYMMDD)
            @Param days: 预测周期(天数)
        """

        # 查询交易数据
        sql = f'SELECT stock_key, date, days, \
                        base_price, pred_price, pred_ratio \
                    FROM t_predict \
                    WHERE stock_key=%s AND date=%s AND days=%s'

        conn, cursor = self.mysql.open()
        cursor.execute(sql, (stock_key, date, days))
        item = cursor.fetchone()
        self.mysql.close(conn, cursor)

        if item is None:
            logging.debug("Select data from database failed. stock_key:%s date:%s days:%s",
                          stock_key, date, days)
            return None

        return item

    def set_technical_index(self, data):
        """ 设置交易指数
            @Param data: 预测信息
        """

        old_data = self.get_technical_index(data["stock_key"], data["date"])
        if old_data is None:
            return self._add_transaction_index(data)
        return self._update_transaction_index(data)

    def _add_transaction_index(self, data):
        """ 新增交易指数 """

        # 生成SQL语句
        sql, conditions = gen_insert_sql("t_technical_index", data)

        # logging.debug("sql: %s", sql)

        # 执行SQL语句
        conn, cursor = self.mysql.open()
        execute = cursor.execute(sql, conditions)
        conn.commit()
        self.mysql.close(conn, cursor)

        return None

    def _update_transaction_index(self, data):
        """ 更新交易指数 """

        # 生成SQL语句
        sql = f'UPDATE t_technical_index SET '

        index = 0
        conditions = list()

        for key in data.keys():
            if (key == "stock_key") or (key == "date"):
                continue
            if index != 0:
                sql += ","
            sql += key + "=%s"
            conditions.append(data[key])
            index += 1
        sql += " WHERE stock_key=%s AND date=%s"

        # logging.debug("sql: %s", sql)

        conditions.append(data["stock_key"])
        conditions.append(data["date"])

        # 执行SQL语句
        conn, cursor = self.mysql.open()
        cursor.execute(sql, tuple(conditions))
        conn.commit()
        self.mysql.close(conn, cursor)

    def get_technical_index(self, stock_key, date):
        """ 获取指定股票技术指标数据
            @Param stock_key: 股票KEY
            @Param date: 交易日期(格式: YYYYMMDD)
        """

        # 查询交易数据
        sql = f'SELECT stock_key, date, data \
                FROM t_technical_index \
                WHERE stock_key=%s AND date=%s'

        conn, cursor = self.mysql.open()
        cursor.execute(sql, (stock_key, date))
        item = cursor.fetchone()
        self.mysql.close(conn, cursor)

        if item is None:
            logging.debug("No found. stock_key:%s date:%s",
                          stock_key, date)
            return None

        return item

    def get_technical_index_dict(self, stock_key, lastest_date):
        """ 获取指定股票技术指标数据(字典)
            @Param stock_key: 股票KEY
            @Param lastest_date: 最新交易日期(格式: YYYYMMDD)
        """

        # 查询交易数据
        sql = f'SELECT stock_key, date, data \
                FROM t_technical_index \
                WHERE stock_key=%s AND date<=%s'

        conn, cursor = self.mysql.open()
        cursor.execute(sql, (stock_key, lastest_date))
        items = cursor.fetchall()
        self.mysql.close(conn, cursor)

        if items is None:
            logging.debug("No found. stock_key:%s date:%s", stock_key, date)
            return None

        # 数据梳理
        data = dict()
        for item in items:
            data[item["date"]] = item

        return data

    def get_technical_index_list(self, stock_key, lastest_date):
        """ 获取指定股票技术指标数据(列表)
            @Param stock_key: 股票KEY
            @Param lastest_date: 最新交易日期(格式: YYYYMMDD)
        """

        # 查询交易数据
        sql = f'SELECT stock_key, date, data \
                FROM t_technical_index \
                WHERE stock_key=%s AND date<=%s ORDER BY date'

        conn, cursor = self.mysql.open()
        cursor.execute(sql, (stock_key, lastest_date))
        items = cursor.fetchall()
        self.mysql.close(conn, cursor)

        return items


if __name__ == "__main__":
    db = Database()

    result = db.get_transaction_list("hkex:0700", "20211231", 30)

    for item in result:
        print(item)
