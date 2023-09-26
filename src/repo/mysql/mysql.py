import pymysql

from dbutils.pooled_db import PooledDB


class MySQLPool:
    def __init__(self, host, port, user, password, database):
        self.pool = PooledDB(
            creator=pymysql,  # 使用链接数据库的模块
            mincached=10,  # 初始化时，链接池中至少创建的链接，0表示不创建
            maxconnections=10000,  # 连接池允许的最大连接数，0和None表示不限制连接数
            blocking=True,  # 连接池中如果没有可用连接后，是否阻塞等待。True，等待；False，不等待然后报错
            host=host,
            port=port,
            user=user,
            password=password,
            database=database)

    def open(self):
        """ 获取连接 """
        conn = self.pool.connection()
        cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)  # 表示读取的数据为字典类型
        return conn, cursor

    def close(self, conn, cursor):
        """ 关闭连接 """
        cursor.close()
        conn.close()
