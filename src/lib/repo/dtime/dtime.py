# encoding=utf-8

import time

# 一天的时间秒
TIME_DAY_SEC = 86400

def date_to_timestamp(date):
    ''' 将日期转为时间戳
        @Param date: 日期. 格式:YYYYMMDD
    '''

    date_str = str(date)

    year = int(date_str[0:4])
    month = int(date_str[4:6])
    day = int(date_str[6:8])

    dt = "%04d-%02d-%02d 00:00:00" % (year, month, day)

    ta = time.strptime(dt, "%Y-%m-%d %H:%M:%S")

    return int(time.mktime(ta))

def timestamp_to_date(timestamp):
    ''' 将时间戳转为日期. 格式:YYYYMMDD '''
    lt = time.localtime(timestamp)

    return "%04d%02d%02d" % (lt.tm_year, lt.tm_mon, lt.tm_mday)


