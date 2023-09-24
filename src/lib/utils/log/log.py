# -*- coding:utf-8 -*-
# 君子爱财 取之YOODO!

import logging

def log_init(fname):
    """ 初始化日志 """

    logging.basicConfig(
            level = logging.DEBUG,
            format ='%(asctime)s-%(levelname)s-%(message)s',
            datefmt = '%y-%m-%d %H:%M',
            filename = fname,
            filemode = 'a')
