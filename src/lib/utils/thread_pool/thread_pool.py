# -*- coding:utf-8 -*-
# 君子爱财 取之YOODO!

import logging
import threading
import time

WORKER_NUM = 10  # 默认线程数量


# 处理消息
class Message:
    def __init__(self, typ, data):
        self.type = typ
        self.data = data


# 线程池
class ThreadPool:
    def __init__(self, capacity, worker_num):
        """ 初始化
            @Param capacity: 队列容量
            @Param worker_num: 工作线程数量
        """
        self.is_exit = False  # 是否退出
        self.callback = dict()  # 回调注册表
        self.capacity = capacity  # 队列容量

        self.pop_count = 0  # POP消息数
        self.push_count = 0  # PUSH消息数
        self.wait_queue = list()  # 处理等待队列

        # 启动多个工程线程
        self.worker_num = worker_num
        if self.worker_num <= 0:
            self.worker_num = WORKER_NUM

        for i in range(self.worker_num):
            wt = threading.Thread(target=self.handle, args=())
            wt.setDaemon(True)
            wt.start()

    def handle(self):
        """ 工作线程入口函数 """
        while True:
            if self.is_exit:
                return

            try:
                # 获取数据
                message = self.wait_queue.pop()
                self.pop_count += 1
            except Exception as e:
                logging.error("Wait queue empty! err:%s", e)
                time.sleep(1)
                continue

            # 处理数据
            if message.type in self.callback.keys():
                self.callback[message.type](message.data)
            else:
                logging.error("Unsupported message type! type:%d", message.type)

    def register(self, typ, callback):
        """ 注册回调函数 """
        self.callback[typ] = callback

    def bpush(self, typ, data):
        """ 往队列中加入数据(阻塞) """
        message = Message(typ, data)
        while True:
            if len(self.wait_queue) >= self.capacity:
                time.sleep(1)
                continue
            self.wait_queue.append(message)
            self.push_count += 1
            return

    def print(self):
        """ 打印当前处理情况 """
        print("Wait queue capacity:%d wait:%d push:%d pop:%d" %
              (self.capacity, len(self.wait_queue), self.push_count, self.pop_count))

        logging.info("Wait queue capacity:%d wait:%d push:%d pop:%d" %
                     (self.capacity, len(self.wait_queue), self.push_count, self.pop_count))

    def exit(self):
        self.is_exit = True

    def wait(self):
        """ 等待处理结束 """
        while True:
            if len(self.wait_queue) > 0:
                time.sleep(1)
                continue
            return
