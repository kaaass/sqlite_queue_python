# -*-coding:utf8;-*-
import sqlite3
import queue
import threading
import time

"""基于python实现的sqlite队列，方便的处理sqlite并发。

讲道理，写这个库的人并不会写python。
SqliteQueue是继承了threading.Thread的线程，并且维护了一个向sqlite请求的队列。
"""

__author__ = "KAAAsS"  # ←超帅
__version__ = '0.0.1'
__license__ = "GPL v2"


class SqliteQueue(threading.Thread):
    def __init__(self, db, wait=5):
        """
        :param db: sqlite数据库文件
        :param wait: 任务等候时间，单位秒，默认5
        """
        threading.Thread.__init__(self)
        self._queue = queue.Queue()
        self.wait = wait
        self._conn = sqlite3.connect(db)  # 链接sqlite库
        self._cursor = self._conn.cursor()  # 获取cursor

    def run(self):
        while True:
            if self._queue.qsize() < 1:  # 如果队列空则sleep线程一段时间，以等待新操作
                time.sleep(self.wait)
                continue
            task = self._queue.get()
            self._deal_task(task)

    def _deal_task(self, task):
        if isinstance(task['data'], tuple):  # 元组即execute
            self._cursor.execute(task['execute'], task['data'])
        elif isinstance(task['data'], list):  # 列表即executemany
            self._cursor.executemany(task['execute'], task['data'])

    def register_execute(self, execute, data=None, callback=None):
        """
        注册一个执行指定SQL有进步的任务
        :param execute: SQL语句
        :param data: 预编译参数
        :param callback: 回调
        :return:
        """
        if isinstance(execute, str):
            raise SqliteQueueError('Illegal param! "execute" must be string!')
        elif data is not None and (isinstance(data, tuple) or isinstance(data, list)):
            raise SqliteQueueError('Illegal param! "data" must be tuple or list!')
        elif callback is not None and type(callback) == "<class 'function'>":
            raise SqliteQueueError('Illegal param! "callback" must be function!')
        self._queue.put({
            'execute': execute,
            'data': data,
            'callback': callback
        })


class SqliteQueueError(Exception):
    pass
