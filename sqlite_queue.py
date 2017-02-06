# -*-coding:utf8;-*-
import sqlite3
import queue
import threading
import time
import re
import inspect

__peewee__ = True
try:
    import peewee
except ImportError:
    __peewee__ = False

"""基于python实现的sqlite队列，方便的处理sqlite并发。

讲道理，写这个库的人并不会写python。SqliteQueue是继承了threading.Thread的线程，并且维护了一个向sqlite请求的队列。
回调函数参数：lst_rowid（整数，最后一行行号）、data（数据，如select会有返回）、rowcount（行数）
"""

__author__ = "KAAAsS"  # ←超帅
__version__ = '0.1.0'
__license__ = "GPL v2"

_OPERATOR_MAPPING = {
    "!": "!=",
    "~": "LIKE",
    "!~": "NOT LIKE"
}


class SqliteQueue(threading.Thread):
    def __init__(self, db, wait=5):
        """
        :param db: sqlite数据库文件
        :param wait: 任务等候时间，单位秒，默认5
        """
        threading.Thread.__init__(self)
        self.daemon = True  # 默认为守护线程
        self._queue = queue.Queue()
        self.wait = wait
        self._db = db
        self._conn = None
        self._cursor = None

    def run(self):
        self._conn = sqlite3.connect(self._db)  # 链接sqlite库
        self._cursor = self._conn.cursor()  # 获取cursor
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
        else:
            self._cursor.execute(task['execute'])
        self._conn.commit()
        # 取回调函数的参数
        if task['callback'] is None:
            return
        paras = inspect.getargspec(task['callback'])[0]
        kwargs = {}
        for param in paras:
            if param == 'data':
                kwargs['data'] = self._cursor.fetchall()
            elif param == 'rowcount':
                kwargs['rowcount'] = self._cursor.rowcount
            elif param == 'lst_rowid':
                kwargs['lst_rowid'] = self._cursor.lastrowid
            else:
                kwargs[param] = None
        task['callback'](**kwargs)  # 回调

    def register_execute(self, execute, data=None, callback=None):
        """
        注册一个执行指定SQL命令的操作
        :param execute: SQL语句
        :param data: 预编译参数
        :param callback: 回调
        :return:
        """
        if not isinstance(execute, str):
            raise SqliteQueueError('Illegal param! "execute" must be string!')
        elif data is not None and (not isinstance(data, tuple) and not isinstance(data, list)):
            raise SqliteQueueError('Illegal param! "data" must be tuple or list!')
        elif callback is not None and str(type(callback)) != "<class 'function'>":
            raise SqliteQueueError('Illegal param! "callback" must be function!')
        self._queue.put({
            'execute': execute,
            'data': data,
            'callback': callback
        })

    def register_peewee_query(self, pw_query, callback=None):
        """
        注册一个执行peewee查询的操作
        :param pw_query: peewee查询对象
        :param callback: 回调
        :return:
        """
        if not __peewee__:
            raise SqliteQueueError('Module "peewee" have not been installed.')
        if not isinstance(pw_query, peewee.Query):
            raise SqliteQueueError('Illegal param! "pw_query" must be peewee.Query!')
        sql = pw_query.sql()
        self.register_execute(sql[0], tuple(sql[1]), callback=callback)

    def select(self, table):
        """
        构建select语句
        :param table: 目标表
        :return:
        """
        return SqlQuery(table, obj_queue=self)

    def insert(self, table, data=None):
        """
        构建insert语句
        :param table: 目标表
        :param data: 插入的数据(dict)
        :return:
        """
        return SqlQuery(table, method='INSERT', params=data, obj_queue=self)

    def update(self, table, data=None):
        """
        构建update语句
        :param table: 目标表
        :param data: 插入的数据(dict)
        :return:
        """
        return SqlQuery(table, method='UPDATE', params=data, obj_queue=self)

    def delete(self, table):
        """
        构建delete语句
        :param table: 目标表
        :return:
        """
        return SqlQuery(table, method='UPDATE', obj_queue=self)

    def drop(self, table):
        """
        构建drop语句
        :param table: 目标表
        :return:
        """
        return SqlQuery(table, method='DROP', obj_queue=self)

    def create(self, table, params=None):
        """
        创建表
        :param table: 目标表
        :param params: 包含字段
        :return:
        """
        return SqlQuery(table, method='CREATE', params=params, obj_queue=self)


class SqlQuery:
    """
    简单的sql命令封装
    """

    def __init__(self, table, method='SELECT', params=None, obj_queue=None):
        self._queue = obj_queue
        self._sql = {'table': table, 'method': method, 'distinct': False}
        self._params = params
        self._data = None

    def execute(self, command, data=None):
        self._sql = command
        self._data = data
        return self

    def _has_commanded(self):
        """
        检查是否执行过execute方法
        :return:
        """
        if isinstance(self._sql, str):
            raise Exception('This method cannot be invoked after using method "execute"!')

    def table(self, table):
        """
        设定操作的目标表
        :param table:
        :return:
        """
        self._has_commanded()
        self._sql['table'] = table
        return self

    def field(self, *args):
        """
        表示操作对象的字段
        :param args:
        :return:
        """
        self._has_commanded()
        result = ''
        if len(args) < 1:
            raise ValueError('Method "field" needs at least one param!')
        elif len(args) == 1:
            args = args[0]
            if isinstance(args, str):  # 若为字符串，则视为直接输入SQL格式
                result = args + ' '  # 空格占位哈哈哈哈，我有毒
            elif isinstance(args, dict):  # 若为字典，则可能有as
                if len(args) < 1:
                    raise ValueError('Illegal length of param for method "field"!')
                for k, v in args.items():
                    if v is None:
                        result += '`%s`,' % str(k)
                    else:
                        result += '`%s` AS %s,' % (str(k), str(v))
            else:
                raise ValueError('Illegal param for method "field"!')
        else:
            for v in args:  # 直接拼接即可
                result += '`%s`,' % str(v)
        self._sql['field'] = result[:-1]
        return self

    def where(self, *args):
        """
        设置where条件，重复调用会叠加并使用AND连接
        :param args:
        :return:
        """
        self._has_commanded()
        cond = _parse_condition(*args)
        if 'where' in self._sql:  # 已经存在where了，拼接之
            self._sql['where'][0] += ' AND ' + cond[0]  # 拼接条件语句
            self._sql['where'][1] += cond[1]  # 拼接参数
        else:
            self._sql['where'] = cond
        return self

    def or_where(self, *args):
        """
        设置where条件，重复调用会叠加并使用OR连接
        :param args:
        :return:
        """
        self._has_commanded()
        if 'where' in self._sql:  # 已经存在where了，拼接之
            cond = _parse_condition(*args)
            self._sql['where'][0] += ' OR ' + cond[0]  # 拼接条件语句
            self._sql['where'][1] += cond[1]  # 拼接参数
        else:  # 不存在和where方法相同
            self.where(*args)
        return self

    def order(self, *args):
        """
        设置排序字段
        :param args:
        :return:
        """
        self._has_commanded()
        result = ''
        if len(args) < 1:
            raise ValueError('Method "order" needs at least one param!')
        elif len(args) == 1 and _is_sql(args[0]):  # SQL格式
            self._sql['order'] = args[0]
            return self
        elif isinstance(args[0], dict):  # 含排序方式
            for k, v in args[0].items():
                if v is None:
                    result += '`%s`,' % str(k)
                else:
                    result += '`%s` %s,' % (str(k), str(v))
        else:
            for v in args:
                result += '`%s`,' % str(v)
        self._sql['order'] = result[:-1]
        return self

    def group(self, *args):
        """
        设置结果分组(GROUP BY)
        :param args:
        :return:
        """
        self._has_commanded()
        result = ''
        if len(args) < 1:
            raise ValueError('Method "order" needs at least one param!')
        elif len(args) == 1 and _is_sql(args[0]):  # SQL格式
            self._sql['group'] = args[0]
            return self
        else:
            for v in args:
                result += '`%s`,' % str(v)
        self._sql['group'] = result[:-1]
        return self

    def limit(self, start, num):
        """
        设置返回条数限制
        :param start: 开始位置
        :param num: 返回条数
        :return:
        """
        self._has_commanded()
        # 参数检查
        if int(start) < 0:
            raise ValueError('The value of param "start" must be positive number!')
        if int(num) < 1:
            raise ValueError('The value of param "num" must bigger than zero!')
        self._sql['limit'] = ['?,?', [start, num]]
        return self

    def page(self, index, num):
        """
        返回结果分页
        :param index: 页码，1开始
        :param num: 单页结果数
        :return:
        """
        self.limit((int(index) - 1) * int(num), int(num))
        return self

    def having(self, *args):
        """
        设置having条件，重复调用会叠加并使用AND连接
        :param args:
        :return:
        """
        self._has_commanded()
        cond = _parse_condition(*args)
        if 'having' in self._sql:  # 已经存在where了，拼接之
            self._sql['having'][0] += ' AND ' + cond[0]  # 拼接条件语句
            self._sql['having'][1] += cond[1]  # 拼接参数
        else:
            self._sql['having'] = cond
        return self

    def or_having(self, *args):
        """
        设置having条件，重复调用会叠加并使用OR连接
        :param args:
        :return:
        """
        self._has_commanded()
        if 'having' in self._sql:  # 已经存在where了，拼接之
            cond = _parse_condition(*args)
            self._sql['having'][0] += ' OR ' + cond[0]  # 拼接条件语句
            self._sql['having'][1] += cond[1]  # 拼接参数
        else:  # 不存在和where方法相同
            self.having(*args)
        return self

    def distinct(self, is_distinct):
        """
        设置是否返回重复值
        :param is_distinct: 是否返回重复值
        :type is_distinct: bool
        :return:
        """
        self._has_commanded()
        self._sql['distinct'] = bool(is_distinct)
        return self

    def data(self, data):
        """
        增加命令所需数据。如INSERT, UPDATE。
        :param data:
        :return:
        """
        self._params = data
        return self

    def get_sql(self):
        """
        生成sql语句
        :return: 元组，若长度为2则包含参数
        """
        if isinstance(self._sql, str):
            if self._data is None:
                return self._sql,
            else:
                return self._sql, self._data
        method = self._sql['method'].upper()
        # select语句顺序: SELECT-field-from-where-order-group-limit-having
        if method == 'SELECT':  # 生成SELECT语句
            if 'field' not in self._sql:
                field = '*'
            else:
                field = self._sql['field']
            if self._sql['distinct']:
                distinct = 'DISTINCT '
            else:
                distinct = ''
            sql = 'SELECT %s%s FROM %s' % (distinct, field, self._sql['table'])
            data = []
            if 'where' in self._sql:
                sql += ' WHERE ' + self._sql['where'][0]
                data += self._sql['where'][1]
            if 'order' in self._sql:
                sql += ' ORDER BY ' + self._sql['order']
            if 'group' in self._sql:
                sql += ' GROUP BY ' + self._sql['group']
            if 'limit' in self._sql:
                sql += ' LIMIT ' + self._sql['limit'][0]
                data += self._sql['limit'][1]
            if 'having' in self._sql:
                sql += ' HAVING ' + self._sql['having'][0]
                data += self._sql['having'][1]
            return sql, tuple(data)
        elif method == 'INSERT':  # 生成INSERT语句
            if isinstance(self._params, dict) and len(self._params) > 0:
                dic = [self._params]
            elif isinstance(self._params, list) and len(self._params) > 0:
                dic = self._params
            else:
                raise ValueError('Illegal value for param!')
            result = []
            for v in dic:
                sql = 'INSERT INTO %s (' % self._sql['table']
                for k in v.keys():
                    sql += '`%s`,' % k
                sql = sql[:-1] + ') VALUES (' + ('?,' * len(v))[:-1] + ')'
                result.append((sql, tuple(v.values())))
            return result
        elif method == 'UPDATE':  # 生成UPDATE语句
            if not isinstance(self._params, dict) or len(self._params) < 1:
                raise ValueError('Illegal value for param!')
            sql = 'UPDATE %s SET ' % self._sql['table']
            data = []
            for k in self._params.keys():
                sql += '`%s`=?,' % k
            sql = sql[:-1]  # 去除多余逗号
            data += self._params.values()
            if 'where' in self._sql:
                sql += ' WHERE ' + self._sql['where'][0]
                data += self._sql['where'][1]
            return sql, tuple(data)
        elif method == 'DELETE':  # 生成DELETE语句
            sql = 'DELETE FROM %s' % self._sql['table']
            data = []
            if 'where' in self._sql:
                sql += ' WHERE ' + self._sql['where'][0]
                data += self._sql['where'][1]
            return sql, tuple(data)
        elif method == 'TRUNCATE':
            raise Exception('Method "TRUNCATE" wasn\'t support in sqlite!')
        elif method == 'DROP':  # 生成DROP语句
            return 'DROP TABLE ' + self._sql['table'],
        elif method == 'CREATE':
            sql = 'CREATE TABLE `%s`(' % self._sql['table']
            for k, v in self._params.items():
                sql += '%s %s,' % (k, v)
            return sql[:-1] + ')',
        else:
            raise Exception("Unknown method %s!" % method)

    def register(self, callback=None):
        """
        注册为SqliteQueue的任务
        :param callback: 回调函数
        :return:
        """
        if self._queue is None or not isinstance(self._queue, SqliteQueue):
            raise Exception("This object wasn't belong to a SqliteQueue!")
        sql = self.get_sql()
        if not isinstance(sql, list):
            sql = [sql]
        for v in sql:
            self._queue.register_execute(*v, callback=callback)
        return self


class SqliteQueueError(Exception):
    pass


def _is_sql(obj):
    """
    最简单的sql语句判断，别乱用哦！
    :param obj:
    :return:
    """
    return isinstance(obj, str) and ('`' in str(obj) or ',' in str(obj))


def _get_operator(string):
    """
    解析字符串获得操作符和字段名
    :param string:
    :return:
    """
    o = re.findall("^([a-zA-Z_]+)(?:\[(.+)\])?$", string)
    if len(o) < 1:  # 匹配失败
        raise Exception('Illegal syntax of expression: ' + string)
    o = list(o[0])  # 原先是tuple
    if len(o[1]) < 1:  # 未填写操作符，默认等号
        o[1] = '='
    if o[1] in _OPERATOR_MAPPING:
        o[1] = _OPERATOR_MAPPING[o[1]]
    return o


def _parse_condition(*args):
    """
    解析条件语句
    :param args:
    :return:
    """
    conj = 'AND'
    if len(args) < 1:
        raise ValueError('This method need at least one param!')
    elif len(args) == 1 and isinstance(args[0], str):  # SQL格式
        return [args[0], None]
    elif len(args) == 2 and isinstance(args[0], str) and isinstance(args[1], tuple):  # SQL格式带参数
        return list(args)
    elif len(args) == 2 and isinstance(args[0], str):  # 单行，=号
        cond = {args[0]: args[1]}
    elif len(args) == 3 and isinstance(args[0], str):  # 单行
        cond = {'%s[%s]' % (args[0], str(args[1])): args[2]}
    elif isinstance(args[0], dict):
        cond = args[0]
        if len(args) == 2 and isinstance(args[1], str):
            conj = args[1]
    else:
        raise ValueError('Illegal param! ' + str(args))
    return _parse_dict_condition(cond, conj)


def _parse_dict_condition(obj, conj='AND'):
    """
    由字典
    :param obj: 字典对象
    :param conj: 连接词
    :return:
    """
    if conj not in ['AND', 'OR']:  # 检查连接词是否合法
        raise ValueError('Unknown conjunction: ' + conj)
    cond = ''
    value_set = []  # 存放预编译用参数
    for (k, v) in obj.items():
        if isinstance(v, dict):  # 解析嵌套
            op = re.findall('\((.+)\).?', k)
            if len(op) < 1:
                raise Exception('Illegal syntax of expression: ' + k)
            inner = _parse_dict_condition(v, op[0])  # 递归解决此类嵌套
            cond += ' %s (%s)' % (conj, inner[0])
            value_set += inner[1]
            continue
        o = _get_operator(k)
        if isinstance(v, list):
            not_ = ''
            if o[1] in ['<>', '><']:  # 为BETWEEN
                if not len(v) == 2:
                    raise Exception('Illegal length for value: ' + str(v))
                if o[1] == '<>':
                    not_ = 'NOT '
                cond += ' %s (`%s` %sBETWEEN ? AND ?)' % (conj, o[0], not_)
            else:  # 为IN
                if o[1] == '!=':  # 不用判断!，已经被转为!=
                    not_ = 'NOT '
                cond += ' %s `%s` %sIN (' % (conj, o[0], not_) \
                        + ('?,' * len(v))[:-1] + ')'
            value_set += v
        else:
            cond += " %s `%s` %s ?" % (conj, o[0], o[1])
            value_set.append(v)
    return [cond[len(conj) + 2:], value_set]
