import sqlite_queue
from peewee import *

# 配置数据库
queue = sqlite_queue.SqliteQueue('test3.db')
# 防止线程在测试时被杀
queue.setDaemon(False)
# 启动线程
queue.start()


# 创建表模型
class stocks(Model):
    id = PrimaryKeyField()
    trans = TextField()
    symbol = TextField()
    price = FloatField()
    qty = FloatField()
    date = DateField()


# 创建表，这一步暂时不可以
queue.register_execute("""CREATE TABLE `stocks`(
                              id INTEGER PRIMARY KEY AUTOINCREMENT,
                              trans TEXT,
                              symbol TEXT,
                              price REAL,
                              qty REAL,
                              date DATE
                          );""")

# 插入单条数据(事实上，peewee为自动将参数提取出来并使用预编译)
queue.register_peewee_query(stocks.insert(trans='BUY', symbol='RHAT', price=35.14, qty=100, date='2017-01-01'))
queue.register_peewee_query(stocks.insert(trans='BUY', symbol='BlLl', price=12.450, qty=18000, date='2017-01-01'))

# 批量插入数据
data_source = [
    {'trans': 'BUY', 'symbol': 'DOUB', 'price': 500, 'qty': 233, 'date': '2017-03-01'},
    {'trans': 'SELL', 'symbol': 'S6', 'price': 1, 'qty': 4369, 'date': '2017-04-01'},
    {'trans': 'BUY', 'symbol': 'BlLl', 'price': 12.450, 'qty': 18000, 'date': '2017-12-01'}
]
for data_dict in data_source:
    queue.register_peewee_query(stocks.insert(**data_dict))

# 回调返回插入的行数
queue.register_peewee_query(stocks.insert(trans='SELL', symbol='LOL', price=1, qty=4369, date='2017-04-01'),
                            callback=lambda lst_rowid: print('S' + str(lst_rowid)))

# 测试select，回调读入数据
queue.register_peewee_query(stocks.select(), callback=lambda data: print(data))

# 测试预编译select，回调读入数据
queue.register_peewee_query(stocks.select().where((stocks.price >= 30) &
                                                  ((stocks.trans == 'BUY') | (stocks.trans == 'SELL')) &
                                                  ((stocks.date > '2017-02-01') & (stocks.date < '2017-12-31')))
                            .order_by('price'), callback=lambda data: print(data))
