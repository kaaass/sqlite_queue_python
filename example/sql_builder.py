import sqlite_queue

# 配置数据库
queue = sqlite_queue.SqliteQueue('test2.db')
# 防止线程在测试时被杀
queue.setDaemon(False)
# 启动线程
queue.start()

# 创建表
queue.create('stocks').data({
    'id': 'INTEGER PRIMARY KEY AUTOINCREMENT',
    'trans': 'TEXT',
    'symbol': 'TEXT',
    'price': 'REAL',
    'qty': 'REAL',
    'date': 'DATE'
}).register()

# 插入单条数据(事实上，本库会自动将参数提取出来并使用预编译)
queue.insert('stocks')\
    .data({'trans': 'BUY', 'symbol': 'RHAT', 'price': 35.14, 'qty': 100, 'date': '2017-01-01'}).register()
queue.insert('stocks')\
    .data({'trans': 'BUY', 'symbol': 'BlLl', 'price': 12.450, 'qty': 18000, 'date': '2017-01-01'}).register()

# 批量插入数据
data_source = [
    {'trans': 'BUY', 'symbol': 'DOUB', 'price': 500, 'qty': 233, 'date': '2017-03-01'},
    {'trans': 'SELL', 'symbol': 'S6', 'price': 1, 'qty': 4369, 'date': '2017-04-01'},
    {'trans': 'BUY', 'symbol': 'BlLl', 'price': 12.450, 'qty': 18000, 'date': '2017-12-01'}
]
queue.insert('stocks').data(data_source).register()

# 回调返回插入的行数
queue.insert('stocks').data({'trans': 'SELL', 'symbol': 'LOL', 'price': 1, 'qty': 4369, 'date': '2017-04-01'})\
    .register(lambda lst_rowid: print('S' + str(lst_rowid)))

# 测试select，回调读入数据
queue.select('stocks').register(lambda data: print(data))

# 测试预编译select，回调读入数据
queue.select('stocks').where('price', '>=', 30).where('trans', ['BUY', 'SELL'])\
    .where('date', '><', ['2017-02-01', '2017-12-31']).order('price').register(lambda data: print(data))

# 附带另外一种where写法，实际上是相同的
queue.select('stocks').where({
    'price[>=]': 30,
    'trans': ['BUY', 'SELL'],
    'date[><]': ['2017-02-01', '2017-12-31']}).order('price').register(lambda data: print(data))
