import sqlite_queue

# 配置数据库
queue = sqlite_queue.SqliteQueue('test1.db')
# 防止线程在测试时被杀
queue.setDaemon(False)
# 启动线程
queue.start()

# 创建表
queue.register_execute("""CREATE TABLE `stocks`(
                              id INTEGER PRIMARY KEY AUTOINCREMENT,
                              trans TEXT,
                              symbol TEXT,
                              price REAL,
                              qty REAL,
                              date DATE
                          );""")

# 插入单条数据
queue.register_execute("INSERT INTO stocks (`trans`,`symbol`,`price`,`qty`,`date`) "
                       "VALUES ('BUY','RHAT',35.14,100,'2017-01-01')")

# 预编译插入数据（更加安全，防止注入）
queue.register_execute("INSERT INTO stocks (`trans`,`symbol`,`price`,`qty`,`date`) "
                       "VALUES (?,?,?,?,?)", ('BUY', 'BlLl', 12.450, 18000, '2017-01-01'))

# 批量插入数据
purchases = [('BUY', 'DOUB', 500, 233, '2017-03-01'),
             ('SELL', 'S6', 1, 4369, '2017-04-01'),
             ('BUY', 'BlLl', 12.450, 18000, '2016-12-01')]
queue.register_execute("INSERT INTO stocks (`trans`,`symbol`,`price`,`qty`,`date`) "
                       "VALUES (?,?,?,?,?)", purchases)  # list会直接被识别为批量插入

# 回调返回插入的行数
queue.register_execute("INSERT INTO stocks (`trans`,`symbol`,`price`,`qty`,`date`) "
                       "VALUES (?,?,?,?,?)", ('SELL', 'LOL', 1, 4369, '2017-04-01'),
                       callback=lambda lst_rowid: print('S' + str(lst_rowid)))

# 测试select，回调读入数据
queue.register_execute("SELECT * FROM stocks", callback=lambda data: print(data))

# 测试预编译select，回调读入数据
queue.register_execute(
    "SELECT * FROM stocks WHERE `price` >= ? AND `trans` IN (?, ?) "
    "AND `date` BETWEEN ? AND ? ORDER BY `price`", (30, 'BUY', 'SELL', '2017-02-01', '2017-12-31'),
    callback=lambda data: print(data))
